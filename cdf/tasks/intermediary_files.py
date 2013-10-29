import os
import gzip
import itertools

from boto.exception import S3ResponseError

from cdf.utils.s3 import fetch_files, fetch_file, push_file
from cdf.streams.caster import Caster
from cdf.streams.mapping import STREAMS_HEADERS, STREAMS_FILES
from cdf.collections.urls.transducers.metadata_duplicate import get_duplicate_metadata
from cdf.collections.urls.transducers.links import OutlinksTransducer, InlinksTransducer
from cdf.streams.utils import split_file
from cdf.utils.remote_files import nb_parts_from_crawl_location
from cdf.collections.urls.generators.suggestions import UrlSuggestionsGenerator
from cdf.collections.urls.constants import SUGGEST_CLUSTERS



def make_links_counter_file(crawl_id, s3_uri, part_id, link_direction, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    if link_direction == "out":
        transducer = OutlinksTransducer
        stream_name = "outlinks_raw"
    else:
        transducer = InlinksTransducer
        stream_name = "inlinks_raw"

    links_file_path = 'url{}.txt.{}.gz'.format("links" if link_direction == "out" else "inlinks", part_id)
    try:
        links_file, fecthed = fetch_file(
            os.path.join(s3_uri, links_file_path),
            os.path.join(tmp_dir, links_file_path),
            force_fetch=force_fetch
        )
    except S3ResponseError:
        return

    cast = Caster(STREAMS_HEADERS[stream_name.upper()]).cast
    stream_links = cast(split_file(gzip.open(links_file)))
    generator = transducer(stream_links).get()

    filenames = {
        'links': 'url_{}_links_counters.txt.{}.gz'.format(link_direction, part_id),
        'canonical': 'url_{}_canonical_counters.txt.{}.gz'.format(link_direction, part_id),
        'redirect': 'url_{}_redirect_counters.txt.{}.gz'.format(link_direction, part_id),
    }

    f_list = {k: gzip.open(os.path.join(tmp_dir, v), 'w') for k, v in filenames.iteritems()}

    for i, entry in enumerate(generator):
        f_list[entry[1]].write(str(entry[0]) + '\t' + '\t'.join(str(k) for k in entry[2:]) + '\n')

    for counter_filename in filenames.values():
        push_file(
            os.path.join(s3_uri, counter_filename),
            os.path.join(tmp_dir, counter_filename),
        )


def make_metadata_duplicates_file(crawl_id, s3_uri, first_part_id_size, part_id_size, tmp_dir_prefix='/tmp', force_fetch=False):
    # Fetch locally the files from S3
    tmp_dir = os.path.join(tmp_dir_prefix, 'crawl_%d' % crawl_id)

    streams_types = {'patterns': [],
                     'contents': []
                     }

    for part_id in xrange(0, nb_parts_from_crawl_location(s3_uri)):
        files_fetched = fetch_files(s3_uri,
                                    tmp_dir,
                                    regexp='url(ids|contents).txt.%d.gz' % part_id,
                                    force_fetch=force_fetch)

        for path_local, fetched in files_fetched:
            stream_identifier = STREAMS_FILES[os.path.basename(path_local).split('.')[0]]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams_types[stream_identifier].append(cast(split_file(gzip.open(path_local))))

    generator = get_duplicate_metadata(itertools.chain(*streams_types['patterns']),
                                       itertools.chain(*streams_types['contents']))

    current_part_id = 0
    f = gzip.open(os.path.join(tmp_dir, 'urlcontentsduplicate.txt.0.gz'), 'w')
    for i, (url_id, metadata_type, filled_nb, duplicates_nb, is_first, target_urls_ids) in enumerate(generator):

        # check the part id
        if (current_part_id == 0 and url_id > first_part_id_size) or \
           (current_part_id > 0 and (url_id - first_part_id_size) / part_id_size != current_part_id - 1):
            f.close()
            push_file(
                os.path.join(s3_uri, 'urlcontentsduplicate.txt.{}.gz'.format(current_part_id)),
                os.path.join(tmp_dir, 'urlcontentsduplicate.txt.{}.gz'.format(current_part_id)),
            )
            current_part_id += 1
            f = gzip.open(os.path.join(tmp_dir, 'urlcontentsduplicate.txt.{}.gz'.format(current_part_id)), 'w')

        f.write('\t'.join((
            str(url_id),
            str(metadata_type),
            str(filled_nb),
            str(duplicates_nb),
            '1' if is_first else '0',
            ';'.join(str(u) for u in target_urls_ids)
        )) + '\n')
    f.close()
    push_file(
        os.path.join(s3_uri, 'urlcontentsduplicate.txt.{}.gz'.format(current_part_id)),
        os.path.join(tmp_dir, 'urlcontentsduplicate.txt.{}.gz'.format(current_part_id)),
    )
