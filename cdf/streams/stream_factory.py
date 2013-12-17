import os
import re
import gzip
import itertools

from urlparse import urlsplit

from cdf.log import logger

from cdf.streams.mapping import (STREAMS_HEADERS,
                                 STREAMS_FILES,
                                 CONTENT_TYPE_NAME_TO_ID)
from cdf.streams.caster import Caster
from cdf.streams.utils import split_file, idx_from_stream


class StreamFactory(object):
    """Factory that produces a stream out of files."""

    def __init__(self, dirpath, content, part_id=None):
        """Initiate a factory

        :param content: basename of a content, eg. urlids
        :param part_id: select a partition, if None, stream all
            partitions in order
        """
        self.dirpath = dirpath
        self.part_id = part_id

        # pre-check on file basename
        if content in STREAMS_FILES:
            self.content = content
        else:
            raise Exception("{} is not a known raw file basename".format(content))

    def _get_file_regexp(self):
        template = '{}.txt.{}.gz'
        if self.part_id:
            return template.format(self.content, self.part_id)
        else:
            return template.format(self.content, '*')

    # TODO(darkjh) maybe put this in a util module
    def _list_local_files(self, regexp, full_path=True, sort=True):
        # assume file name format to be `basename.txt.part_id.gz`
        def file_sort_key(filename):
            return int(filename.split('.')[2])

        result = []
        for f in os.listdir(self.dirpath):
            # regexp is a string, try match it
            if isinstance(regexp, str) and re.match(regexp, f):
                result.append(f)
            # regexp is a list of string, try match anyone of it
            elif isinstance(regexp, (list, tuple)):
                if any(re.match(r, f) for r in regexp):
                    result.append(f)
        if sort:
            result.sort(key=file_sort_key)

        if full_path:
            for i in xrange(0, len(result)):
                result[i] = os.path.join(self.dirpath, result[i])

        return result

    def get_stream(self):
        regexp = self._get_file_regexp()
        logger.info('Streaming files with regexp {}'.format(regexp))
        ordered_files = self._list_local_files(regexp)

        streams = []
        for f in ordered_files:
            stream_identifier = STREAMS_FILES[self.content]
            cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
            streams.append(cast(split_file(gzip.open(f))))

        return itertools.chain(*streams)

    def generate_paths(self):
        """Create a generator for the paths
        data_directory_path: the path to the directory
                             that contains crawl data
        The generator creates tuples (urlid, path)
        """
        url_generator = self.get_stream()
        for url in url_generator:
            urlid = url[idx_from_stream("PATTERNS", "id")]
            path = url[idx_from_stream("PATTERNS", "path")]
            path = unicode(path, encoding="utf-8")
            parsed_path = urlsplit(path)
            path = parsed_path.path
            yield urlid, path


# getting the number of pages takes a while.
# We'd better remember the result.
def get_number_pages(data_directory_path):
    """Return the number of available pages"""
    max_crawled_urlid = get_max_crawled_urlid(data_directory_path)
    urlinfos_stream_factory = StreamFactory(data_directory_path, "urlinfos")
    urlinfo_generator = urlinfos_stream_factory.get_stream()
    result = 0
    for urlinfo in urlinfo_generator:
        urlid = urlinfo[idx_from_stream("INFOS", "id")]
        httpcode = urlinfo[idx_from_stream("INFOS", "http_code")]
        if urlid > max_crawled_urlid:
            break  # there will be no more crawled url
        if httpcode == 0:
            continue
        result += 1
    return result


def get_max_crawled_urlid(data_directory_path):
    """Return the highest urlid that correspond to an url
    that was actually crawled.
    data_directory_path: the path to the directory that contains the data
    """
    #locate file
    filename = os.path.join(data_directory_path, "files.json")
    with open(filename) as f:
        json_content = json.load(f)
    result = json_content["max_uid_we_crawled"]
    result = int(result)
    return result
