import os
import gzip

from cdf.utils.s3 import push_file
from cdf.features.main.streams import IdStreamDef, InfosStreamDef, ZoneStreamDef
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir

from cdf.core.streams.utils import group_left

def get_lang(info_entry, lang_idx):
    """Get the lang.
    :param info_entry: the entry from the urlinfos stream
    :type info_entry: list
    :param lang_idx: the index of the lang value in the urlinfos stream
    :type lang_idx: int
    :returns: str
    """
    if lang_idx < len(info_entry):
        result = info_entry[lang_idx]
        if result == "?":
            result = "notset"
    else:
        result = "notset"
    return result


def generate_zone_stream(id_stream,
                         info_stream):
    """Generate a zone stream from two streams: urlids and urlinfos
    :param id_stream: the input urlids stream
    :type id_stream: stream
    :param info_stream: the input urlinfos stream
    :type info_stream: stream
    """
    group_stream = group_left((id_stream, 0), info=(info_stream, 0))
    protocol_idx = IdStreamDef.field_idx("protocol")
    lang_idx = InfosStreamDef.field_idx("lang")
    for urlid, id_entry, info_entry in group_stream:
        protocol = id_entry[protocol_idx]
        lang = get_lang(info_entry["info"][0], lang_idx)
        zone = "{},{}".format(lang, protocol)
        yield (urlid, zone)


@with_temporary_dir
def compute_zones(s3_uri,
                  part_id,
                  tmp_dir=None,
                  force_fetch=False):
    """A task to compute the zones for a given part
    :param s3_uri: the uri where the crawl data is stored
    :type s3_uri: str
    :param part_id: the id of the part to process
    :type part_id:int
    :param tmp_dir: the path to the tmp directory to use.
                    If None, a new tmp directory will be created.
    :param tmp_dir: str
    :param force_fetch: if True, the files will be downloaded from s3
                        even if they are in the tmp directory.
                        if False, files that are present in the tmp_directory
                        will not be downloaded from s3.
    :rtype: string - the s3_uri of the generated zone file
    """
    #get base streams
    id_stream = IdStreamDef.get_stream_from_s3(s3_uri,
                                               tmp_dir=tmp_dir,
                                               part_id=part_id)
    info_stream = InfosStreamDef.get_stream_from_s3(s3_uri,
                                                    tmp_dir=tmp_dir,
                                                    part_id=part_id)
    output_file_name = "{}.txt.{}.gz".format(ZoneStreamDef.FILE, part_id)
    output_file_path = os.path.join(tmp_dir, output_file_name)
    with gzip.open(output_file_path, "w") as f:
        for urlid, zone in generate_zone_stream(id_stream, info_stream):
            f.write("{}\t{}\n".format(urlid, zone))

    #push file to s3
    s3_destination = "{}/{}".format(s3_uri, output_file_name)
    push_file(
        s3_destination,
        output_file_path
    )

    return s3_destination
