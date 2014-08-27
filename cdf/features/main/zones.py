from cdf.features.main.streams import IdStreamDef, InfosStreamDef

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

