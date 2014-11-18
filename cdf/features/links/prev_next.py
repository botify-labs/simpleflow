import itertools
from cdf.features.links.streams import InlinksStreamDef


def compute_prev_next_stream(inlinks_stream):
    """Compute a prev/next stream (based on PrevNextStreamDef) from a
    inlinks stream (based on InlinksStreamDef).
    Only follow links are considered.
    :param inlinks_stream: the input stream (based on InlinksStreamDef)
    :type inlinks_stream: iterable
    :returns: iterable - based on PrevNextStreamDef
    """
    id_index = InlinksStreamDef.field_idx('id')
    follow_mask_index = InlinksStreamDef.field_idx('follow')

    #consider only follow links
    inlinks_stream = itertools.ifilter(lambda x: 'follow' in x[follow_mask_index], inlinks_stream)
    for url_id, inlinks in itertools.groupby(inlinks_stream, key=lambda x: x[id_index]):
        receives_prev = False
        receives_next = False
        for inlink in inlinks:
            follow_mask = inlink[follow_mask_index]
            if "prev" in follow_mask:
                receives_prev = True
            if "next" in follow_mask:
                receives_next = True
        yield url_id, int(receives_prev), int(receives_next)
