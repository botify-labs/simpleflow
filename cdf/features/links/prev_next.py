import itertools
from cdf.features.links.streams import InlinksStreamDef
from cdf.features.links.helpers.masks import prev_next_mask


def compute_prev_next_stream(inlinks_stream):
    """Compute a prev/next stream (based on PrevNextStreamDef) from a
    inlinks stream (based on InlinksStreamDef)
    :param inlinks_stream: the input stream (based on InlinksStreamDef)
    :type inlinks_stream: iterable
    :returns: iterable - based on PrevNextStreamDef
    """
    id_index = InlinksStreamDef.field_idx('id')
    follow_mask_index = InlinksStreamDef.field_idx('follow')
    for url_id, inlinks in itertools.groupby(inlinks_stream, key=lambda x: x[id_index]):
        receives_prev = False
        receives_next = False
        for inlink in inlinks:
            follow_mask = inlink[follow_mask_index]
            prev_next_masks = prev_next_mask(follow_mask)
            if "prev" in prev_next_masks:
                receives_prev = True
            if "next" in prev_next_masks:
                receives_next = True
        yield url_id, receives_prev, receives_next
