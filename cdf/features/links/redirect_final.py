from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.core.metadata.dataformat import check_enabled
from cdf.core.streams.base import StreamDefBase
from cdf.features.links.settings import GROUPS
from cdf.features.links.streams import OutlinksRawStreamDef
from cdf.metadata.url.url_metadata import STRUCT_TYPE, BOOLEAN_TYPE, INT_TYPE, ES_NO_INDEX, URL_ID, DIFF_QUALITATIVE, \
    ES_DOC_VALUE, AGG_CATEGORICAL, AGG_NUMERICAL, DIFF_QUANTITATIVE

__author__ = 'zeb'
"""
Class/functions for computing redirections' final destinations.
"""


class FinalRedirectionStreamDef(StreamDefBase):
    URL_DOCUMENT_MAPPING = {
        # outgoing redirection: final destination
        "redirect.to.final_url": {
            "verbose_name": "Ultimate Redirection",
            "group": GROUPS.redirects.name,
            "type": STRUCT_TYPE,
            "values": {
                "url_str": {"type": "string"},
                "url_id": {"type": "integer"},
                "http_code": {"type": "integer"}
            },
            "settings": {
                ES_NO_INDEX,
                RENDERING.URL_STATUS,
                FIELD_RIGHTS.FILTERS_EXIST,
                FIELD_RIGHTS.SELECT,
                URL_ID,
                DIFF_QUALITATIVE
            },
            "enabled": check_enabled("chains")
        },
        "redirect.to.final_url_exists": {
            "type": BOOLEAN_TYPE,
            "default_value": None,
            "enabled": check_enabled("chains")
        },
        "redirect.to.nb_hops": {
            "verbose_name": "Number of Redirection Hops Until Final Destination",
            "group": GROUPS.redirects.name,
            "type": INT_TYPE,
            "settings": {
                ES_DOC_VALUE,
                AGG_CATEGORICAL,
                AGG_NUMERICAL,
                DIFF_QUANTITATIVE
            },
            "enabled": check_enabled("chains")
        },
        "redirect.to.in_loop": {
            "verbose_name": "Url is Part of Redirection Loop",
            "group": GROUPS.redirects.name,
            "type": BOOLEAN_TYPE,
            "default_value": None,
            "enabled": check_enabled("chains")
        },
    }

    @staticmethod
    def compute_final_redirections(links):
        try:
            uid_to_dst = judy.JudyL()
            uid_to_ext = judy.JudyToObject()
            uid_nb_hops = judy.JudyL()
            uid_in_loop = judy.Judy1()
        except NameError:
            uid_to_dst = {}
            uid_to_ext = {}
            uid_nb_hops = {}
            uid_in_loop = set()
        for uid, link_type, mask, dst, ext_url in links:
            if link_type[0] != 'r':
                continue
            uid_to_dst[uid] = dst
            if dst == -1:
                uid_to_ext[uid] = ext_url
        del uid, link_type, mask, dst, ext_url
        for hop in uid_to_dst.keys():
            nb_hops = uid_nb_hops.get(hop, 0)
            if nb_hops == 0:
                hops_seen = [hop]
                hops_seen_set = {hop}
                in_loop = False
                while True:
                    nb_hops += 1
                    next_hop = uid_to_dst.get(hop, -1)
                    # Optimization, but must redo the final update (hops_seen, last_hop, nb_hops and i are different)
                    # if next_hop in uid_nb_hops:
                    #     # TODO path found: check loop; update data; break
                    #     raise NotImplementedError("path found")
                    if next_hop == -1:
                        next_hop_url = uid_to_ext.get(hop, None)
                        if next_hop_url is None:
                            break
                        # TODO check loop; update data; break
                        last_hop_url = next_hop_url
                        hops_seen.append(-1)
                        nb_hops += 1  # count that last -1
                        break  # raise NotImplementedError("to external")
                    hop = next_hop
                    if hop in hops_seen_set:
                        in_loop = True
                        break
                    hops_seen.append(hop)
                    hops_seen_set.add(hop)
                    if nb_hops > 10:
                        in_loop = True
                        break
                last_hop = hops_seen[-1]
                # if last_hop == -1:
                #     # TODO: use next_hop_url
                #     raise NotImplementedError("to external")
                for i in range(nb_hops - 1):
                    hop = hops_seen[i]
                    uid_to_dst[hop] = last_hop
                    if last_hop == -1:
                        uid_to_ext[hop] = last_hop_url
                    uid_nb_hops[hop] = nb_hops - i
                    if in_loop:
                        uid_in_loop.add(hop)
            else:
                pass # already computed

        return uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop
