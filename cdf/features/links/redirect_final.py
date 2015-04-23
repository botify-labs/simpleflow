"""
Class/functions for computing redirects' final destinations.
"""


from collections import namedtuple
import collections
from cdf.utils.convert import _raw_to_bool

try:
    import judyz
except Exception:
    pass

from cdf.core.metadata.constants import RENDERING, FIELD_RIGHTS
from cdf.core.metadata.dataformat import check_enabled
from cdf.core.streams.base import StreamDefBase
from cdf.features.links.settings import GROUPS
from cdf.features.main.streams import InfosStreamDef
from cdf.metadata.url.url_metadata import STRUCT_TYPE, BOOLEAN_TYPE, INT_TYPE, ES_NO_INDEX, URL_ID, DIFF_QUALITATIVE, \
    ES_DOC_VALUE, AGG_CATEGORICAL, AGG_NUMERICAL, DIFF_QUANTITATIVE


RedirectFinal = namedtuple('RedirectFinal', ['uid', 'dst', 'nb_hops', 'ext', 'in_loop', 'http_code'])


class _Result(collections.Iterable):
    """
    Redirects' final destinations, internal format.
    """
    def __init__(self):
        try:
            self.uid_to_dst = judyz.JudyL()
            self.uid_to_http_code = judyz.JudyL()
            self.uid_to_ext = judyz.JudyToObject()
            self.uid_nb_hops = judyz.JudyL()
            self.uid_in_loop = judyz.Judy1()
        except (NameError, AttributeError):
            self.uid_to_dst = {}
            self.uid_to_http_code = {}
            self.uid_to_ext = {}
            self.uid_nb_hops = {}
            self.uid_in_loop = set()

    def free(self):
        """
        Cleanup the results.
        :return:
        :rtype:
        """
        del self.uid_to_dst
        del self.uid_to_http_code
        del self.uid_to_ext
        del self.uid_nb_hops
        del self.uid_in_loop

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.free()

    def __iter__(self):
        for uid, dst in self.uid_to_dst.iteritems():
            ext = self.uid_to_ext.get(uid, '')
            in_loop = uid in self.uid_in_loop
            if not ext and not in_loop:
                http_code = self.uid_to_http_code.get(dst, 200)
            else:
                http_code = 0
            in_loop = int(in_loop)
            yield RedirectFinal(
                uid=uid, dst=dst, nb_hops=self.uid_nb_hops[uid], ext=ext, in_loop=in_loop, http_code=http_code
            )


class FinalRedirectionStreamDef(StreamDefBase):
    FILE = "final_redirection"

    HEADERS = (
        ('id', int),
        ('dst', int),
        ('nb_hops', int),
        ('ext', str),
        ('in_loop', _raw_to_bool),
        ('http_code', int),
    )

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

    def process_document(self, document, stream):
        uid, dst, nb_hops, external_url, in_loop, http_code = stream
        redirects_to = document['redirect']['to']
        redirects_to['final_url'] = {}
        if dst != -1:
            redirects_to['final_url']['url_id'] = dst
        else:
            redirects_to['final_url']['url_str'] = external_url
        if http_code:
            redirects_to['final_url']['http_code'] = http_code
        redirects_to['final_url_exists'] = True
        redirects_to['nb_hops'] = nb_hops
        redirects_to['in_loop'] = in_loop


def compute_final_redirects(stream_infos, stream_links):
    """Compute, for each redirect link, its final URL along with the hop count and whether it's in a loop.

    E.g.
    1 r301 0 2 ''
    2 r302 0 3 ''

    Where URL 3 is a 200 OK,

    Returns [
        (uid: 1, dst: 3, hops: 2, in_loop: False, http_code: 200),
        (uid: 2, dst: 3, hops: 1, in_loop: False, http_code: 200),
    ]

    The result is an context manager to turn into an iterable. Each iteration return a RedirectFinal named tuple.
    (Either dst or ext is None)
    :param stream_infos:
    :param stream_links:
    :return:
    :rtype:
    """
    r = _Result()
    dsts = set()

    for uid, link_type, mask, dst, ext_url in stream_links:
        if link_type[0] != 'r':
            continue
        r.uid_to_dst[uid] = dst
        if dst != -1:
            dsts.add(dst)
        else:
            r.uid_to_ext[uid] = ext_url

    http_code_idx = InfosStreamDef.field_idx('http_code')
    url_id_idx = InfosStreamDef.field_idx('id')
    for info in stream_infos:
        uid = info[url_id_idx]
        if uid in dsts:
            http_code = info[http_code_idx]
            # 200 is the default; redirects can't be in the result
            if http_code not in (200, 301, 302, 307, 308):
                r.uid_to_http_code[uid] = http_code

    # del uid, link_type, mask, dst, ext_url
    for hop in r.uid_to_dst.keys():
        nb_hops = r.uid_nb_hops.get(hop, 0)
        if nb_hops == 0:
            hops_seen = [hop]
            hops_seen_set = {hop}
            in_loop = False
            while True:
                next_hop = r.uid_to_dst.get(hop, -1)
                # Optimization, but must redo the final update (hops_seen, last_hop, nb_hops and i are different)
                # if next_hop in r.uid_nb_hops:
                #     # TODO path found: check loop; update data; break
                #     raise NotImplementedError("path found")
                if next_hop == -1:
                    next_hop_url = r.uid_to_ext.get(hop, None)
                    if next_hop_url is None:
                        break
                    # TODO check loop; update data; break
                    last_hop_url = next_hop_url
                    hops_seen.append(-1)
                    nb_hops += 1  # count that last -1
                    break
                hop = next_hop
                if hop in hops_seen_set:
                    in_loop = True
                    break
                hops_seen.append(hop)
                hops_seen_set.add(hop)
                nb_hops += 1
                if nb_hops > 10:
                    in_loop = True
                    break
            last_hop = hops_seen[-1]
            for i in range(nb_hops):
                hop = hops_seen[i]
                r.uid_to_dst[hop] = last_hop
                if last_hop == -1:
                    r.uid_to_ext[hop] = last_hop_url
                r.uid_nb_hops[hop] = nb_hops - i
                if in_loop:
                    r.uid_in_loop.add(hop)
        else:
            pass  # already computed

    return r  # uid_to_dst, uid_to_ext, uid_nb_hops, uid_in_loop
