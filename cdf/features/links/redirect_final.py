"""
Class/functions for computing redirects' final destinations.
"""


import collections
import logging

from cdf.features.main.streams import InfosStreamDef

try:
    from judyz_cffi import Judy1
    from judyz_cffi import JudyL
except Exception:
    pass

logger = logging.getLogger(__name__)

RedirectFinal = collections.namedtuple('RedirectFinal', [
    'uid', 'dst', 'nb_hops', 'ext', 'in_loop', 'http_code'
])


class _Result(collections.Iterable):
    """
    Redirects' final destinations, internal format.
    """
    def __init__(self):
        try:
            self.uid_to_dst = JudyL()
            self.uid_to_http_code = JudyL()
            self.uid_to_ext = {}  # TODO judyz.JudyToStr()
            self.uid_nb_hops = JudyL()
            self.uid_in_loop = Judy1()
        except (NameError, AttributeError):
            self.uid_to_dst = {}
            self.uid_to_http_code = {}
            self.uid_to_ext = {}
            self.uid_nb_hops = {}
            self.uid_in_loop = set()

    def clear(self):
        """
        Cleanup the results.
        """
        self.uid_to_dst.clear()
        self.uid_to_http_code.clear()
        self.uid_to_ext.clear()
        self.uid_nb_hops.clear()
        self.uid_in_loop.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()

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
                uid=uid, dst=dst, nb_hops=self.uid_nb_hops[uid], ext=ext,
                in_loop=in_loop, http_code=http_code
            )


def compute_final_redirects(stream_infos, stream_links):
    """Compute, for each redirect link, its final URL along with the hop count
     and whether it's in a loop.

    E.g.
    1 r301 0 2 ''
    2 r302 0 3 ''

    Where URL 3 is a 200 OK,

    Returns [
        (uid: 1, dst: 3, hops: 2, in_loop: False, http_code: 200),
        (uid: 2, dst: 3, hops: 1, in_loop: False, http_code: 200),
    ]

    The result is an context manager to turn into an iterable. Each iteration
    returns a RedirectFinal named tuple.
    (Either dst or ext is None)
    :param stream_infos: lines iterator on InfosStreamDef
    :param stream_links: lines iterator on OutlinksRawStreamDef
    :return: 'Tuples' of RedirectFinal
    """
    r = _Result()
    dsts = set()

    logger.info('Reading links')

    for line in stream_links:
        if 1:
            if line[1][0] != 'r':
                continue
            uid, link_type, mask, dst, ext_url = line
        else:
            # uid, link_type, mask, dst, ext_url
            p = line.find('\t') + 1
            if line[p] != 'r':
                continue
            uid, link_type, mask, dst, ext_url = line.split('\t')
            uid, dst = int(uid), int(dst)
        r.uid_to_dst[uid] = dst
        if dst != -1:
            dsts.add(dst)
        else:
            r.uid_to_ext[uid] = ext_url.rstrip()  # remove \n

    logger.info('Read %d redirects; %d are external', len(r.uid_to_dst),
                len(r.uid_to_ext))

    # http_code_idx = InfosStreamDef.field_idx('http_code')
    for line in stream_infos:
        if 0:
            # id, ..., http_code, ...
            p = line.find('\t')
            uid = int(line[:p])
            if uid not in dsts:
                continue
            line = line.split('\t')
            http_code = line[http_code_idx]
            # 200 is the default, and redirects can't be in the result
            if http_code not in ('200', '301', '302', '307', '308'):
                r.uid_to_http_code[uid] = int(http_code)
        else:
            uid, http_code = line
            # 200 is the default, and redirects can't be in the result
            if http_code not in (200, 301, 302, 307, 308):
                r.uid_to_http_code[uid] = http_code

    logger.info('Read %d http codes', len(r.uid_to_http_code))

    for hop in r.uid_to_dst.keys():
        nb_hops = r.uid_nb_hops.get(hop, 0)
        if nb_hops > 0:
            continue  # already computed
        hops_seen = [hop]
        hops_seen_set = {hop}
        in_loop = False
        while True:
            next_hop = r.uid_to_dst.get(hop, -1)
            # Optimization, requires changing the final update (hops_seen,
            # last_hop, nb_hops and i are different)
            # if next_hop in r.uid_nb_hops:
            #     raise NotImplementedError("path found")
            if next_hop == -1:
                next_hop_url = r.uid_to_ext.get(hop, None)
                if next_hop_url is None:
                    break
                last_hop_url = next_hop_url
                hops_seen.append(-1)
                nb_hops += 1  # count that -1
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

    logger.info('Loops: %d', len(r.uid_in_loop))

    return r
