from collections import defaultdict
import itertools

from judyz_cffi import JudySL


class ProblematicUids(object):
    """
    ID's
    Array of tuples
    """
    def __init__(self):
        self.prob_uids = []

    def __iter__(self):
        """
        Get IDs formatted for StreamDefBase.persist
        :return: (id, string-of-other-ids)
        """
        for it in self.prob_uids:
            yield it[0], ' '.join((str(item) for item in it[1:]))

    def add_duplicates(self, kvs_to_uid):
        """
        Find which (k, v) are present multiple time and store them.
        :param kvs_to_uid: dictionary: kv -> [uids]
        :type kvs_to_uid: dict
        :return:
        :rtype:
        """
        for kv, uids in kvs_to_uid.iteritems():
            if len(uids) > 1:
                self.prob_uids.append(uids)

    def explode(self):
        """Generate all the meaningful 'prob_uids' permutations and sort them
        1 2 3 ->
            1 2 3
            2 1 3
            3 1 2
            but not 1 3 2
                    2 3 1
                    3 2 1
        """
        all_prob_uids = []
        id1s = set()
        for it in self.prob_uids:
            for p in itertools.permutations(it, len(it)):
                if p[0] not in id1s:
                    id1s.add(p[0])
                    all_prob_uids.append(p)
        all_prob_uids.sort()
        self.prob_uids = all_prob_uids


def get_urls_with_same_kv(urlids, max_crawled_urlid):
    with JudySL() as urls:
        for uid, protocol, host, path, query_string in urlids:
            if uid > max_crawled_urlid:
                break
            if '&' not in query_string:
                continue
            url = "{}://{}{}{}".format(protocol, host, path, query_string)
            urls[url] = uid

        prob_uids = ProblematicUids()
        prev_path = ""
        kvs_to_uid = defaultdict(list)
        url, uid, buf = urls.get_first()
        while url is not None:
            qsp = url.find('?')
            if url[:qsp] != prev_path:
                prob_uids.add_duplicates(kvs_to_uid)
                prev_path = url[:qsp]
                kvs_to_uid.clear()
            kv = '&'.join(sorted(url[qsp + 1:].split('&')))
            kvs_to_uid[kv].append(uid)
            url, uid, buf = urls.get_next(buf)
        prob_uids.add_duplicates(kvs_to_uid)
    prob_uids.explode()
    return prob_uids
