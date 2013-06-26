from collections import defaultdict, Counter


from cdf.settings import CONTENT_TYPE_INDEX
from cdf.streams.utils import group_left, idx_from_stream


def delay_to_range(delay):
    if delay >= 2000:
        return "delay_gte_2s"
    elif delay >= 1000:
        return "delay_gte_1s"
    elif delay >= 500:
        return "delay_gte_500ms"
    return "delay_lt_500ms"


class PropertiesStatsAggregator(object):

    def __init__(self, stream_patterns, stream_infos, stream_properties, stream_outlinks, stream_inlinks, stream_contents):
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.stream_properties = stream_properties
        self.stream_inlinks = stream_inlinks
        self.stream_outlinks = stream_outlinks
        self.stream_contents = stream_contents

    def get(self):
        """
        Return a dictionnary where key is a tuple :
        (host, resource_type, depth, index, follow)
          str      str         int   bool    bool

        Values is a sub-dictonnary with counters keys.

        Ex :
        {
           ("www.site.com", "/article", 1, True, True) : {
                   "pages_nb": 10,
                   "pages_code_200": 5,
                   "pages_code_301": 5,
                   "pages_code_ok": 5,
                   "pages_code_ko": 5,
                   "redirections_nb": 0,
                   "inlinks_nb": 10,
                   "inlinks_follow_nb": 10,
                   "inlinks_nofollow_nb": 0,
                   "outlinks_nb": 5,
                   "total_delay_ms": 3400,
                   "avg_delay": 800,
                   "delay_gte_500ms": 3,
                   "delay_gte_1s": 3,
                   "delay_gte_2s": 1,
                   "title_filled_nb": 6,
                   "desc_filled_nb": 6,
                   "h1_filled_nb": 6,
                   "canonical_filled_nb": 3,
                   "canonical_duplicated_nb": 2,
            }
        }
        """

        left = (self.stream_patterns, 0)
        streams_ref = {'properties': (self.stream_properties, 0),
                       'infos': (self.stream_infos, 0),
                       'inlinks': (self.stream_inlinks, idx_from_stream('inlinks', 'id')),
                       'outlinks': (self.stream_outlinks, idx_from_stream('outlinks', 'id')),
                       'contents': (self.stream_contents, idx_from_stream('contents', 'id'))
                       }

        host_idx = idx_from_stream('patterns', 'host')
        depth_idx = idx_from_stream('infos', 'depth')
        content_type_idx = idx_from_stream('infos', 'content_type')
        infos_mask_idx = idx_from_stream('infos', 'infos_mask')
        resource_type_idx = idx_from_stream('properties', 'resource_type')

        http_code_idx = idx_from_stream('infos', 'http_code')
        delay2_idx = idx_from_stream('infos', 'delay2')

        inlinks_type_idx = idx_from_stream('inlinks', 'link_type')
        inlinks_follow_idx = idx_from_stream('inlinks', 'follow')

        outlinks_type_idx = idx_from_stream('outlinks', 'link_type')
        outlinks_src_idx = idx_from_stream('outlinks', 'id')
        outlinks_dst_idx = idx_from_stream('outlinks', 'dst_url_id')

        results = defaultdict(Counter)
        for result in group_left(left, **streams_ref):
            infos = result[2]['infos'][0]
            contents = result[2]['contents']
            outlinks = result[2]['outlinks']
            inlinks = result[2]['inlinks']

            # Reminder : 1 gzipped, 2 notused, 4 meta_noindex 8 meta_nofollow 16 has_canonical 32 bad canonical
            follow = not (4 & infos[infos_mask_idx] == 4)
            index = not (8 & infos[infos_mask_idx] == 8)

            key = (result[1][host_idx], result[2]['properties'][0][resource_type_idx], result[2]['infos'][0][content_type_idx], result[2]['infos'][0][depth_idx], follow, index)

            results[key]['pages_nb'] += 1
            results[key]['pages_code_%s' % infos[http_code_idx]] += 1

            code_type = 'pages_code_ok' if infos[http_code_idx] in ('200', '304') else 'pages_code_ko'
            results[key][code_type] += 1

            results[key][delay_to_range(infos[delay2_idx])] += 1
            results[key]['total_delay_ms'] += infos[delay2_idx]

            # Meta filled
            ct_idx = idx_from_stream('contents', 'content_type')
            for ct_id, ct_txt in CONTENT_TYPE_INDEX.iteritems():
                if len(filter(lambda i: i[ct_idx] == ct_id, contents)):
                    results[key]['%s_filled_nb' % ct_txt] += 1

            results[key]['outlinks_nb'] += len(filter(lambda i: i[outlinks_type_idx] == "a", outlinks))
            results[key]['redirections_nb'] += len(filter(lambda i: i[outlinks_type_idx].startswith("r"), outlinks))
            results[key]['redirections_nb'] += len(filter(lambda i: i[outlinks_type_idx].startswith("r"), outlinks))
            results[key]['canonical_filled_nb'] += len(filter(lambda i: i[outlinks_type_idx] == "canonical", outlinks))
            results[key]['canonical_duplicates_nb'] += len(filter(lambda i: i[outlinks_type_idx] == "canonical" and i[outlinks_src_idx] != i[outlinks_dst_idx], outlinks))

            results[key]['inlinks_nb'] += len(filter(lambda i: i[inlinks_type_idx] == "a", inlinks))
            for link in inlinks:
                if link[inlinks_follow_idx]:
                    results[key]['inlinks_follow_nb'] += 1
                else:
                    results[key]['inlinks_nofollow_nb'] += 1

        return results
