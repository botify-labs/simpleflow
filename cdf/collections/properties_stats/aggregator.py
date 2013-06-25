from collections import defaultdict, Counter


from cdf.streams.utils import group_left, idx_from_stream


class PropertiesStatsAggregator(object):

    def __init__(self, stream_patterns, stream_infos, stream_properties):
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.stream_properties = stream_properties

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
                   "inlinks_nb": 10,
                   "inlinks_follow_nb": 10,
                   "inlinks_nofollow_nb": 0,
                   "outlinks_nb": 5,
                   "avg_delay": 800,
                   "delay_gte_500ms": 3,
                   "delay_gte_1s": 3,
                   "delay_gte_2s": 1,
                   "title_filled_nb": 6,
                   "title_global_unik_nb": 4,
                   "title_local_unik_nb": 5,
                   "desc_filled_nb": 6,
                   "desc_global_unik_nb": 4,
                   "desc_local_unik_nb": 5,
                   "h1_filled_nb": 6,
                   "h1_global_unik_nb": 4,
                   "h1_local_unik_nb": 5,
                   "canonical_nb": 3,
                   "duplicated_nb": 2,
                   "duplicated_unik_nb": 1,
            }
        }
        """

        left = (self.stream_patterns, 0)
        streams_ref = {'properties': (self.stream_properties, 0),
                       'infos': (self.stream_infos, 0)
                       }

        host_idx = idx_from_stream('patterns', 'host')
        depth_idx = idx_from_stream('infos', 'depth')
        resource_type_idx = idx_from_stream('properties', 'resource_type')

        http_code_idx = idx_from_stream('infos', 'http_code')
        delay1_idx = idx_from_stream('infos', 'delay1')
        delay2_idx = idx_from_stream('infos', 'delay2')

        results = defaultdict(Counter)
        for result in group_left(left, **streams_ref):
            infos = result[2]['infos'][0]

            key = (result[1][host_idx], result[2]['properties'][0][resource_type_idx], result[2]['infos'][0][depth_idx], True, True)

            results[key]['pages_nb'] += 1
            results[key]['pages_code_%s' % infos[http_code_idx]] += 1
        return results
