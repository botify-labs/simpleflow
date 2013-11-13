import copy
import itertools
import pyhash
hasher = pyhash.fnv1_32()

from collections import defaultdict, Counter
from pandas import DataFrame

from cdf.streams.mapping import CONTENT_TYPE_INDEX, MANDATORY_CONTENT_TYPES
from cdf.streams.utils import group_left, idx_from_stream
from cdf.collections.tagging_stats.constants import COUNTERS_FIELDS, CROSS_PROPERTIES_COLUMNS
from cdf.utils.dict import deep_update, flatten_dict
from cdf.utils.hashing import string_to_int64
from cdf.log import logger


def delay_to_range(delay):
    if delay >= 2000:
        return "delay_gte_2s"
    elif delay >= 1000:
        return "delay_from_1s_to_2s"
    elif delay >= 500:
        return "delay_from_500ms_to_1s"
    return "delay_lt_500ms"


def get_keys_from_stream_suggest(stream_suggest):
    """
    Return all possible combinations from stream suggest hashes
    Ex for hashes ["1", "2", "3"], it will return ["1", "2", "3", "1;2", "1;3", "2;3", "1;2;3"]
    """
    keys = ["0"]
    hashes = []
    for entry in stream_suggest:
        url_id, section, stype, query, query_hash = entry
        hashes.append(query_hash)
    for L in range(1, len(hashes) + 1):
        for subset in itertools.combinations(hashes, L):
            keys.append(';'.join(subset))
    return keys


class MetricsConsolidator(object):

    def __init__(self, part_stats):
        """
        Consolidate all dictionnaries coming from all PropertiesStats parts and aggregate counters by cross-property into one.
        """
        self.part_stats = part_stats

    def consolidate(self, return_flatten=True):
        """
        Return a dictionnary of aggregated values by cross-property

        {
            ("3233;3223;87742", "text/html", 1, 200, True, True): {
                "pages_nb": 6766,
                ...
            }
        }
        """
        results = defaultdict(Counter)
        for part_stat in self.part_stats:
            for s_ in part_stat:
                results[tuple(s_['cross_properties'])].update(Counter(flatten_dict(s_['counters'])))

        # Replace Counters objects by dicts
        if return_flatten:
            return {key: dict(values) for key, values in results.iteritems()}

        document = {}
        for cross_properties, counters in results.iteritems():
            document[cross_properties] = {}
            for k, v in counters.iteritems():
                deep_update(document[cross_properties], reduce(lambda x, y: {y: x}, reversed(k.split('.') + [v])))
        return document

    def get_dataframe(self):
        results = self.consolidate()

        def transform_dict(cross_property, d_):
            t_dict = dict(d_)
            t_dict.update({CROSS_PROPERTIES_COLUMNS[i]: value for i, value in enumerate(cross_property)})
            t_dict.update({k: t_dict.get(k, 0) for k in COUNTERS_FIELDS})
            return t_dict

        prepare_df_rows = []
        for key, counters in results.iteritems():
            prepare_df_rows.append(transform_dict(key, counters))

        df = DataFrame(prepare_df_rows)
        return df


class MetadataAggregator(object):

    """
    Streams injected in This class should be the entire dataset of a crawl to ensure that the unicity of metadatas are valid
    """
    def __init__(self, stream_patterns, stream_suggest, stream_contents, stream_infos):
        self.stream_patterns = stream_patterns
        self.stream_suggest = stream_suggest
        self.stream_contents = stream_contents
        self.stream_infos = stream_infos

    def get(self):
        """
        Return a tuple of dictionnaries :

        For each dict, an index "keys" with the following tuple :
        (host, resource_type, content_type, depth, http_code, index, follow)
         str,  str,           str,          int,   http_code, bool,  bool

        Ex :
        [
            {
                "keys": ["www.site.com", "/article", "text/html", 1, 200, True, True],
                "counters": {
                   "title_global_unik_nb": 4,
                   "title_local_unik_nb": 5,
                   "desc_filled_nb": 6,
                   "desc_global_unik_nb": 4,
                   "desc_local_unik_nb": 5,
                   "h1_filled_nb": 6,
                   "h1_global_unik_nb": 4,
                   "h1_local_unik_nb": 5
                    "h1_filled_nb": 3,
                    "h1_unique_nb": 5,
                    ...
            },
            {
                ...
            }
        ]
        """
        left = (self.stream_patterns, 0)
        streams_ref = {'suggest': (self.stream_suggest, 0),
                       'contents': (self.stream_contents, idx_from_stream('contents', 'id')),
                       'infos': (self.stream_infos, idx_from_stream('infos', 'id')),
                       }

        hashes_global = {ct_id: defaultdict(set) for ct_id in CONTENT_TYPE_INDEX.iterkeys()}

        content_meta_type_idx = idx_from_stream('contents', 'content_type')
        content_hash_idx = idx_from_stream('contents', 'hash')
        depth_idx = idx_from_stream('infos', 'depth')
        http_code_idx = idx_from_stream('infos', 'http_code')
        infos_mask_idx = idx_from_stream('infos', 'infos_mask')
        content_type_idx = idx_from_stream('infos', 'content_type')

        results = defaultdict(Counter)

        for result in group_left(left, **streams_ref):
            # Reminder : 1 gzipped, 2 notused, 4 meta_noindex 8 meta_nofollow 16 has_canonical 32 bad canonical
            infos = result[2]['infos'][0]
            index = not (4 & infos[infos_mask_idx] == 4)
            follow = not (8 & infos[infos_mask_idx] == 8)

            http_code = infos[http_code_idx]
            if http_code != 200:
                continue

            for suggest_key in get_keys_from_stream_suggest(result[2]["suggest"]):
                key = (suggest_key,
                       infos[content_type_idx],
                       infos[depth_idx],
                       infos[http_code_idx],
                       index,
                       follow
                       )
                hash_key = hasher(','.join(str(k) for k in key))
                contents = result[2]['contents']

                # For each url, we check if it has correctly title, description and h1 filled
                # If not, we'll consider that the url has not enough metadata

                metadata_score = 0
                # Meta filled
                for ct_id, ct_txt in CONTENT_TYPE_INDEX.iteritems():
                    if len(filter(lambda i: i[content_meta_type_idx] == ct_id, contents)):
                        results[key]['metadata_nb.%s.filled' % ct_txt] += 1
                        if ct_txt in MANDATORY_CONTENT_TYPES:
                            metadata_score += 1
                    else:
                        results[key]['metadata_nb.%s.not_filled' % ct_txt] += 1

                if metadata_score < 3:
                    results[key]['metadata_nb.not_enough'] += 1

                # Fetch --first-- hash from each content type and watch add it to hashes set
                ct_found = set()
                for content in contents:
                    ct_id = content[content_meta_type_idx]
                    # If ct_i is already in ct_found, so it's the not the first content
                    if ct_id not in ct_found:
                        ct_found.add(ct_id)
                        hashes_global[ct_id][content[content_hash_idx]].add(hash_key)

        # Concatenate results
        results = dict(results)
        final_results = []
        for key in results.iterkeys():
            # Transform Counter to dict
            counters = copy.copy(dict(results[key]))
            hash_key = hasher(','.join(str(k) for k in key))
            for ct_id, ct_txt in CONTENT_TYPE_INDEX.iteritems():
                # If [ct_txt]_filled_nb not exists, we create the fields
                if not 'metadata_nb.%s.filled' % ct_txt in counters:
                    counters['metadata_nb.%s.filled' % ct_txt] = 0
                    counters['metadata_nb.%s.unique' % ct_txt] = 0
                else:
                    # We fetch all set where there is only the hash_key (that means uniq)
                    counters['metadata_nb.%s.unique' % ct_txt] = len(filter(lambda i: i == set((hash_key,)), hashes_global[ct_id].itervalues()))
                if not 'metadata_nb.%s.not_filled' % ct_txt in counters:
                    counters['metadata_nb.%s.not_filled' % ct_txt] = 0

            if not 'metadata_nb.not_enough' in counters:
                counters['metadata_nb.not_enough'] = 0
            final_results.append(
                {
                    "cross_properties": key,
                    "counters": counters
                }
            )
        return final_results
