# -*- coding: utf-8 -*-
import copy
import itertools
import pyhash
hasher = pyhash.fnv1_32()

from collections import defaultdict, Counter
from pandas import DataFrame

from cdf.streams.mapping import CONTENT_TYPE_INDEX, MANDATORY_CONTENT_TYPES, MANDATORY_CONTENT_TYPES_IDS
from cdf.streams.utils import group_left, idx_from_stream
from cdf.collections.suggestions.constants import COUNTERS_FIELDS, CROSS_PROPERTIES_COLUMNS
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
        url_id, section, stype, query_hash = entry
        hashes.append(query_hash)
    # Todo : refactor to send directly the generate key
    if hashes:
        keys.append(';'.join(sorted(hashes)))
    return keys
    #for L in range(1, len(hashes) + 1):
    #    for subset in itertools.combinations(hashes, L):
    #        keys.append(';'.join(sorted(subset)))
    #return keys


class MetricsAggregator(object):

    def __init__(self, stream_patterns, stream_infos, stream_suggest, stream_outlinks, stream_inlinks, stream_contents_duplicate):
        self.stream_patterns = stream_patterns
        self.stream_infos = stream_infos
        self.stream_suggest = stream_suggest
        self.stream_inlinks = stream_inlinks
        self.stream_outlinks = stream_outlinks
        self.stream_contents_duplicate = stream_contents_duplicate

    def get(self):
        """
        Return a tuple of dictionaries
        Values are a sub-dictonnary with fields :
            * `cross_properties`, a tuple with following format :
            (query_id, content_type, depth, http_code, index, follow)
            str,  str,           str,          int,   http_code, bool,  bool
            * `counters` : a dictionary of counters

        Ex :
        {
            "cross_properties": ["1233223;11222211;33322", "text/html", 1, 200, True, True],
            "counters": {
                "pages_nb": 10,
                "redirections_nb": 0,
                "inlinks_internal_nb": {
                    "total": 10,
                    "follow": 8,
                    "follow_unique": 6,
                    "nofollow": 2,
                    "nofollow_combinations": {
                        "link_meta": 1,
                        "link": 1
                    }
                },
                "outlinks_internal_nb": {
                    "total": 10,
                    "follow": 8,
                    "follow_unique": 6,
                    "nofollow": 2,
                    "nofollow_combinations": {
                        "link_meta": 1,
                        "link": 1
                    }
                },
                "outlinks_external_nb": {
                    "total": 10,
                    "follow": 8,
                    "follow_unique": 6,
                    "nofollow": 2,
                    "nofollow_combinations": {
                        "link_meta": 1,
                        "link": 1
                    }
                },
                "total_delay_ms": 3400,
                "avg_delay": 800,
                "delay_gte_500ms": 3,
                "delay_gte_1s": 3,
                "delay_gte_2s": 1,
                "canonical_nb": {
                    "filled": 3,
                    "equal": 1,
                    "not_filled": 0
                }
            }
        }
        """

        left = (self.stream_patterns, 0)
        streams_ref = {'suggest': (self.stream_suggest, 0),
                       'infos': (self.stream_infos, 0),
                       'inlinks': (self.stream_inlinks, idx_from_stream('inlinks', 'id')),
                       'outlinks': (self.stream_outlinks, idx_from_stream('outlinks', 'id')),
                       'contents_duplicate': (self.stream_contents_duplicate, idx_from_stream('contents_duplicate', 'id'))
                       }

        depth_idx = idx_from_stream('infos', 'depth')
        content_type_idx = idx_from_stream('infos', 'content_type')
        infos_mask_idx = idx_from_stream('infos', 'infos_mask')

        http_code_idx = idx_from_stream('infos', 'http_code')
        delay2_idx = idx_from_stream('infos', 'delay2')

        inlinks_type_idx = idx_from_stream('inlinks', 'link_type')
        inlinks_src_idx = idx_from_stream('inlinks', 'src_url_id')

        outlinks_type_idx = idx_from_stream('outlinks', 'link_type')
        outlinks_src_idx = idx_from_stream('outlinks', 'id')
        outlinks_dst_idx = idx_from_stream('outlinks', 'dst_url_id')

        content_duplicate_meta_type_idx = idx_from_stream('contents_duplicate', 'content_type')

        counter_dict = {}
        for field in COUNTERS_FIELDS:
            deep_update(counter_dict, reduce(lambda x, y: {y: x}, reversed(field.split('.') + [0])))

        results = dict()

        def increment_results_for_key(key):
            results[key]['pages_nb'] += 1
            results[key][delay_to_range(infos[delay2_idx])] += 1
            results[key]['total_delay_ms'] += infos[delay2_idx]
            results[key]['redirections_nb'] += len(filter(lambda i: i[outlinks_type_idx].startswith("r"), outlinks))

            # Get the first canonical tag found (a page may be have 2 canonicals tags by mistake
            canonicals = filter(lambda i: i[outlinks_type_idx] == "canonical", outlinks)
            if canonicals:
                canonical = canonicals[0]
                results[key]['canonical_nb']['filled'] += 1
                if canonical[outlinks_src_idx] == canonical[outlinks_dst_idx]:
                    results[key]['canonical_nb']['equal'] += 1
                else:
                    results[key]['canonical_nb']['not_equal'] += 1

            results[key]['canonical_nb']['incoming'] += len(filter(lambda i: i[inlinks_type_idx] == "canonical", inlinks))

            # Store metadata counters
            """
            "metadata_nb": {
                "h1": {
                    "filled": 2,
                    "not_filled": 1,
                    "unique": 1,
                },
                "title": {
                    ...
                },
                "not_enough": 1
            }
            """
            # If the url has not h1, title or description, we considered that there is not enough metadata
            meta_types_available = [i[content_duplicate_meta_type_idx] for i in contents_duplicate]
            if len(meta_types_available) < 3:
                results[key]['metadata_nb']['not_enough'] += 1

            for entry in contents_duplicate:
                url_id, ct_id, nb_filled, nb_duplicates, is_first_url, _ = entry
                # Meta filled
                metadata_dict = results[key]['metadata_nb'][CONTENT_TYPE_INDEX[ct_id]]
                metadata_dict['filled'] += 1
                if nb_duplicates == 1 and ct_id in MANDATORY_CONTENT_TYPES_IDS:
                    metadata_dict['unique'] += 1

            # If a metadata type is not set, we increment not_filled
            for ct_id in MANDATORY_CONTENT_TYPES_IDS:
                if ct_id not in meta_types_available:
                    results[key]['metadata_nb'][CONTENT_TYPE_INDEX[ct_id]]['not_filled'] += 1

            # Store inlinks and outlinks counters
            """
            "outlinks_external_nb": {
                    "total": 10,
                    "follow": 8,
                    "follow_unique": 6,
                    "nofollow": 2,
                    "nofollow_combinations": {
                        "link_meta": 1,
                        "link": 1
                    }
                },
            """
            for link_direction in ('inlinks', 'outlinks'):
                follow_idx = idx_from_stream(link_direction, 'follow')
                type_idx = inlinks_type_idx if link_direction == "inlinks" else outlinks_type_idx
                if link_direction == "outlinks":
                    dst_idx = idx_from_stream(link_direction, 'dst_url_id')
                    external_idx = idx_from_stream(link_direction, 'external_url')

                # Count follow_unique links
                follow_urls = set()

                if link_direction == "outlinks":
                    unique_idx = outlinks_dst_idx
                    is_inlink = False
                else:
                    unique_idx = inlinks_src_idx
                    is_inlink = True

                for link in result[2][link_direction]:
                    if link[type_idx] == "a":
                        # If is_inlink, it's necessarily as we don't crawl the web :)
                        is_internal = is_inlink or link[dst_idx] > 0
                        url_id = link[unique_idx]

                        """
                        If the link is external and the follow_key is robots,
                        That means that the url is finally internal (not linked once in follow)
                        """
                        if not is_internal and link_direction == "outlinks" and "robots" in link[follow_idx]:
                            is_internal = True
                            url_id = string_to_int64(link[external_idx])

                        # Many statuses possible for an url, we concatenate them after a sort an split them with a double underscore
                        follow_key = '_'.join(sorted(link[follow_idx]))
                        counter_key = '{}_{}_nb'.format(link_direction, "internal" if is_internal else "external")
                        results[key][counter_key]['total'] += 1
                        results[key][counter_key]['follow' if follow_key == 'follow' else 'nofollow'] += 1

                        if is_internal and follow_key == "follow":
                            follow_urls.add(url_id)

                        if follow_key != 'follow':
                            if follow_key not in results[key][counter_key]['nofollow_combinations']:
                                results[key][counter_key]['nofollow_combinations'][follow_key] = 1
                            else:
                                results[key][counter_key]['nofollow_combinations'][follow_key] += 1

                if len(follow_urls) > 0:
                    results[key]['{}_internal_nb'.format(link_direction)]['follow_unique'] += len(follow_urls)

        for k, result in enumerate(group_left(left, **streams_ref)):
            if k % 1000 == 999:
                logger.info('MetricAggregator iter {}'.format(k))
            #if k == 2:
            #    break
            infos = result[2]['infos'][0]
            outlinks = result[2]['outlinks']
            inlinks = result[2]['inlinks']
            contents_duplicate = result[2]['contents_duplicate']

            # Reminder : 1 gzipped, 2 notused, 4 meta_noindex 8 meta_nofollow 16 has_canonical 32 bad canonical
            index = not (4 & infos[infos_mask_idx] == 4)
            follow = not (8 & infos[infos_mask_idx] == 8)

            http_code = infos[http_code_idx]
            in_queue = http_code in (0, 1, 2)
            # If the page has not been crawled, we skip it
            if in_queue:
                continue

            suggest_keys = get_keys_from_stream_suggest(result[2]['suggest'])

            for suggest_key in suggest_keys:
                key = (suggest_key,
                       infos[content_type_idx],
                       infos[depth_idx],
                       http_code,
                       index,
                       follow)

                if key not in results:
                    results[key] = copy.deepcopy(counter_dict)
                increment_results_for_key(key)

        # Transform defaultdict to dict
        final_results = []
        for key, counters in results.iteritems():
            final_results.append({"cross_properties": list(key), "counters": counters})
        return final_results


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
