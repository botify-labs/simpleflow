# -*- coding: utf-8 -*-
import copy
import pyhash
hasher = pyhash.fnv1_32()

from collections import defaultdict, Counter
from pandas import DataFrame

from cdf.features.semantic_metadata.settings import CONTENT_TYPE_INDEX, MANDATORY_CONTENT_TYPES_IDS
from cdf.core.streams.utils import group_left
from cdf.metadata.aggregates import COUNTERS_FIELDS, CROSS_PROPERTIES_COLUMNS
from cdf.utils.dict import deep_update, flatten_dict
from cdf.log import logger
from cdf.features.main.streams import IdStreamDef, InfosStreamDef
from cdf.features.links.streams import (
    OutcanonicalCountersStreamDef, IncanonicalCountersStreamDef,
    InredirectCountersStreamDef
)
from cdf.features.semantic_metadata.streams import ContentsDuplicateStreamDef


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
    Return possible combinations from stream suggest hashes
    Basically it returns every hash plus "0" which correspond to
    "all pages"
    Ex for hashes ["1", "2", "3"], it will return ["0, "1", "2", "3"]
    """
    keys = {query_hash for _, query_hash in stream_suggest}
    keys.add("0")
    return list(keys)


def get_http_code_kind(http_code):
    """
    :return: the range key for the http code, eg. '3xx' for 301
    """
    if 300 <= http_code < 400:
        return '3xx'
    elif 400 <= http_code < 500:
        return '4xx'
    elif http_code >= 500:
        return '5xx'


class MetricsAggregator(object):
    def __init__(self, streams):
        self.streams = streams

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
            "cross_properties": [17376626532, "text/html", 1, 200, True, True],
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

        right_streams = {}
        for s in self.streams:
            if isinstance(s, IdStreamDef):
                left_stream = s
            else:
                right_streams[s.__class__.__name__].append((s, 0))

        depth_idx = InfosStreamDef.field_idx('depth')
        content_type_idx = InfosStreamDef.field_idx('content_type')
        infos_mask_idx = InfosStreamDef.field_idx('infos_mask')

        http_code_idx = InfosStreamDef.field_idx('http_code')
        delay2_idx = InfosStreamDef.field_idx('delay_last_byte')

        outcanonical_equals_idx = OutcanonicalCountersStreamDef.field_idx('equals')
        incanonical_score_idx = IncanonicalCountersStreamDef.field_idx('score')
        inredirect_score_idx = InredirectCountersStreamDef.field_idx('score')
        content_duplicate_meta_type_idx = ContentsDuplicateStreamDef.field_idx('content_type')

        counter_dict = {}
        for field in COUNTERS_FIELDS:
            deep_update(counter_dict, reduce(lambda x, y: {y: x}, reversed(field.split('.') + [0])))

        results = dict()

        def aggregate_badlinks(target_dict, http_code, score):
            target_dict['any'] += score
            target_dict[get_http_code_kind(http_code)] += score

        def inlink_follow_dist(target_dict, score_unique):
            if score_unique <= 3:
                keys = [str(score_unique), 'lt_10', 'lte_3']
            elif score_unique < 10:
                keys = [str(score_unique), 'lt_10']
            elif 10 <= score_unique < 20:
                keys = ['10_to_19']
            elif 20 <= score_unique < 30:
                keys = ['20_to_29']
            elif 30 <= score_unique < 40:
                keys = ['30_to_39']
            elif 40 <= score_unique < 50:
                keys = ['40_to_49']
            elif 50 <= score_unique < 100:
                keys = ['50_to_99']
            elif 100 <= score_unique < 1000:
                keys = ['100_to_999']
            elif 1000 <= score_unique < 10000:
                keys = ['1000_to_9999']
            elif 10000 <= score_unique < 100000:
                keys = ['10000_to_99999']
            elif 100000 <= score_unique < 1000000:
                keys = ['100000_to_999999']
            elif score_unique >= 1000000:
                keys = ['gte_1M']
            for k in keys:
                target_dict['follow_distribution_urls'][k] += 1
                target_dict['follow_distribution_links'][k] += score_unique

        def increment_results_for_key(key):
            results[key]['pages_nb'] += 1
            results[key][delay_to_range(infos[delay2_idx])] += 1
            results[key]['total_delay_ms'] += infos[delay2_idx]

            if outredirect:
                results[key]['redirects_to_nb'] += 1

            if inredirects:
                results[key]['redirects_from_nb'] += inredirects[inredirect_score_idx]

            if outcanonical:
                results[key]['canonical_nb']['filled'] += 1
                if outcanonical[outcanonical_equals_idx]:
                    results[key]['canonical_nb']['equal'] += 1
                else:
                    results[key]['canonical_nb']['not_equal'] += 1
            else:
                results[key]['canonical_nb']['not_filled'] += 1

            if incanonicals:
                results[key]['canonical_nb']['incoming'] += incanonicals[incanonical_score_idx]

            if len(inlinks) > 0:
                results[key]['inlinks_internal_nb']['total_urls'] += 1

            # control flags for `follow_urls` and `total_urls`
            has_in_follow = False
            has_out_follow = False
            has_out_internal = False

            # Store metadata counters

            # "metadata_nb": {
            #     "h1": {
            #         "filled": 2,
            #         "not_filled": 1,
            #         "unique": 1,
            #     },
            #     "title": {
            #         ...
            #     },
            #     "not_enough": 1
            # }

            # If the url has not h1, title or description, we considered that there is not enough metadata
            # TODO if we have meta types 1,2,3, is it `not_enough` ??
            meta_types_available = [i[content_duplicate_meta_type_idx] for i in contents_duplicate]
            if len(meta_types_available) < 3:
                results[key]['metadata_nb']['not_enough'] += 1

            for entry in contents_duplicate:
                url_id, ct_id, nb_filled, nb_duplicates, is_first_url, _ = entry
                # Meta filled
                metadata_dict = results[key]['metadata_nb'][CONTENT_TYPE_INDEX[ct_id]]
                metadata_dict['filled'] += 1
                if ct_id in MANDATORY_CONTENT_TYPES_IDS:
                    if nb_duplicates == 0:
                        metadata_dict['unique'] += 1
                    else:
                        metadata_dict['duplicate'] += 1

            # If a metadata type is not set, we increment not_filled
            for ct_id in MANDATORY_CONTENT_TYPES_IDS:
                if ct_id not in meta_types_available:
                    results[key]['metadata_nb'][CONTENT_TYPE_INDEX[ct_id]]['not_filled'] += 1

            # Store inlinks and outlinks counters

            # "outlinks_external_nb": {
            #     "total": 10,
            #     "follow": 8,
            #     "follow_unique": 6,
            #     "nofollow": 2,
            #     "nofollow_combinations": {
            #         "link_meta": 1,
            #         "link": 1
            #     }
            # },

            for entry in inlinks:
                url_id, follow, score, score_unique = entry
                counter_key = 'inlinks_internal_nb'
                follow_key = '_'.join(sorted(follow))
                results[key][counter_key]['total'] += score
                results[key][counter_key]['follow' if follow_key == 'follow' else 'nofollow'] += score

                results[key][counter_key]['total_unique'] += score_unique

                if follow_key == 'follow':
                    if not has_in_follow:
                        has_in_follow = True
                        results[key][counter_key]['follow_urls'] += 1
                    results[key][counter_key]['follow_unique'] += score_unique
                    inlink_follow_dist(results[key][counter_key], score_unique)
                else:
                    results[key][counter_key]['nofollow_unique'] += score_unique
                    if follow_key not in results[key][counter_key]['nofollow_combinations']:
                        results[key][counter_key]['nofollow_combinations'][follow_key] = score
                        results[key][counter_key]['nofollow_combinations_unique'][follow_key] = score_unique
                    else:
                        results[key][counter_key]['nofollow_combinations'][follow_key] += score
                        results[key][counter_key]['nofollow_combinations_unique'][follow_key] += score_unique

            for entry in outlinks:
                url_id, follow, is_internal, score, score_unique = entry
                counter_key = 'outlinks_{}_nb'.format("internal" if is_internal else "external")
                follow_key = '_'.join(sorted(follow))
                results[key][counter_key]['total'] += score
                results[key][counter_key]['follow' if follow_key == 'follow' else 'nofollow'] += score

                # `unique` link information is needed only for `internal` links
                if is_internal:
                    results[key][counter_key]['total_unique'] += score_unique
                    if not has_out_internal:
                        has_out_internal = True
                        results[key][counter_key]['total_urls'] += 1

                # follow
                if follow_key == 'follow':
                    if is_internal:
                        results[key][counter_key]['follow_unique'] += score_unique
                        if not has_out_follow:
                            has_out_follow = True
                            results[key][counter_key]['follow_urls'] += 1
                # nofollow
                else:
                    if is_internal:
                        results[key][counter_key]['nofollow_unique'] += score_unique
                    if follow_key not in results[key][counter_key]['nofollow_combinations']:
                        results[key][counter_key]['nofollow_combinations'][follow_key] = score
                        if is_internal:
                            results[key][counter_key]['nofollow_combinations_unique'][follow_key] = score_unique
                    else:
                        results[key][counter_key]['nofollow_combinations'][follow_key] += score
                        if is_internal:
                            results[key][counter_key]['nofollow_combinations_unique'][follow_key] += score_unique

            for entry in badlinks:
                _, http_code, count = entry
                aggregate_badlinks(results[key]['error_links'], http_code, count)

        for k, result in enumerate(group_left(left_stream, **right_streams)):
            if k % 1000 == 999:
                logger.info('MetricAggregator iter {}'.format(k))
            #if k == 2:
            #    break
            infos = result[2]['infos'][0]
            outlinks = result[2]['out_links_counters']
            inlinks = result[2]['in_links_counters']
            contents_duplicate = result[2]['contents_duplicate']

            outcanonical = result[2]['out_canonical_counters'][0] if result[2]['out_canonical_counters'] else None
            outredirect = result[2]['out_redirect_counters'][0] if result[2]['out_redirect_counters'] else None

            incanonicals = result[2]['in_canonical_counters'][0] if result[2]['in_canonical_counters'] else None
            inredirects = result[2]['in_redirect_counters'][0] if result[2]['in_redirect_counters'] else None

            badlinks = result[2]['badlinks_counters']

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
                content_type = infos[content_type_idx]
                if content_type == '?':
                    content_type = 'not-set'

                key = (suggest_key,
                       content_type,
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
        Consolidate all dictionnaries coming from all PropertiesStats parts
        and aggregate counters by cross-property into one.
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
