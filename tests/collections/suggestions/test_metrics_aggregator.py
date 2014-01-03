import unittest

from cdf.collections.suggestions.aggregator import (MetricsAggregator,
                                                    get_keys_from_stream_suggest)


class TestMetricsAggregator(unittest.TestCase):
    def setUp(self):
        self.param_streams = [
            "stream_patterns",
            "stream_infos",
            "stream_suggest",
            "stream_contents_duplicate",
            "stream_outlinks_counters",
            "stream_outcanonical_counters",
            "stream_outredirect_counters",
            "stream_inlinks_counters",
            "stream_incanonical_counters",
            "stream_inredirect_counters",
            "stream_badlinks_counters"
        ]

        # Canonical streams
        # These two streams are also MANDATORY
        self.stream_patterns = [
            [1, 'http', 'www.site.com', '/path/a.html', ''],
            [2, 'http', 'www.site.com', '/path/b.html', ''],
            [3, 'http', 'www.site.com', '/path/c.html', ''],
            [4, 'http', 'www.site.com', '/path/d.html', ''],
            [5, 'http', 'www.site.com', '/path/f.html', ''],
        ]

        self.stream_infos = [
            [1, 1, 'text/html', 0, 1, 200, 1024, 100, 100],
            [2, 1, 'text/html', 0, 1, 200, 1024, 100, 2200],
            [3, 1, 'text/html', 0, 1, 200, 1024, 100, 100],
            [4, 1, 'text/html', 0, 1, 200, 1024, 100, 500],
            [5, 1, 'text/html', 0, 1, 200, 1024, 100, 1500]
        ]

        # By default all stream other than `patterns` and `infos` are empty
        self.kwargs = {k: iter([]) for k in self.param_streams}
        self.kwargs['stream_patterns'] = iter(self.stream_patterns)
        self.kwargs['stream_infos'] = iter(self.stream_infos)

    def tearDown(self):
        pass

    def test_get_key_from_suggestions(self):
        # if a url doesn't fit in any cluster/pattern", it "should at
        # least be considered in cluster `whole site`", that i"s", `0`
        self.assertEqual(get_keys_from_stream_suggest(iter([])),
                         ['0'])

        self.assertEqual(sorted(get_keys_from_stream_suggest(iter([(1, '1'), (1, '2')]))),
                         ['0', '1', '2'])

    def test_empty(self):
        result = list(MetricsAggregator(**{k: iter([]) for k in self.param_streams}).get())

        # noting in, nothing out
        self.assertEqual(result, [])

    def test_basics(self):
        result = list(MetricsAggregator(**self.kwargs).get())
        target = result[0]['counters']
        self.assertEqual(target['pages_nb'], 5)
        self.assertEqual(target['total_delay_ms'], 4400)

        self.assertEqual(target['delay_lt_500ms'], 2)
        self.assertEqual(target['delay_from_500ms_to_1s'], 1)
        self.assertEqual(target['delay_from_1s_to_2s'], 1)
        self.assertEqual(target['delay_gte_2s'], 1)

    def test_cross_property_keys(self):
        """Aggregator should generate correctly the cross property keys

        Keys are in format:
            (query_id: str, content_type: str, depth: int,
            http_code: int, index: bool, follow: bool)
        """
        stream_infos = [
            [1, 1, 'text/html', 0, 1, 200, 1024, 100, 100],
            [2, 1, 'text/html', 1, 1, 300, 1024, 100, 100],
            [3, 8, 'text/html', 1, 1, 400, 1024, 100, 100],
            [4, 1, 'text/html', 0, 1, 200, 1024, 100, 100],
            [5, 4, 'text/html', 3, 1, 200, 1024, 100, 100]
        ]

        stream_suggest = [
            [1, '1']  # so url 1 results in 2 cross properties
        ]

        self.kwargs['stream_infos'] = iter(stream_infos)
        self.kwargs['stream_suggest'] = iter(stream_suggest)
        result = list(MetricsAggregator(**self.kwargs).get())
        properties = [property['cross_properties'] for property in result]

        expected = [
            ['1', 'text/html', 0, 200, True, True],
            ['0', 'text/html', 0, 200, True, True],
            ['0', 'text/html', 1, 300, True, True],
            ['0', 'text/html', 1, 400, True, False],
            ['0', 'text/html', 3, 200, False, True]
        ]

        self.assertEqual(sorted(expected), sorted(properties))

    def test_badlinks(self):
        stream_badlinks_counters = [
            [1, 300, 10],
            [1, 400, 101],
            [1, 500, 2],
            [2, 505, 3],
        ]

        self.kwargs['stream_badlinks_counters'] = iter(stream_badlinks_counters)
        result = list(MetricsAggregator(**self.kwargs).get())

        self.assertEqual(1, len(result))
        target = result[0]['counters']['error_links']
        self.assertEqual(target['3xx'], 10)
        self.assertEqual(target['4xx'], 101)
        self.assertEqual(target['5xx'], 5)
        self.assertEqual(target['any'], 116)

    def test_inlink_follow_dist(self):
        stream_inlink_counters = [
            [1, ['follow'], 100, 99],
            [2, ['follow'], 3, 2],
            [3, ['follow'], 12, 9],
            [4, ['follow'], 10**7, 10**7],
        ]

        self.kwargs['stream_inlinks_counters'] = iter(stream_inlink_counters)
        result = list(MetricsAggregator(**self.kwargs).get())

        self.assertEqual(1, len(result))
        urls_target = result[0]['counters']['inlinks_internal_nb']['follow_distribution_urls']
        links_target = result[0]['counters']['inlinks_internal_nb']['follow_distribution_links']

        self.assertEqual(urls_target['2'], 1)
        self.assertEqual(urls_target['9'], 1)
        self.assertEqual(urls_target['lte_3'], 1)
        self.assertEqual(urls_target['lt_10'], 2)
        self.assertEqual(urls_target['50_to_99'], 1)
        self.assertEqual(urls_target['gte_1M'], 1)

        self.assertEqual(links_target['2'], 2)
        self.assertEqual(links_target['9'], 9)
        self.assertEqual(links_target['lte_3'], 2)
        self.assertEqual(links_target['lt_10'], 11)
        self.assertEqual(links_target['50_to_99'], 99)
        self.assertEqual(links_target['gte_1M'], 10**7)

    def test_contents_duplicates(self):
        """
        (url_id, content_type, filled_nb, dup_nb, if_first, dup_urls)
        """
        stream_contents_duplicate = [
            [1, 1, 2, 3, True, [10, 11]],
            [1, 2, 10, 1, True, [11]],
            [1, 4, 1, 0, True, []],
            [2, 1, 1, 1, True, [15]],
        ]
        self.kwargs['stream_contents_duplicate'] = iter(stream_contents_duplicate)
        result = list(MetricsAggregator(**self.kwargs).get())
        target = result[0]['counters']['metadata_nb']

        # only url 1 has metadata, others haven't got enough
        self.assertEqual(target['not_enough'], 4)

        self.assertEqual(target['title']['not_filled'], 3)
        self.assertEqual(target['title']['filled'], 2)
        self.assertEqual(target['title']['unique'], 0)
        self.assertEqual(target['title']['duplicate'], 2)
        self.assertEqual(target['description']['unique'], 1)

    def test_out_links(self):
        stream_outlinks_counters = [
            [1, ['follow'], True, 10, 5],
            [1, ['follow'], False, 5, 3],
            [1, ['robots', 'link', 'meta'], True, 3, 2],
            [2, ['follow'], True, 30, 25],
            [2, ['meta'], False, 10, 8],
            [2, ['meta', 'link'], False, 2, 1],
        ]

        self.kwargs['stream_outlinks_counters'] = iter(stream_outlinks_counters)
        result = list(MetricsAggregator(**self.kwargs).get())

        nfc_key = 'nofollow_combinations'
        target_ext = result[0]['counters']['outlinks_external_nb']
        target_int = result[0]['counters']['outlinks_internal_nb']

        # counters for internal outlinks
        self.assertEqual(target_int['total'], 43)
        self.assertEqual(target_int['follow'], 40)
        self.assertEqual(target_int['follow_unique'], 30)
        self.assertEqual(target_int['nofollow'], 3)
        self.assertEqual(target_int['nofollow_unique'], 2)
        self.assertEqual(target_int['total_unique'], 32)
        self.assertEqual(target_int['total_urls'], 2)
        self.assertEqual(target_int['follow_urls'], 2)

        # counters for external outlinks
        self.assertEqual(target_ext['total'], 17)
        self.assertEqual(target_ext['follow'], 5)
        self.assertEqual(target_ext['nofollow'], 12)

        # asserts on `nofollow_combinations`
        # `nofollow_combinations_unique` calculated with `score_unique`
        nfc_key = 'nofollow_combinations_unique'
        self.assertEqual(target_int[nfc_key]['link_meta_robots'], 2)
        self.assertEqual(target_ext[nfc_key]['meta'], 8)
        self.assertEqual(target_ext[nfc_key]['link_meta'], 1)
        self.assertEqual(target_ext[nfc_key]['link'], 0)

        # `nofollow_combinations` calculated with `score`
        nfc_key = 'nofollow_combinations'
        self.assertEqual(target_int[nfc_key]['link_meta_robots'], 3)
        self.assertEqual(target_ext[nfc_key]['meta'], 10)
        self.assertEqual(target_ext[nfc_key]['link_meta'], 2)
        self.assertEqual(target_ext[nfc_key]['link'], 0)

    def test_in_links(self):
        stream_inlinks_counters = [
            [1, ['follow'], 10, 5],
            [1, ['link', 'meta'], 3, 2],
            [2, ['follow'], 30, 25],
        ]
        self.kwargs['stream_inlinks_counters'] = iter(stream_inlinks_counters)
        result = list(MetricsAggregator(**self.kwargs).get())
        target = result[0]['counters']['inlinks_internal_nb']

        self.assertEqual(target['total'], 43)
        self.assertEqual(target['follow'], 40)
        self.assertEqual(target['follow_unique'], 30)
        self.assertEqual(target['nofollow'], 3)
        self.assertEqual(target['nofollow_unique'], 2)
        self.assertEqual(target['total_unique'], 32)
        self.assertEqual(target['total_urls'], 2)
        self.assertEqual(target['follow_urls'], 2)

        nfc_key = 'nofollow_combinations'
        self.assertEqual(target[nfc_key]['link_meta'], 3)

        nfc_key = 'nofollow_combinations_unique'
        self.assertEqual(target[nfc_key]['link_meta'], 2)

    def test_redirects(self):
        stream_outredirect_counters = [
            [1, True],
            [2, True],
            [3, False],
            [4, False],
        ]

        stream_inredirect_counters = [
            [1, 50],
            [2, 10**7]
        ]

        self.kwargs['stream_outredirect_counters'] = iter(stream_outredirect_counters)
        self.kwargs['stream_inredirect_counters'] = iter(stream_inredirect_counters)
        result = list(MetricsAggregator(**self.kwargs).get())
        target = result[0]['counters']

        self.assertEqual(target['redirects_to_nb'], 4)
        self.assertEqual(target['redirects_from_nb'], 10**7 + 50)

    # TODO `not_filled`
    def test_canonicals(self):
        """Canonicals counters should be aggregated correctly
        """
        stream_infos = [
            [1, 1, 'text/html', 0, 1, 200, 1024, 100, 100],
            [2, 1, 'text/html', 0, 1, 200, 1024, 100, 2200],
            [3, 1, 'text/html', 0, 1, 200, 1024, 100, 100],
            [4, 1, 'text/html', 0, 1, 200, 1024, 100, 500],
            [5, 1, 'text/html', 0, 1, 200, 1024, 100, 1500]
        ]

        stream_outcanonical_counters = [
            [1, True],
            [2, False],
            [3, True],
        ]

        stream_incanonical_counters = [
            [1, 10],
            [2, 10**7]
        ]

        self.kwargs['stream_outcanonical_counters'] = iter(stream_outcanonical_counters)
        self.kwargs['stream_incanonical_counters'] = iter(stream_incanonical_counters)
        self.kwargs['stream_infos'] = iter(stream_infos)
        result = list(MetricsAggregator(**self.kwargs).get())
        target = result[0]['counters']['canonical_nb']

        self.assertEqual(target['equal'], 2)
        self.assertEqual(target['not_equal'], 1)
        self.assertEqual(target['filled'], 3)
        #self.assertEqual(target['not_filled'], 2)
        self.assertEqual(target['incoming'], 10**7 + 10)
