from cdf.streams.mapping import CONTENT_TYPE_NAME_TO_ID


SUGGEST_CLUSTERS = ['mixed']


CLUSTER_TYPE_TO_ID = {
    'pattern': {
        'host': 10,
        'path': 11,
        'qskey': 12,
    },
    'metadata': {
        CONTENT_TYPE_NAME_TO_ID['title']: 20,
        CONTENT_TYPE_NAME_TO_ID['h1']: 21,
        CONTENT_TYPE_NAME_TO_ID['h2']: 22,
        CONTENT_TYPE_NAME_TO_ID['h3']: 32
    }
}

# TODO this should be de deducted from ElasticSearch mapping
QUERY_FIELDS_DEPRECATED = {
    "url",
    "protocol",
    "path",
    "query_string",
    "query_string_keys",
    "date_crawled",
    "depth",
    "http_code",
    "delay1",
    "delay2",
    "bytesize",
    "patterns",

    "inlinks_internal_nb",
    "inlinks_internal_nb.total",
    "inlinks_internal_nb.total_unique",
    "inlinks_internal_nb.follow",
    "inlinks_internal_nb.follow_unique",
    "inlinks_internal_nb.nofollow",
    "inlinks_internal_nb.nofollow_combinations",

    "outlinks_internal_nb",
    "outlinks_internal_nb.total",
    "outlinks_internal_nb.total_unique",
    "outlinks_internal_nb.follow_unique",
    "outlinks_internal_nb.follow",
    "outlinks_internal_nb.nofollow_combinations",
    "outlinks_internal_nb.nofollow",

    "outlinks_external_nb",
    "outlinks_external_nb.total",
    "outlinks_external_nb.follow",
    "outlinks_external_nb.nofollow",
    "outlinks_external_nb.nofollow_combinations",

    "inlinks_internal",
    "outlinks_internal",
    "metadata",
    "metadata.title",
    "metadata.description",
    "metadata.h1",
    "metadata.h2",
    "metadata_nb",
    "metadata_nb.title",
    "metadata_nb.description",
    "metadata_nb.h1",
    "metadata_nb.h2",
    "metadata_duplicate",
    "metadata_duplicate.h1",
    "metadata_duplicate.title",
    "metadata_duplicate.description",
    "metadata_duplicate_nb.h1",
    "metadata_duplicate_nb.title",
    "metadata_duplicate_nb.description",
    "redirects_from_nb",
    "redirects_from",
    "redirects_to",

    "canonical_from_nb",
    "canonical_from",
    "canonical_to",
    "canonical_to_equal",

    "error_links",
    "error_links.3xx",
    "error_links.3xx.nb",
    "error_links.3xx.urls",
    "error_links.4xx",
    "error_links.4xx.nb",
    "error_links.4xx.urls",
    "error_links.5xx",
    "error_links.5xx.nb",
    "error_links.5xx.urls",
    "error_links.any",
    "error_links.any.nb",
}