import abc

from cdf.log import logger
from cdf.analysis.urls.utils import get_es_id, get_url_id
from cdf.metadata.url import ELASTICSEARCH_BACKEND
from cdf.utils.dict import path_in_dict, get_subdict_from_path, update_path_in_dict
from cdf.metadata.raw.masks import follow_mask
from cdf.query.constants import MGET_CHUNKS_SIZE


class ResultTransformer(object):
    """Post-processing for ElasticSearch search results
    """

    @abc.abstractmethod
    def transform(self):
        """In-place transformation of ES search results"""
        pass


# url_id extract (prepare) and transform functions for different field
# Essentially, they just go down in the result dict and extract/transform
# the list of url_ids

# all `transform` functions here modifies ES result IN PLACE

def _extract_list_ids(es_result, path, extract_func=None):
    if path_in_dict(path, es_result):
        ids = get_subdict_from_path(path, es_result)
        if extract_func:
            # apply custom logic on list for extraction
            return map(extract_func, ids)
        else:
            return ids
    else:
        return []


def _extract_single_id(es_result, path, extract_func=None):
    if path_in_dict(path, es_result):
        url_id = get_subdict_from_path(path, es_result)
        if extract_func:
            return filter(None, [extract_func(url_id)])
        else:
            return [url_id]
    else:
        return []


def _transform_list_field(es_result, path, func):
    if path_in_dict(es_result, path):
        original = get_subdict_from_path(es_result, path)
        # in place list comprehension with `None` check
        original[:] = [res for res in (func(item) for item in original) if res]


def _transform_single_field(es_result, path, func):
    if path_in_dict(es_result, path):
        original = get_subdict_from_path(es_result, path)
        update_path_in_dict(path, func(original), es_result)


def _transform_error_links(es_result, id_to_url, code_kind):
    path = 'outlinks_errors'
    if path_in_dict(path, es_result):
        target = get_subdict_from_path(path, es_result)
        if code_kind in target:
            original = target[code_kind]['urls']
            urls = []
            for url_id in original:
                if url_id not in id_to_url:
                    logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
                    continue
                urls.append(id_to_url.get(url_id)[0])
                # in-place
            target[code_kind]['urls'] = urls


def _transform_inlinks(es_results, id_to_url):
    if path_in_dict('inlinks_internal.urls', es_results):
        target = es_results['inlinks_internal']
        target['urls'] = _transform_link_items(target['urls'], id_to_url)


def _transform_outlinks(es_results, id_to_url):
    if path_in_dict('outlinks_internal.urls', es_results):
        target = es_results['outlinks_internal']
        target['urls'] = _transform_link_items(target['urls'], id_to_url)


def _transform_link_items(items, id_to_url):
    res = []
    for item in items:
        mask = follow_mask(item[1])
        url_id = item[0]
        url, http_code = id_to_url.get(url_id, (None, None))
        if not url:
            logger.warning(
                "Urlid %d could not be found in elasticsearch.", url_id)
            continue
        if mask != ['follow']:
            mask = ["nofollow_{}".format(m) for m in mask]
        res.append({
            'url': {
                'url': url,
                'crawled': http_code > 0
            },
            'status': mask
        })
    return res


def _transform_single_link_to(es_result, id_to_url, path):
    if path_in_dict(path, es_result):
        target = get_subdict_from_path(path, es_result)
        if target.get('url_id', 0) > 0:
            # to an internal url
            url_id = target['url_id']
            if url_id not in id_to_url:
                logger.warning(
                    "Urlid %d could not be found in elasticsearch.", url_id)
                return

            url, http_code = id_to_url.get(url_id)
            target['url'] = url
            target['crawled'] = True if http_code > 0 else False

        # delete unused field
        target.pop('url_id', None)


# 3 cases
#   - not_crawled url
#   - normal url
#   - external url
# same for redirect_to
def _transform_canonical_to(es_result, id_to_url):
    _transform_single_link_to(es_result, id_to_url, 'canonical.to.url')


def _transform_redirects_to(es_result, id_to_url):
    _transform_single_link_to(es_result, id_to_url, 'redirect.to.url')


def _transform_canonical_from(es_result, id_to_url):
    path = 'canonical.from'
    if path_in_dict(path, es_result):
        target = get_subdict_from_path(path, es_result)
        if 'urls' in target:
            urls = []
            for uid in target['urls']:
                if uid not in id_to_url:
                    logger.warning(
                        "Urlid %d could not be found in elasticsearch.", uid)
                    continue
                urls.append(id_to_url.get(uid)[0])
            target['urls'] = urls


def _transform_redirects_from(es_result, id_to_url):
    path = 'redirect.from'
    if path_in_dict(path, es_result):
        target = get_subdict_from_path(path, es_result)
        if 'urls' in target:
            urls = []
            for uid, http_code in target['urls']:
                if uid not in id_to_url:
                    logger.warning(
                        "Urlid %d could not be found in elasticsearch.", uid)
                    continue
                urls.append({
                    'http_code': http_code,
                    'url': {
                        'url': id_to_url.get(uid)[0],
                        'crawled': True
                    }
                })
            target['urls'] = urls


def _transform_metadata_duplicate(es_result, id_to_url, meta_type):
    path = 'metadata.{}.duplicates'.format(meta_type)
    if path_in_dict(path, es_result):
        target = get_subdict_from_path(path, es_result)
        if 'urls' in target:
            urls = []
            for uid in target['urls']:
                if uid not in id_to_url:
                    logger.warning(
                        "Urlid %d could not be found in elasticsearch.", uid)
                    continue
                url, http_code = id_to_url.get(uid)
                urls.append({
                    'url': url,
                    'crawled': True  # only crawled url has metadata
                })
            target['urls'] = urls


class IdToUrlTransformer(ResultTransformer):
    """Replace all `url_id` in ElasticSearch result by their
    corresponding complete url"""

    FIELD_TRANSFORM_STRATEGY = {
        'outlinks_errors.3xx.urls': {
            'extract': lambda res: _extract_list_ids(res, 'outlinks_errors.3xx.urls'),
            'transform': lambda res, id_to_url: _transform_error_links(res, id_to_url, '3xx')
        },
        'outlinks_errors.4xx.urls': {
            'extract': lambda res: _extract_list_ids(res, 'outlinks_errors.4xx.urls'),
            'transform': lambda res, id_to_url: _transform_error_links(res, id_to_url, '4xx')
        },
        'outlinks_errors.5xx.urls': {
            'extract': lambda res: _extract_list_ids(res, 'outlinks_errors.5xx.urls'),
            'transform': lambda res, id_to_url: _transform_error_links(res, id_to_url, '5xx')
        },

        'inlinks_internal.urls': {
            'extract': lambda res: _extract_list_ids(res, 'inlinks_internal.urls', lambda l: l[0]),
            'transform': lambda res, id_to_url: _transform_inlinks(res, id_to_url)
        },
        'outlinks_internal.urls': {
            'extract': lambda res: _extract_list_ids(res, 'outlinks_internal.urls', lambda l: l[0]),
            'transform': lambda res, id_to_url: _transform_outlinks(res, id_to_url)
        },

        'canonical.to.url': {
            'extract': lambda res: _extract_single_id(res, 'canonical.to.url.url_id'),
            'transform': lambda res, id_to_url: _transform_canonical_to(res, id_to_url)
        },
        'canonical.from.urls': {
            'extract': lambda res: _extract_list_ids(res, 'canonical.from.urls'),
            'transform': lambda res, id_to_url: _transform_canonical_from(res, id_to_url)
        },

        'redirect.to.url': {
            'extract': lambda res: _extract_single_id(res, 'redirect.to.url.url_id'),
            'transform': lambda res, id_to_url: _transform_redirects_to(res, id_to_url)
        },
        'redirect.from.urls': {
            'extract': lambda res: _extract_list_ids(res, 'redirect.from.urls', lambda l: l[0]),
            'transform': lambda res, id_to_url: _transform_redirects_from(res, id_to_url)
        },

        'metadata.title.duplicates.urls': {
            'extract': lambda res: _extract_list_ids(res, 'metadata.title.duplicates.urls'),
            'transform': lambda res, id_to_url: _transform_metadata_duplicate(res, id_to_url, 'title')
        },
        'metadata.h1.duplicates.urls': {
            'extract': lambda res: _extract_list_ids(res, 'metadata.h1.duplicates.urls'),
            'transform': lambda res, id_to_url: _transform_metadata_duplicate(res, id_to_url, 'h1')
        },
        'metadata.description.duplicates.urls': {
            'extract': lambda res: _extract_list_ids(res, 'metadata.description.duplicates.urls'),
            'transform': lambda res, id_to_url: _transform_metadata_duplicate(res, id_to_url, 'description')
        }
    }

    def __init__(self, es_result, query=None, backend=None, **kwargs):
        if not query:
            # a list of query fields, flatten
            self.fields = kwargs['fields']
            # ES info.
            self.es_conn = kwargs['es_conn']
            self.es_index = kwargs['es_index']
            self.es_doctype = kwargs['es_doctype']
            self.crawl_id = kwargs['crawl_id']
        else:
            # TODO manage `fields` in query
            self.fields = query.fields
            self.es_conn = query.search_backend
            self.es_index = query.es_index
            self.es_doctype = query.es_doc_type
            self.crawl_id = query.crawl_id

        self.backend = backend
        # ES search result to transform
        # a list of dict (`fields`)
        self.results = es_result
        # url ids to be resolved
        self.ids = set()
        # id to url lookup
        self.id_to_url = {}

        self.fields_to_transform = set()

    def prepare(self):
        # find all fields that needs to be transformed
        for field in self.fields:
            if not self.backend.has_child(field):
                if field in self.FIELD_TRANSFORM_STRATEGY:
                    self.fields_to_transform.add(field)
            else:
                for child in self.backend.get_children(field):
                    if child in self.FIELD_TRANSFORM_STRATEGY:
                        self.fields_to_transform.add(child)

        for result in self.results:
            # call each field's extractor for url ids
            for field in self.fields_to_transform:
                if not path_in_dict(field, result):
                    continue
                id_list = self.FIELD_TRANSFORM_STRATEGY[field]['extract'](result)
                for url_id in id_list:
                    self.ids.add(url_id)

    def _get_urls_from_ids(self, ids):
        """
        Return a dict with url_id as key a a tuple (url, http_code) as value
        """
        urls = {}
        for i in xrange(0, len(ids), MGET_CHUNKS_SIZE):
            resolved_urls = self.es_conn.mget(body={"ids": ids},
                                              index=self.es_index,
                                              doc_type=self.es_doctype,
                                              routing=self.crawl_id,
                                              preference=self.crawl_id,
                                              _source=["url", "http_code"])
            urls.update({
                get_url_id(url['_id']): (url['_source']['url'], url['_source']['http_code'])
                for url in resolved_urls['docs'] if url["found"]
            })
        return urls

    def transform(self):
        self.prepare()
        if len(self.ids) == 0:
            # nothing to transform
            return

        # Fill the (url_id -> url) lookup table
        # Also fetch the http_code
        # Assumption: we don't do query over multiple crawls, one site at a time
        id_to_url = self._get_urls_from_ids(
            [get_es_id(self.crawl_id, url_id) for url_id in self.ids])

        for result in self.results:
            # Resolve urls in each field found by prepare
            for field in self.fields_to_transform:
                if not path_in_dict(field, result):
                    continue
                trans_func = self.FIELD_TRANSFORM_STRATEGY[field]['transform']
                # Reminder, in-place transformation
                trans_func(result, id_to_url)

        return self.results


class DefaultValueTransformer(ResultTransformer):
    """Assign default value to some missing field"""

    def __init__(self, es_result, query=None, backend=None, **kwargs):
        # ES search result to transform
        # a list of dict (`fields`)
        self.results = es_result
        if query:
            # fields to retrieve
            self.fields = query.fields
        else:
            self.fields = kwargs['fields']

        # Strategies here defines the default value of all
        # children fields
        if backend is not None:
            self.field_default_values = backend.field_default_value()
            self.backend = backend

    def transform(self):
        # For each result document
        for result in self.results:
            # Check all children of query's required fields
            # Update the result document for default value if any of the
            # children is missing in that document
            for required_field in self.fields:
                if not self.backend.has_child(required_field):
                    if not path_in_dict(required_field, result) and \
                                    required_field in self.field_default_values:
                        default = self.field_default_values[required_field]
                        # in-place update
                        update_path_in_dict(required_field, default, result)
                else:
                    for child in self.backend.get_children(required_field):
                        if not path_in_dict(child, result) and \
                                        child in self.field_default_values:
                            default = self.field_default_values[child]
                            # in-place update
                            update_path_in_dict(child, default, result)


class ExternalUrlNormalizer(ResultTransformer):
    """External urls should also be marked as not crawled"""

    _TARGET_FIELDS = {
        'redirect.to.url',
        'canonical.to.url'
    }

    def __init__(self, es_result, query=None, **kwargs):
        self.results = es_result
        if query:
            self.fields = query.fields
        else:
            self.fields = kwargs['fields']

    def transform(self):
        for result in self.results:
            for field in self._TARGET_FIELDS:
                if path_in_dict(field, result):
                    target = get_subdict_from_path(field, result)
                    if 'url_str' in target:
                        # signals that this url is not crawled (external)
                        target['crawled'] = False
                        # normalize `url_str` to `url`
                        target['url'] = target['url_str']
                        target.pop('url_str')


class AggregationTransformer(ResultTransformer):
    """Unify aggregation result format"""

    def __init__(self, agg_results):
        self.agg_results = agg_results

    @classmethod
    def _is_terms(cls, bucket):
        return 'key' in bucket

    @classmethod
    def _is_range(cls, bucket):
        return 'to' in bucket or 'from' in bucket

    # simple solution for the moment
    #   - no nested bucket
    #   - only support count metric
    def transform(self):
        for name, results in self.agg_results.iteritems():
            if 'buckets' in results:
                for bucket in results['buckets']:
                    # arrange aggregation keys
                    if self._is_terms(bucket):
                        bucket['key'] = [bucket.pop('key')]
                    if self._is_range(bucket):
                        key = {}
                        for k in ('from', 'to'):
                            if k in bucket:
                                key[k] = int(bucket.pop(k))
                        bucket['key'] = [key]

                    # rename `doc_count` to `count`
                    bucket['count'] = bucket.pop('doc_count')

                # rename `buckets` to `groups`
                results['groups'] = results.pop('buckets')


def transform_result(results, query, backend=ELASTICSEARCH_BACKEND):
    """Walk through every result and transform it"""
    transformers = [
        ExternalUrlNormalizer(results, query),
        IdToUrlTransformer(results, query, backend),
        DefaultValueTransformer(results, query, backend)
    ]
    for trans in transformers:
        trans.transform()


def transform_aggregation_result(agg_results):
    AggregationTransformer(agg_results).transform()