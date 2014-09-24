import abc

from cdf.log import logger
from cdf.analysis.urls.utils import get_es_id, get_url_id
from cdf.metadata.url.backend import ELASTICSEARCH_BACKEND
from cdf.utils.dict import path_in_dict, get_subdict_from_path, update_path_in_dict
from cdf.features.links.helpers.masks import follow_mask
from cdf.query.constants import MGET_CHUNKS_SIZE, SUB_AGG, METRIC_AGG_PREFIX


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
    if path_in_dict('redirect.to.url', es_result):
        target = get_subdict_from_path('redirect.to.url', es_result)
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
            del target['http_code']

        # delete unused field
        target.pop('url_id', None)


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
                urls.append([id_to_url.get(uid)[0], http_code])
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
            self.es = kwargs['es']
            self.crawl_id = kwargs['crawl_id']
        else:
            self.fields = query.fields
            self.es = query.es_handler
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
        resolved = self.es.mget(
            ids,
            fields=['url', 'http_code'],
            routing=self.crawl_id,
            chunk_size=MGET_CHUNKS_SIZE
        )

        urls.update({
            get_url_id(_id): (doc['url'], doc['http_code'])
            for _id, doc, found in resolved if found
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
    def _transform_terms(cls, bucket):
        return bucket['key']

    @classmethod
    def _is_range(cls, bucket):
        return 'to' in bucket or 'from' in bucket

    @classmethod
    def _transform_range(cls, bucket):
        bucket_key = {}
        for k in ('from', 'to'):
            if k in bucket:
                bucket_key[k] = int(bucket.pop(k))
        return bucket_key

    @classmethod
    def _has_buckets(cls, bucket):
        return 'buckets' in bucket

    @classmethod
    def _is_single_bucket_agg(cls, agg_result):
        """Single bucket aggregation has no `buckets` sub-structure
        """
        return not 'buckets' in agg_result

    @classmethod
    # TODO further simplifies this function
    # TODO unify term usage (bucket, agg_result set etc.)
    def parse_complex_result(cls, agg_result):
        """
        Return a list of key/value dictionnaries
        Ex :
            [
                {"key": ["a", "b", "e"], "metrics": [100]},
                {"key": ["a", "c", "e"], "metrics": [120]}
            ]
        """
        if SUB_AGG in agg_result:
            _transform_func = cls._transform_range if cls._is_range(agg_result) else cls._transform_terms
            sub_bucket = cls.parse_complex_result(agg_result[SUB_AGG])
            bucket_key = _transform_func(agg_result)
            for results in sub_bucket:
                results["key"].insert(0, bucket_key)
            return sub_bucket

        if cls._is_terms(agg_result) or cls._is_range(agg_result):
            _transform_func = cls._transform_range if cls._is_range(agg_result) else cls._transform_terms
            result = {"key": [_transform_func(agg_result)], "metrics": []}
            for key in sorted([k for k in agg_result.keys() if k.startswith(METRIC_AGG_PREFIX)]):
                result["metrics"].append(agg_result[key]["value"])
            return [result]

        if cls._has_buckets(agg_result):
            buckets_list = []
            for sub_bucket in agg_result["buckets"]:
                buckets_list += cls.parse_complex_result(sub_bucket)
            return buckets_list

    @classmethod
    def parse_simple_result(cls, agg_result):
        results = []
        for _, result in sorted(agg_result.iteritems()):
            # need this check here since there could be
            # other things in the ES result
            if isinstance(result, dict):
                results.append(result['value'])
        return results

    @classmethod
    def parse_single_agg_result(cls, agg_result):
        """Parse a single aggregation result

        The result to parse can be:
          - simple, single bucket aggregation
          - multi-bucket aggregation (with explicit `group_by` clause)
        """
        if cls._is_single_bucket_agg(agg_result):
            # if `group_by` clause is missing
            # no `buckets` in the ES result
            return {'metrics': cls.parse_simple_result(agg_result)}
        else:
            # if there's a `group_by` clause
            # need to parse group buckets
            return {'groups': cls.parse_complex_result(agg_result)}

    def transform(self):
        res = []
        for _, agg_result in sorted(self.agg_results.iteritems()):
            res.append(self.parse_single_agg_result(agg_result))
        return res


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
    return AggregationTransformer(agg_results).transform()
