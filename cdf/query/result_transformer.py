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
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def transform(self):
        """In-place transformation of ES search results"""
        pass


# url_id extract (prepare) and transform functions for different field
# Essentially, they just go down in the result dict and extract/transform
# the list of url_ids

# all `transform` strategies here modifies ES result IN PLACE

class IdResolutionStrategy(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def extract(self, result):
        pass

    @abc.abstractmethod
    def transform(self, result, id_to_url):
        pass

    @classmethod
    def _extract_list_ids(cls, es_result, path, extract_func=None):
        if path_in_dict(path, es_result):
            ids = get_subdict_from_path(path, es_result)
            if extract_func:
                # apply custom logic on list for extraction
                return map(extract_func, ids)
            else:
                return ids
        else:
            return []


    @classmethod
    def _extract_single_id(cls, es_result, path, extract_func=None):
        if path_in_dict(path, es_result):
            url_id = get_subdict_from_path(path, es_result)
            if extract_func:
                return filter(None, [extract_func(url_id)])
            else:
                return [url_id]
        else:
            return []

    @classmethod
    def _report_not_found(cls, url_id):
        logger.warning(
            'url_id {} could not be found in data storage',
            url_id
        )


class LinksStrategy(IdResolutionStrategy):
    def __init__(self, link_type, prefix=''):
        self.field = prefix + '{}.urls'.format(link_type)

    @classmethod
    def extract_link_id(cls, link_tuple):
        return link_tuple[0]

    def extract(self, result):
        return self._extract_list_ids(result, self.field, self.extract_link_id)

    def transform(self, result, id_to_url):
        if path_in_dict(self.field, result):
            target = get_subdict_from_path(self.field, result)

            urls = []
            for url_id, mask in target:
                mask = follow_mask(mask)
                url, http_code = id_to_url.get(url_id, (None, None))
                if not url:
                    self._report_not_found(url_id)
                    continue
                if mask != ['follow']:
                    mask = ["nofollow_{}".format(m) for m in mask]
                urls.append({
                    'url': {
                        'url': url,
                        'crawled': http_code > 0
                    },
                    'status': mask
                })
            del target[:]
            target.extend(urls)


class ErrorLinkStrategy(IdResolutionStrategy):
    def __init__(self, error_type, prefix=''):
        self.error_type = error_type
        self.field = prefix + 'outlinks_errors.{}.urls'.format(self.error_type)

    def extract(self, result):
        return self._extract_list_ids(result, self.field)

    def transform(self, result, id_to_url):
        if path_in_dict(self.field, result):
            target = get_subdict_from_path(self.field, result)
            urls = []
            for url_id in target:
                if url_id not in id_to_url:
                    self._report_not_found(url_id)
                    continue
                urls.append(id_to_url.get(url_id)[0])
            del target[:]
            target.extend(urls)


class MetaDuplicateStrategy(IdResolutionStrategy):
    def __init__(self, meta_type, prefix=''):
        self.field = prefix + 'metadata.{}.duplicates.urls'.format(meta_type)

    def extract(self, result):
        return self._extract_list_ids(result, self.field)

    def transform(self, result, id_to_url):
        if path_in_dict(self.field, result):
            target = get_subdict_from_path(self.field, result)

            urls = []
            for url_id in target:
                if url_id not in id_to_url:
                    self._report_not_found(url_id)
                    continue
                urls.append({'url': id_to_url.get(url_id)[0], 'crawled': True})
            del target[:]
            target.extend(urls)


class RedirectToStrategy(IdResolutionStrategy):
    def __init__(self, prefix=''):
        self.field = prefix + 'redirect.to.url'
        self.extract_field = self.field + '.url_id'

    def extract(self, result):
        return self._extract_single_id(result, self.extract_field)

    def transform(self, result, id_to_url):
        if path_in_dict(self.field, result):
            target = get_subdict_from_path('redirect.to.url', result)
            if target.get('url_id', 0) > 0:
                # to an internal url
                url_id = target['url_id']
                if url_id not in id_to_url:
                    self._report_not_found(url_id)
                    return

                url, http_code = id_to_url.get(url_id)
                target['url'] = url
                target['crawled'] = True if http_code > 0 else False
                del target['http_code']

            # delete unused field
            target.pop('url_id', None)


class RedirectFromStrategy(IdResolutionStrategy):
    def __init__(self, prefix=''):
        self.field = prefix + 'redirect.from.urls'

    def extract(self, result):
        return self._extract_list_ids(result, self.field, lambda l: l[0])

    def transform(self, result, id_to_url):
        if path_in_dict(self.field, result):
            target = get_subdict_from_path(self.field, result)

            urls = []
            for url_id, http_code in target:
                if url_id not in id_to_url:
                    self._report_not_found(url_id)
                    continue
                urls.append([id_to_url.get(url_id)[0], http_code])
            del target[:]
            target.extend(urls)


class CanonicalToStrategy(IdResolutionStrategy):
    def __init__(self, prefix=''):
        self.field = prefix + 'canonical.to.url'
        self.extract_field = self.field + '.url_id'

    def extract(self, result):
        return self._extract_single_id(result, self.extract_field)

    def transform(self, result, id_to_url):
        # 3 cases
        #   - not_crawled url
        #   - normal url
        #   - external url
        if path_in_dict(self.field, result):
            target = get_subdict_from_path(self.field, result)
            if target.get('url_id', 0) > 0:
                # to an internal url
                url_id = target['url_id']
                if url_id not in id_to_url:
                    self._report_not_found(url_id)
                    return

                url, http_code = id_to_url.get(url_id)
                target['url'] = url
                target['crawled'] = True if http_code > 0 else False

            # delete unused field
            target.pop('url_id', None)


class CanonicalFromStrategy(IdResolutionStrategy):
    def __init__(self, prefix=''):
        self.field = prefix + 'canonical.from.urls'

    def extract(self, result):
        return self._extract_list_ids(result, self.field)

    def transform(self, result, id_to_url):
        path = 'canonical.from.urls'
        if path_in_dict(path, result):
            target = get_subdict_from_path(path, result)
            urls = []
            for url_id in target:
                if url_id not in id_to_url:
                    self._report_not_found(url_id)
                    continue
                urls.append(id_to_url.get(url_id)[0])

            del target[:]
            target.extend(urls)


class IdToUrlTransformer(ResultTransformer):
    """Replace all `url_id` in ElasticSearch result by their
    corresponding complete url"""

    FIELD_TRANSFORM_STRATEGY = {
        'outlinks_errors.3xx.urls': ErrorLinkStrategy('3xx'),
        'outlinks_errors.4xx.urls': ErrorLinkStrategy('4xx'),
        'outlinks_errors.5xx.urls': ErrorLinkStrategy('5xx'),

        'inlinks_internal.urls': LinksStrategy('inlinks_internal'),
        'outlinks_internal.urls': LinksStrategy('outlinks_internal'),

        'canonical.to.url': CanonicalToStrategy(),
        'canonical.from.urls': CanonicalFromStrategy(),

        'redirect.to.url': RedirectToStrategy(),
        'redirect.from.urls': RedirectFromStrategy(),

        'metadata.title.duplicates.urls': MetaDuplicateStrategy('title'),
        'metadata.h1.duplicates.urls': MetaDuplicateStrategy('h1'),
        'metadata.description.duplicates.urls': MetaDuplicateStrategy('description')
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
                id_list = self.FIELD_TRANSFORM_STRATEGY[field].extract(result)
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
                trans_func = self.FIELD_TRANSFORM_STRATEGY[field].transform
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
