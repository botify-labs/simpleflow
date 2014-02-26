import abc

from cdf.metadata.url import has_child, get_children
from cdf.metadata.url.es_backend_utils import generate_default_value_lookup
from cdf.metadata.url import URLS_DATA_FORMAT_DEFINITION
from cdf.log import logger
from cdf.analysis.urls.utils import get_es_id, get_url_id
from cdf.utils.dict import path_in_dict, get_subdict_from_path, update_path_in_dict
from cdf.metadata.raw.masks import follow_mask


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
def _prepare_error_links(es_result, code_kind):
    key = 'error_links'
    if key in es_result:
        if code_kind in es_result[key]:
            # `urls` field is guaranteed to be a list, even if
            # it's a single url
            return es_result[key][code_kind]['urls']


def _transform_error_links(es_result, id_to_url, code_kind):
    key = 'error_links'
    if key in es_result:
        if code_kind in es_result[key]:
            original = es_result[key][code_kind]
            urls = []
            for url_id in original['urls']:
                if url_id not in id_to_url:
                    logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
                    continue
                urls.append(id_to_url.get(url_id)[0])
                # in-place
            original['urls'] = urls


def _prepare_links(es_result, link_kind):
    ids = []
    for link_item in es_result.get(link_kind, []):
        ids.append(link_item[0])
    return ids


def _transform_links(es_result, id_to_url, link_kind):
    if link_kind in es_result:
        res = []
        for link_item in es_result[link_kind]:
            mask = follow_mask(link_item[1])
            url_id = link_item[0]
            if url_id not in id_to_url and link_kind != 'outlinks_internal':
                logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
            url, http_code = id_to_url.get(url_id, (None, None))
            if not url:
                continue
            if mask != ['follow']:
                mask = ["nofollow_{}".format(m) for m in mask]
            res.append({
                'url': {
                    'url': url,
                    'crawled': http_code > 0
                },
                'status': mask,
                'nb_links': link_item[2]
            })
            es_result[link_kind] = res
    else:
        es_result[link_kind] = []


# for `canonical_to` and `redirects_to`
def _prepare_single_id(es_result, field):
    if path_in_dict(field, es_result):
        url_item = get_subdict_from_path(field, es_result)
        if 'url_id' in url_item:
            return [url_item['url_id']]
        else:
            return []


def _transform_single_link_to(es_result, id_to_url, field):
    if field in es_result:
        if 'url_id' in es_result[field]:
            url_id = es_result[field]['url_id']
            if url_id not in id_to_url:
                logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
                return
            url, http_code = id_to_url.get(url_id)
            if http_code > 0:
                es_result[field] = {
                    'url': url,
                    'crawled': True
                }
            else:
                es_result[field] = {
                    'url': url,
                    'crawled': False
                }
        else:
            es_result[field] = {
                'url': es_result[field]['url'],
                'crawled': False
            }


# 3 cases
#   - not_crawled url
#   - normal url
#   - external url
# same for redirect_to
def _transform_canonical_to(es_result, id_to_url):
    _transform_single_link_to(es_result, id_to_url, 'canonical_to')


def _transform_redirects_to(es_result, id_to_url):
    _transform_single_link_to(es_result, id_to_url, 'redirects_to')


def _prepare_canonical_from(es_result):
    field = 'canonical_from'
    if field in es_result:
        return es_result[field]
    else:
        return []


def _transform_canonical_from(es_result, id_to_url):
    field = 'canonical_from'
    if field in es_result:
        urls = []
        for url_id in es_result[field]:
            if url_id not in id_to_url:
                logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
                continue
            urls.append(id_to_url.get(url_id)[0])
        es_result[field] = urls


def _prepare_redirects_from(es_result):
    field = 'redirects_from'
    if field in es_result:
        return map(lambda item: item['url_id'],
                   es_result[field])
    else:
        return []


def _transform_redirects_from(es_result, id_to_url):
    field = 'redirects_from'
    if field in es_result:
        urls = []
        for item in es_result[field]:
            url_id = item['url_id']
            if url_id not in id_to_url:
                logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
                continue
            urls.append({
                'http_code': item['http_code'],
                'url': {
                    'url': id_to_url.get(url_id)[0],
                    'crawled': True
                }
            })
        es_result[field] = urls


def _prepare_metadata_duplicate(es_result, meta_type):
    field = 'metadata_duplicate'
    if field in es_result and meta_type in es_result[field]:
        return es_result[field][meta_type]
    else:
        return []


def _transform_metadata_duplicate(es_result, id_to_url, meta_type):
    field = 'metadata_duplicate'
    if field in es_result and meta_type in es_result[field]:
        urls = []
        for url_id in es_result[field][meta_type]:
            if url_id not in id_to_url:
                logger.warning("Urlid %d could not be found in elasticsearch.", url_id)
                continue
            url, http_code = id_to_url.get(url_id)
            urls.append({
                'url': url,
                'crawled': True  # only crawled url has metadata
            })
        es_result[field][meta_type] = urls


class IdToUrlTransformer(ResultTransformer):
    """Replace all `url_id` in ElasticSearch result by their
    corresponding complete url"""

    FIELD_TRANSFORM_STRATEGY = {
        'error_links.3xx.urls': {
            'extract': lambda res: _prepare_error_links(res, '3xx'),
            'transform': lambda res, id_to_url: _transform_error_links(res, id_to_url, '3xx')
        },
        'error_links.4xx.urls': {
            'extract': lambda res: _prepare_error_links(res, '4xx'),
            'transform': lambda res, id_to_url: _transform_error_links(res, id_to_url, '4xx')
        },
        'error_links.5xx.urls': {
            'extract': lambda res: _prepare_error_links(res, '5xx'),
            'transform': lambda res, id_to_url: _transform_error_links(res, id_to_url, '5xx')
        },
        'inlinks_internal': {
            'extract': lambda res: _prepare_links(res, 'inlinks_internal'),
            'transform': lambda res, id_to_url: _transform_links(res, id_to_url, 'inlinks_internal')
        },
        'outlinks_internal': {
            'extract': lambda res: _prepare_links(res, 'outlinks_internal'),
            'transform': lambda res, id_to_url: _transform_links(res, id_to_url, 'outlinks_internal')
        },
        'canonical_to': {
            'extract': lambda res: _prepare_single_id(res, 'canonical_to'),
            'transform': lambda res, id_to_url: _transform_canonical_to(res, id_to_url)
        },
        'canonical_from': {
            'extract': lambda res: _prepare_canonical_from(res),
            'transform': lambda res, id_to_url: _transform_canonical_from(res, id_to_url)
        },
        'redirects_to': {
            'extract': lambda res: _prepare_single_id(res, 'redirects_to'),
            'transform': lambda res, id_to_url: _transform_redirects_to(res, id_to_url)
        },
        'redirects_from': {
            'extract': lambda res: _prepare_redirects_from(res),
            'transform': lambda res, id_to_url: _transform_redirects_from(res, id_to_url)
        },
        'metadata_duplicate.title': {
            'extract': lambda res: _prepare_metadata_duplicate(res, 'title'),
            'transform': lambda res, id_to_url: _transform_metadata_duplicate(res, id_to_url, 'title')
        },
        'metadata_duplicate.h1': {
            'extract': lambda res: _prepare_metadata_duplicate(res, 'h1'),
            'transform': lambda res, id_to_url: _transform_metadata_duplicate(res, id_to_url, 'h1')
        },
        'metadata_duplicate.description': {
            'extract': lambda res: _prepare_metadata_duplicate(res, 'description'),
            'transform': lambda res, id_to_url: _transform_metadata_duplicate(res, id_to_url, 'description')
        }
    }

    def __init__(self, es_result, query=None, **kwargs):
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
            if not has_child(field):
                if field in self.FIELD_TRANSFORM_STRATEGY:
                    self.fields_to_transform.add(field)
            else:
                for child in get_children(field):
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

    def transform(self):
        self.prepare()
        if len(self.ids) == 0:
            # nothing to transform
            return

        # Resolve urls by requesting ElasticSearch
        resolved_urls = self.es_conn.mget(body={"ids": list(get_es_id(self.crawl_id, url_id)
                                                            for url_id in self.ids)},
                                          index=self.es_index,
                                          doc_type=self.es_doctype,
                                          routing=self.crawl_id,
                                          preference=self.crawl_id,
                                          fields=["url", "http_code"])


        resolved_urls['docs'] = [url for url in resolved_urls['docs'] if url["exists"]]


        # Fill the (url_id -> url) lookup table
        # Also fetch the http_code
        # Assumption: we don't do query over multiple crawls, one site at a time
        self.id_to_url = {get_url_id(es_url['_id']):
                              (es_url['fields']['url'], es_url['fields']['http_code']) for
                          es_url in resolved_urls['docs']}

        for result in self.results:
            # Resolve urls in each field found by prepare
            for field in self.fields_to_transform:
                if not path_in_dict(field, result):
                    continue
                trans_func = self.FIELD_TRANSFORM_STRATEGY[field]['transform']
                # Reminder, in-place transformation
                trans_func(result, self.id_to_url)

        return self.results


class DefaultValueTransformer(ResultTransformer):
    """Assign default value to some missing field"""

    # TODO default_value for nofollow_combinations
    # Strategies here defines the default value of all
    # children fields
    _DEFAULT_VALUE_STRATEGY = generate_default_value_lookup(
        URLS_DATA_FORMAT_DEFINITION)

    def __init__(self, es_result, query=None, **kwargs):
        # ES search result to transform
        # a list of dict (`fields`)
        self.results = es_result
        if query:
            # fields to retrieve
            self.fields = query.fields
        else:
            self.fields = kwargs['fields']

    def transform(self):
        # For each result document
        for result in self.results:
            # Check all children of query's required fields
            # Update the result document for default value if any of the
            # children is missing in that document
            for required_field in self.fields:
                if not has_child(required_field):
                    if not path_in_dict(required_field, result) and \
                                    required_field in self._DEFAULT_VALUE_STRATEGY:
                        default = self._DEFAULT_VALUE_STRATEGY[required_field]
                        # in-place update
                        update_path_in_dict(required_field, default, result)
                else:
                    for child in get_children(required_field):
                        if not path_in_dict(child, result) and \
                                        child in self._DEFAULT_VALUE_STRATEGY:
                            default = self._DEFAULT_VALUE_STRATEGY[child]
                            # in-place update
                            update_path_in_dict(child, default, result)


# TODO this should be managed in a proper way
# Now we need this b/c when transforming a `canonical_to` or
# `redirects_to`, if it's an external url string, we will not
# do a ES mget for it. So it IdToUrlTransformer will not affect
# it.

class ExternalUrlTransformer(ResultTransformer):
    """External urls should also be marked as not crawled"""

    _TRANSFORM_STRATEGY = {
        'redirects_to',
        'canonical_to'
    }

    def __init__(self, es_result, query=None, **kwargs):
        self.results = es_result
        if query:
            self.fields = query.fields
        else:
            self.fields = kwargs['fields']

    def transform(self):
        for result in self.results:
            for required_field in result:
                if required_field in self._TRANSFORM_STRATEGY:
                    field = result[required_field]
                    if 'url' in field:
                        field['crawled'] = False


# Available transformers
# Order is IMPORTANT
RESULT_TRANSFORMERS = [
    ExternalUrlTransformer,
    IdToUrlTransformer,
    DefaultValueTransformer,
]