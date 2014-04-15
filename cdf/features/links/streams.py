from cdf.core.streams.base import StreamBase
from cdf.analysis.urls.utils import is_link_internal
from cdf.log import logger
from cdf.metadata.raw.masks import list_to_mask
from .helpers.masks import follow_mask


def _str_to_bool(string):
    return string == '1'


def _get_nofollow_combination_key(keys):
    return '_'.join(sorted(keys))


class OutlinksRawStream(StreamBase):
    FILE = 'urllinks'
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('dst_url_id', int),
        ('external_url', str)
    )


class OutlinksStream(OutlinksRawStream):
    """
    We just change the type of "follow"
    """
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('follow', follow_mask),
        ('dst_url_id', int),
        ('external_url', str)
    )

    def pre_process_document(self, document):
        # resolve a (dest, mask) to its index in `inlinks_internal` list
        document["processed_outlink_link"] = set()
        # a temp set to track all `seen` dest url of outgoing links
        document["processed_outlink_url"] = set()

    def process_document(self, document, stream):
        url_src, link_type, follow_keys, url_dst, external_url = stream

        if link_type == "a":
            # is_internal = url_dst > 0
            is_internal = is_link_internal(follow_keys, url_dst)
            is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
            outlink_type = "outlinks_internal" if is_internal else "outlinks_external"
            mask = list_to_mask(follow_keys)

            outlink_nb = document[outlink_type]['nb']
            outlink_nb['total'] += 1

            # target dict changes with link follow status
            follow = outlink_nb['follow' if is_follow else 'nofollow']
            follow['total'] += 1
            if is_internal and is_follow:
                # increment follow counters
                if not (url_dst, mask) in document['processed_outlink_link']:
                    follow['unique'] += 1
            elif not is_follow:
                # increment nofollow combination counters
                key = _get_nofollow_combination_key(follow_keys)
                follow['combinations'][key] += 1

            # internal outlinks
            # still need dest url id check since we can have internal url
            # blocked by robots.txt
            if is_internal and url_dst > 0:
                outlink_urls = document['outlinks_internal']['urls']
                exists = (url_dst, mask) in document['processed_outlink_link']
                if len(outlink_urls) < 300 and not exists:
                    outlink_urls.append([url_dst, mask])

                # add this link's dest to the processed set
                document['processed_outlink_url'].add(url_dst)
                document['processed_outlink_link'].add((url_dst, mask))

                document['outlinks_internal']['urls_exists'] = True

        elif link_type.startswith('r'):
            http_code = link_type[1:]
            redirects_to = document['redirect']['to']
            redirects_to['url'] = {}
            if url_dst == -1:
                redirects_to['url']['url_str'] = external_url
            else:
                redirects_to['url']['url_id'] = url_dst
            redirects_to['url']['http_code'] = int(http_code)
            redirects_to['url_exists'] = True

        elif link_type == "canonical":
            canonical_to = document['canonical']['to']
            if canonical_to.get('equal', None) is None:
                # We take only the first canonical found
                canonical_to['equal'] = url_src == url_dst
                canonical_to['url'] = {}
                if url_dst > 0:
                    canonical_to['url']['url_id'] = url_dst
                else:
                    canonical_to['url']['url_str'] = external_url
                canonical_to['url_exists'] = True

    def post_process_document(self, document):
        # If not "outlinks_internal" : we want to store a non-crawled url
        if not 'outlinks_internal' in document:
            return

        document['outlinks_internal']['nb']['unique'] = len(document['processed_outlink_url'])

        # delete intermediate data structures
        del document['processed_outlink_url']
        del document["processed_outlink_link"]


class InlinksStream(StreamBase):
    FILE = 'urllinks'
    HEADERS = (
        ('id', int),
        ('link_type', str),
        ('bitmask', int),
        ('src_url_id', int),
    )

    def pre_process_document(self, document):
        # temporary structures for analytic processing
        document["processed_inlink_link"] = set()
        # a temp set to track all `seen` src url of incoming links
        document["processed_inlink_url"] = set()

    def process_document(self, document, stream):
        url_dst, link_type, follow_keys, url_src = stream

        if link_type == "a":
            is_follow = len(follow_keys) == 1 and follow_keys[0] == "follow"
            mask = list_to_mask(follow_keys)

            inlink_nb = document['inlinks_internal']['nb']
            inlink_nb['total'] += 1

            follow = inlink_nb['follow' if is_follow else 'nofollow']
            follow['total'] += 1

            if is_follow:
                if not (url_src, mask) in document["processed_inlink_link"]:
                    follow['unique'] += 1
            else:
                key = _get_nofollow_combination_key(follow_keys)
                if 'robots' in key:
                    logger.warn('Skip `robots` mask in inlink mask')
                else:
                    follow['combinations'][key] += 1

            inlink_urls = document['inlinks_internal']['urls']
            exists = (url_src, mask) in document['processed_inlink_link']
            if len(inlink_urls) < 300 and not exists:
                inlink_urls.append([url_src, mask])

            # add src to processed set
            document['processed_inlink_url'].add(url_src)
            document['processed_inlink_link'].add((url_src, mask))

            document['inlinks_internal']['urls_exists'] = True

        elif link_type.startswith('r'):
            # TODO dangerous assumption of crawl's string format to be 'r3xx'
            http_code = int(link_type[1:])
            redirects_from = document['redirect']['from']
            redirects_from['nb'] += 1
            if len(redirects_from['urls']) < 300:
                redirects_from['urls'].append([url_src, http_code])
            redirects_from['urls_exists'] = True

        elif link_type == "canonical":
            canonical_from = document['canonical']['from']

            # only count for none self canonical
            if url_dst != url_src:
                canonical_from['nb'] += 1
                if len(canonical_from['urls']) < 300:
                    canonical_from['urls'].append(url_src)
                canonical_from['urls_exists'] = True

    def post_process_document(self, document):
        # If not "inlinks_internal" : we want to store a non-crawled url
        if not 'inlinks_internal' in document:
            return

        document['inlinks_internal']['nb']['unique'] = len(document['processed_inlink_url'])

        # delete intermediate data structures
        del document['processed_inlink_url']
        del document["processed_inlink_link"]


class OutlinksCountersStream(StreamBase):
    FILE = 'url_out_links_counters'
    HEADERS = (
        ('id', int),
        ('follow', follow_mask),
        ('is_internal', _str_to_bool),
        ('score', int),
        ('score_unique', int),
    )


class OutredirectCountersStream(StreamBase):
    FILE = 'url_out_redirect_counters'
    HEADERS = (
        ('id', int),
        ('is_internal', _str_to_bool)
    )


class OutcanonicalCountersStream(StreamBase):
    FILE = 'url_out_canonical_counters'
    HEADERS = (
        ('id', int),
        ('equals', _str_to_bool)
    )


class InlinksCountersStream(StreamBase):
    FILE = 'url_in_links_counters'
    HEADERS = (
        ('id', int),
        ('follow', follow_mask),
        ('score', int),
        ('score_unique', int),
    )


class InredirectCountersStream(StreamBase):
    FILE = 'url_in_redirect_counters'
    HEADERS = (
        ('id', int),
        ('score', int)
    )


class IncanonicalCountersStream(StreamBase):
    FILE = 'url_in_canonical_counters'
    HEADERS = (
        ('id', int),
        ('score', int)
    )


class BadLinksStream(StreamBase):
    FILE = 'urlbadlinks'
    HEADERS = (
        ('id', int),
        ('dst_url_id', int),
        ('http_code', int)
    )

    def process_document(self, document, stream_badlinks):
        _, url_dest_id, http_code = stream_badlinks

        errors = document['outlinks_errors']

        error_kind = None
        if 300 <= http_code < 400:
            error_kind = '3xx'
        elif 400 <= http_code < 500:
            error_kind = '4xx'
        elif http_code >= 500:
            error_kind = '5xx'

        errors[error_kind]['nb'] += 1
        error_urls = errors[error_kind]['urls']
        if len(error_urls) < 10:
            error_urls.append(url_dest_id)

        # increment the consolidate value
        errors['total'] += 1

        errors[error_kind]['urls_exists'] = True


class BadLinksCountersStream(StreamBase):
    FILE = 'urlbadlinks_counters'
    HEADERS = (
        ('id', int),
        ('http_code', int),
        ('score', int)
    )
