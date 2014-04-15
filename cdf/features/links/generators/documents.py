from cdf.analysis.urls.utils import is_link_internal
from cdf.log import logger
from cdf.metadata.raw.masks import list_to_mask


__all__ = ["PROCESSORS", "FINAL_PROCESSORS", "PREPARING_PROCESSORS", "GENERATOR_FILES"]


def _get_nofollow_combination_key(keys):
    return '_'.join(sorted(keys))


def _process_init(document):
    # resolve a (dest, mask) to its index in `inlinks_internal` list
    document["processed_outlink_link"] = set()
    # a temp set to track all `seen` dest url of outgoing links
    document["processed_outlink_url"] = set()
    # temporary structures for analytic processing
    document["processed_inlink_link"] = set()
    # a temp set to track all `seen` src url of incoming links
    document["processed_inlink_url"] = set()


def _process_outlinks(document, stream_oulinks):
    url_src, link_type, follow_keys, url_dst, external_url = stream_oulinks

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


def _process_inlinks(document, stream_inlinks):
    url_dst, link_type, follow_keys, url_src = stream_inlinks

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


def _process_badlinks(document, stream_badlinks):
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


def _process_final(document):
    # If not "outlinks_internal" : we want to store a non-crawled url
    if not 'outlinks_internal' in document:
        return

    document['outlinks_internal']['nb']['unique'] = len(document['processed_outlink_url'])
    document['inlinks_internal']['nb']['unique'] = len(document['processed_inlink_url'])

    # delete intermediate data structures
    del document['processed_inlink_url']
    del document['processed_outlink_url']
    del document["processed_outlink_link"]
    del document["processed_inlink_link"]


PROCESSORS = {
    'inlinks': _process_inlinks,
    'outlinks': _process_outlinks,
    'badlinks': _process_badlinks
}

FINAL_PROCESSORS = [_process_final]

GENERATOR_FILES = [
    'urllinks',
    'urlinlinks',
    'urlbadlinks'
]

PREPARING_PROCESSORS = [_process_init]
