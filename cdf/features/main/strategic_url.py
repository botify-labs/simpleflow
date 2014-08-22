from cdf.core.streams.utils import group_left
from cdf.features.links.helpers.masks import is_first_canonical
from cdf.features.links.streams import OutlinksStreamDef
from cdf.features.main.streams import InfosStreamDef, StrategicUrlStreamDef

from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


STRATEGIC_HTTP_CODE = 200
STRATEGIC_CONTENT_TYPE = 'text/html'


def is_strategic_url(url_id, infos_mask, http_code,
                     content_type, outlinks):
    """Logic to check if a url is SEO strategic

    :param url_id: url id
    :type url_id: int
    :param infos_mask: `urlinfos`'s mask
    :type infos_mask: int
    :param http_code: http code
    :type http_code: int
    :param content_type: content type of the url
    :type content_type: str
    :param outlinks: all out-going links to the url
    :type outlinks: list
    :return: bool value indicating if the url is strategic
    :rtype bool
    """
    # check `http_code`
    if http_code != STRATEGIC_HTTP_CODE:
        return False

    # check `content_type`
    if content_type != STRATEGIC_CONTENT_TYPE:
        return False

    # check no-index
    noindex = ((4 & infos_mask) == 4)
    if noindex == True:
        return False

    # check `canonical`
    canonical_dest = None
    for _, link_type, _, dest, _ in outlinks:
        # takes the first canonical
        if link_type.startswith('c'):
            canonical_dest = dest
            break

    if canonical_dest is not None:
        if canonical_dest != url_id:
            return False

    return True


def generate_strategic_stream(infos_stream, outlinks_stream):
    """Generate a strategic url stream

    :param infos_stream: stream of dataset `urlinfos`
    :param outlinks_stream: stream of dataset `urloutlinks`
    :return: the strategic stream
    """
    http_idx = InfosStreamDef.field_idx('http_code')
    mask_idx = InfosStreamDef.field_idx('infos_mask')
    ctype_idx = InfosStreamDef.field_idx('content_type')

    grouped_stream = group_left(left=(infos_stream, 0),
                                outlinks_stream=(outlinks_stream, 0))

    for uid, info, outlink in grouped_stream:
        http_code = info[http_idx]
        outlinks = outlink['outlinks_stream']
        infos_mask = info[mask_idx]
        content_type = info[ctype_idx]

        is_strategic = is_strategic_url(
            uid,
            infos_mask,
            http_code,
            content_type,
            outlinks
        )

        yield uid, is_strategic


@with_temporary_dir
def compute_strategic_urls(crawl_id, s3_uri, part_id,
                           tmp_dir=None, force_fetch=False):
    # prepare streams
    infos_stream = InfosStreamDef.get_stream_from_s3(
        s3_uri, tmp_dir, part_id=part_id,
        force_fetch=force_fetch
    )
    outlinks_stream = OutlinksStreamDef.get_stream_from_s3(
        s3_uri, tmp_dir, part_id=part_id,
        force_fetch=force_fetch
    )

    stream = generate_strategic_stream(infos_stream, outlinks_stream)
    StrategicUrlStreamDef.persist_part_to_s3(
        stream=stream,
        s3_uri=s3_uri,
        part_id=part_id
    )