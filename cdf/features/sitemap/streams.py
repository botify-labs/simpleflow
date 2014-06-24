from cdf.core.features import StreamDefBase
from cdf.metadata.url.url_metadata import BOOLEAN_TYPE


class SitemapStreamDef(StreamDefBase):
    """A stream that list the urlids for the urls
    that are in the crawl and in the sitemap
    """
    FILE = 'sitemap'
    HEADERS = (
        ('id', int),
    )

    URL_DOCUMENT_MAPPING = {
        # url property data
        "sitemap.present": {
            "verbose_name": "Present in sitemap",
            "type": BOOLEAN_TYPE,
            "order": 54,  # FIXME
        }
    }
