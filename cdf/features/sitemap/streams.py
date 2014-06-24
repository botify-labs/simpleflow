from cdf.core.features import StreamDefBase


class SitemapStreamDef(StreamDefBase):
    """A stream that list the urlids for the urls
    that are in the crawl and in the sitemap
    """
    FILE = 'sitemap'
    HEADERS = (
        ('id', int),
    )

