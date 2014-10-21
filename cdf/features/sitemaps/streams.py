from cdf.core.features import StreamDefBase
from cdf.metadata.url.url_metadata import (
    BOOLEAN_TYPE, AGG_CATEGORICAL,
    DIFF_QUALITATIVE
)


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
        "sitemaps.present": {
            "verbose_name": "Present in sitemap",
            "type": BOOLEAN_TYPE,
            "settings": {
                AGG_CATEGORICAL,
                DIFF_QUALITATIVE
            }
        }
    }

    URL_DOCUMENT_DEFAULT_GROUP = "sitemaps"

    def pre_process_document(self, document):
        document["sitemaps"] = {"present": False}

    def process_document(self, document, stream):
        #the method is called only for urls that are referenced in the stream
        document["sitemaps"]["present"] = True
