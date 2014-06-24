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

    def pre_process_document(self, document):
        document["sitemap"] = {"present": False}

    def process_document(self, document, stream):
        #the method is called only for urls that are referenced in the stream
        document["sitemap"]["present"] = True
