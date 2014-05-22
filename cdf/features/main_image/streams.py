from cdf.metadata.url.url_metadata import (
    STRING_TYPE, ES_NO_INDEX
)
from cdf.core.streams.base import StreamDefBase
from cdf.query.constants import RENDERING
from .settings import IMAGE_FIELDS


__all__ = ["ContentsExtendedStreamDef"]


class ContentsExtendedStreamDef(StreamDefBase):
    FILE = 'urlcontents_x'
    HEADERS = (
        ('id', int),
        ('type', str),  # Type of the extracted field (currently only m.prop)
        ('position', int),  # Position of the extracted field
        ('field', str),  # field name
        ('value', str),  # field value
    )
    URL_DOCUMENT_MAPPING = {
        "main_image": {
            "type": STRING_TYPE,
            "settings": {
                ES_NO_INDEX,
                RENDERING.IMAGE_URL
            }
        }
    }

    def process_document(self, document, stream):
        url_id, ftype, position, field, value = stream
        if ftype == "m.prop" and field in IMAGE_FIELDS and not document["main_image"]:
            document["main_image"] = value
