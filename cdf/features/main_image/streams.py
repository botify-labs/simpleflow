from collections import namedtuple

from cdf.metadata.url.url_metadata import (
    STRING_TYPE, ES_NO_INDEX
)
from cdf.core.streams.base import StreamDefBase
from cdf.query.constants import RENDERING
from .settings import IMAGE_FIELDS


__all__ = ["ContentsExtendedStreamDef"]

# Define MainImage object where the temporary image will be stored during document processing
MainImage = namedtuple('MainImage', ['field', 'value', 'position', 'prior'])


def _is_prioritary_field(image_tmp, field, position):
    """Decide whether a new image field has priority over the existing one"""

    #if the new field has a different type, use the field priority to determine
    #the result
    if (
        field != image_tmp.field and
        IMAGE_FIELDS.index(field) < image_tmp.prior
    ):
        return True

    #if the field are identical, the field position determines the priority
    if (
        field == image_tmp.field and
        position < image_tmp.position
    ):
        return True

    return False


class ContentsExtendedStreamDef(StreamDefBase):
    FILE = 'urlcontents_x'
    HEADERS = (
        ('id', int),
        ('type', str),  # Type of the extracted field (currently only m.prop, which are open-graph anchors)
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

    def pre_process_document(self, document):
        document["main_image_tmp"] = None

    def process_document(self, document, stream):
        # Extraction of the main_image
        url_id, ftype, position, field, value = stream
        if ftype != "m.prop":
            return
        if field not in IMAGE_FIELDS:
            return
        if not document["main_image_tmp"] or (document["main_image_tmp"] and _is_prioritary_field(document["main_image_tmp"], field, position)):
            # Store the field name of the main_image and its values into an Enum instance
            # (field_name, image_url, position, index)
            # field_name : ex: og:image, twitter:image...
            # image_url : url of the main image
            # position : position of the meta property (if we find property name with an index less than the current one, it will
            #            become the current one)
            # index : index of the field_name in IMAGE_FIELDS. If we find a line with a field_name with a smaller index,
            #         it will become the new one (ex: og:image is prior to twitter:image)
            document["main_image_tmp"] = MainImage(field, value, position, IMAGE_FIELDS.index(field))

    def post_process_document(self, document):
        # Store the final image url only
        if document["main_image_tmp"]:
            document["main_image"] = document["main_image_tmp"].value
        # Remove temporary key
        del document["main_image_tmp"]
