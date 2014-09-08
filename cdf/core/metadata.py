from cdf.query.constants import FIELD_RIGHTS


def make_fields_private(mapping):
    """Make all the field of the mapping private.
    The initial intent of this function was to temporarly hide fields
    that we do not want to display to the user because they are still under
    development.
    If you definitely want to make a field private, it is recommanded to
    explicit add FIELD_RIGHTS.PRIVATE to its settings.
    :param mapping: input mapping as a dict field_name -> parameter dict
    :type mapping: dict
    :returns: dict - the modified mapping
    """
    for field in mapping.itervalues():
        if "settings" not in field:
            field["settings"] = set()

        #remove existing field rights
        settings = field["settings"]
        existing_field_rights = [
            elt for elt in settings if isinstance(elt, FIELD_RIGHTS)
        ]
        for field_right in existing_field_rights:
            settings.remove(field_right)

        #add private right
        settings.add(FIELD_RIGHTS.PRIVATE)
    return mapping

