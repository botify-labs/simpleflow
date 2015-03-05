from cdf.features.rel.constants import LANGUAGES_ISO, COUNTRIES_ISO


def is_lang_valid(value):
    """
    Receives an iso code for lang or lang+country
    """
    return value[0:2] in LANGUAGES_ISO


def get_country(value):
    """
    Return country from value when value is lang+country
    Do not check if country code is valid (use `is_country_valid` for that)
    """
    if len(value) != 5:
        return None
    return value[3:5].upper()


def is_country_valid(country_code):
    """
    Returns True is value is in format 'lang_iso-country-iso'
    """
    return country_code in COUNTRIES_ISO
