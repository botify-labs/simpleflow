from cdf.features.rel.constants import LANGUAGES_ISO, REGIONS_ISO


def extract_lang_and_region(value):
    """
    Return a tuple of (lang, region) from a string
    (expected value format is "lang-region" or "lang" without region)
    """
    if value == "x-default":
        return [None, None]
    codes = value.rsplit('-', 2)
    # If lang is standalone, return region as None
    if len(codes) == 1:
        return [codes[0], None]
    return codes[0:2]


def is_lang_valid(value):
    """
    Receives an iso code for lang or lang+region
    Value is expected are lowerized
    """
    return value[0:2] in LANGUAGES_ISO


def is_region_valid(region_code):
    """
    Returns True is value is in format 'lang_iso-region-iso'
    Value is expected are lowerized
    """
    return region_code in REGIONS_ISO
