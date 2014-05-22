from enum import Enum


NAME = "Analytics import"
DESCRIPTION = ""
PRIORITY = 1000

ORGANIC_SOURCES = (
    'google',
    'bing',
    'yahoo',
    "ask",
    "aol",
    "yandex",
    "baidu",
    "naver"
)

SOCIAL_SOURCES = (
    'facebook',
    'twitter',
    'pinterest',
    'linkedin',
    'reddit',
    'google+',
    'tumblr'
)


GROUPS = Enum(
    'Groups',
    [('visits.organic.{}'.format(source), 'Visits from {}'.format(source)) for source in ORGANIC_SOURCES] +
    [('visits.social.{}'.format(source), 'Visits from {}'.format(source)) for source in SOCIAL_SOURCES]
)
