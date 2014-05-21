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


GROUPS = [{'id': 'visits.organic.{}'.format(source), 'name': 'Visits from {}'.format(source)} for source in ORGANIC_SOURCES] + \
         [{'id': 'visits.social.{}'.format(source), 'name': 'Visits from {}'.format(source)} for source in SOCIAL_SOURCES]
