ELASTICSEARCH_LOCATION = 'http://localhost:9200'
ELASTICSEARCH_INDEX = 'cdf_test'
CRAWL_ID = 1
DOC_TYPE = 'crawls'

URLS_FIXTURE = [
    {
        'id': 1,
        '_id': '%d:%d' % (CRAWL_ID, 1),
        'crawl_id': CRAWL_ID,
        'url': 'http://www.mysite.com/france/football/abcde/main.html',
        'path': '/france/football/abcde/main.html',
        'depth': 1,
        'http_code': 200,
        'metadata': {
            'title': {
                'nb': 1,
                'contents': ['My title'],
            },
            'h1': {
                'nb': 1,
                'contents': ['Welcome to our website']
            },
            'description': {'nb': 0},
            'h2': {
                'nb': 3,
                'contents': ['abcd', 'abc', 'botify']
            },
        },
        'outlinks_internal': {
            'urls': [
                [2, 0], # follow
                [3, 7], # link, meta, robots
                [5, 3], # link, meta / link to not crawled
            ],
            'urls_exists': True,
            'nb': {
                'total': 102,
                'unique': 2,
                'follow': {
                    'total': 101,
                    'unique': 1
                },
                'nofollow': {
                    'total': 0,
                }
            }
        },
        'canonical': {
            'to': {'url': {'url_id': 3}, 'url_exists': True},
            'from': {'urls': [2, 3, 4], 'nb': 3, 'urls_exists': True}
        },
    },
    {
        'id': 2,
        '_id': '%d:%d' % (CRAWL_ID, 2),
        'crawl_id': CRAWL_ID,
        'url': 'http://www.mysite.com/football/france/abc/abcde',
        'path': '/football/france/abc/abcde',
        'depth': 2,
        'http_code': 301,
        'metadata': {
            'h2': {
                'nb': 3,
                'contents': ['cba', 'foobar', 'botifyy']
            },
        },
        'inlinks_internal': {
            'urls': [
                [1, 0], # follow
                [3, 8], # follow
                [4, 5], # link, robots
            ],
            'urls_exists': True,
            'nb': {
                'total': 105,
                'unique': 3,
                'follow': {
                    'total': 104,
                    'unique': 2,
                },
                'nofollow': {
                    'total': 0
                }
            }
        },
        'canonical': {'to': {'url': {'url_id': 5}, 'url_exists': True}}
    },
    {
        'id': 3,
        '_id': '%d:%d' % (CRAWL_ID, 3),
        'crawl_id': CRAWL_ID,
        'url': 'http://www.mysite.com/football/article-s.html',
        'path': '/football/article-s.html',
        'http_code': 200,
        'depth': 2,
        'redirect': {
            'from': {
                'nb': 2,
                'urls': [
                    [1, 301],
                    [2, 301],
                ],
                'urls_exists': True
            }
        },
        'outlinks_errors': {
            '4xx': {
                'nb': 1,
                'urls': [4],
                'urls_exists': True,
            },
            'total': 1,
        },
    },
    {
        'id': 4,
        '_id': '%d:%d' % (CRAWL_ID, 4),
        'crawl_id': CRAWL_ID,
        'url': 'http://www.mysite.com/errors',
        'http_code': 200,
        'depth': 2,
        'metadata': {
            'title': {
                'duplicates': {
                    'nb': 1,
                    'urls': [1],
                    'urls_exists': True
                }
            },
            'h1': {
                'duplicates': {
                    'nb': 2,
                    'urls': [2, 3],
                    'urls_exists': True
                }
            },
            'description': {
                'duplicates': {
                    'nb': 1,
                    'urls': [4],
                    'urls_exists': True
                }
            }
        },
        'outlinks_errors': {
            '3xx': {
                'nb': 3,
                'urls': [1, 2, 3],
                'urls_exists': True
            },
            '5xx': {
                'nb': 2,
                'urls': [2, 3],
                'urls_exists': True
            },
            'total': 5
        },
        'redirect': {
            'from': {
                'nb': 2,
                'urls': [
                    [1, 301],
                    [2, 301],
                ],
                'urls_exists': True
            }
        },
    },
    {
        'id': 5,
        '_id': '%d:%d' % (CRAWL_ID, 5),
        'crawl_id': CRAWL_ID,
        'url': 'http://www.notcrawled.com',
        'http_code': 0
    },
    {
        'id': 6,
        '_id': '%d:%d' % (CRAWL_ID, 6),
        'crawl_id': CRAWL_ID,
        'http_code': 200
    },
    {
        'id': 7,
        '_id': '%d:%d' % (CRAWL_ID, 7),
        'crawl_id': CRAWL_ID,
        'depth': 2,
        'http_code': 301,
        'redirect': {
            'to': {'url': {'url_id': 5, 'http_code': 301},
                   'url_exists': True}
        },
    },
    {
        'id': 8,
        '_id': '%d:%d' % (CRAWL_ID, 8),
        'crawl_id': CRAWL_ID,
        'url': 'http://www.error.com',
        'http_code': -160
    }
]
