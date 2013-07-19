# -*- coding:utf-8 -*-
import unittest
import logging
from datetime import datetime


from cdf.log import logger
from cdf.collections.url_properties.generator import UrlPropertiesGenerator

logger.setLevel(logging.DEBUG)


class TestUrlDataGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_simple(self):
        patterns = (
            [1, 'http', 'www.site.com', '/path/name.html', ''],
        )

        resource_type_settings = [
            {
                'host': '*.site.com',
                'rules': [
                    {'query': "STARTS(path, '/path')",
                     'value': "mypath"
                     }
                ]
            }
        ]

        u = UrlPropertiesGenerator(iter(patterns), resource_type_settings)
        results = list(u)
        self.assertEquals(results[0], (1, {"resource_type": "mypath", "host": "www.site.com"}))

    def test_unkown(self):
        patterns = (
            [1, 'http', 'www.site.com', '/path/name.html', ''],
        )

        resource_type_settings = [
            {
                'host': '*.site.com',
                'rules': [
                    {'query': "STARTS(path, '/another_path')",
                     'value': "mypath"
                     }
                ]
            }
        ]

        u = UrlPropertiesGenerator(iter(patterns), resource_type_settings)
        results = list(u)
        self.assertEquals(results[0], (1, {"resource_type": "unkown", "host": "www.site.com"}))

    def test_abstract(self):
        """
        The url matches with the first rule but as it has been abstracted, it should be unkown
        """
        patterns = (
            [1, 'http', 'www.site.com', '/path/name.html', ''],
        )

        resource_type_settings = [
            {
                'host': '*.site.com',
                'rules': [
                    {'query': "STARTS(path, '/path')",
                     'abstract': True,
                     'rule_id': 'path_rule'
                     },
                    {'query': "ENDS(path, '.json')",
                     'value': 'json',
                     'inherits_from': 'path_rule'
                     }
                ]
            }
        ]

        u = UrlPropertiesGenerator(iter(patterns), resource_type_settings)
        results = list(u)
        self.assertEquals(results[0], (1, {"resource_type": "unkown", "host": "www.site.com"}))

        # Now we had another rule which should match
        resource_type_settings[0]['rules'].append({
            'query': "ENDS(path, '.html')",
            'value': 'html',
            'inherits_from': 'path_rule'
        })

        u = UrlPropertiesGenerator(iter(patterns), resource_type_settings)
        results = list(u)
        self.assertEquals(results[0], (1, {"resource_type": "html", "host": "www.site.com"}))

    def test_inherits(self):
        """
        The url matches with the second rule but as it inherits from the first one, it should not match.
        """
        patterns = (
            [1, 'http', 'www.site.com', '/music/name.html', ''],
        )

        resource_type_settings = [
            {
                'host': '*.site.com',
                'rules': [
                        {'query': "STARTS(path, '/movie')",
                         'abstract': True,
                         'rule_id': 'movie_rule'
                         },
                        {'query': "ENDS(path, '.json')",
                         'value': 'movie/json',
                         'inherits_from': 'movie_rule'
                         }
                ]
            }
        ]
        u = UrlPropertiesGenerator(iter(patterns), resource_type_settings)
        results = list(u)
        self.assertEquals(results[0], (1, {"resource_type": "unkown", "host": "www.site.com"}))
