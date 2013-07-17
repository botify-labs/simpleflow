URLS_DATA_MAPPING = {
    "urls": {
        "properties": {
            "tagging": {
                "type": "nested",
                "properties": {
                    "resource_type": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "rev_id": {"type": "long"}
                }
            },
            "url": {"type": "string"},
            "url_hash": {"type": "long"},
            "byte_size": {"type": "long"},
            "date_crawled": {"type": "date"},
            "delay1": {"type": "long"},
            "delay2": {"type": "long"},
            "depth": {"type": "long"},
            "gzipped": {"type": "boolean"},
            "host": {"type": "string"},
            "http_code": {"type": "long"},
            "id": {"type": "long"},
            "metadata_nb": {
                "properties": {
                    "description": {"type": "long"},
                    "h1": {"type": "long"},
                    "h2": {"type": "long"},
                    "title": {"type": "long"}
                }
            },
            "metadata": {
                "properties": {
                    "description": {"type": "string"},
                    "h1": {"type": "string"},
                    "h2": {"type": "string"},
                    "title": {"type": "string"}
                }
            },
            "outlinks_external_follow_nb": {"type": "long"},
            "outlinks_follow_ids": {"type": "long"},
            "outlinks_internal_follow_nb": {"type": "long"},
            "path": {"type": "string"},
            "protocol": {"type": "string"},
            "query_string": {"type": "string"},
            "query_string_items": {"type": "string"},
            "query_string_keys": {"type": "string"},
            "query_string_keys_order": {"type": "string"},
            "redirect_to": {
                "properties": {
                    "http_code": {"type": "string"},
                    "url_id": {"type": "long"}
                }
            }
        }
    }
}
