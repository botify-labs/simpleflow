URLS_DATA_MAPPING = {
    "urls": {
        "properties": {
            "byte_size": {"type": "long"},
            "date_crawled": {"type": "date"},
            "delay1": {"type": "long"},
            "delay2": {"type": "long"},
            "depth": {"type": "long"},
            "gzipped": {"type": "boolean"},
            "host": {"type": "string"},
            "http_code": {"type": "long"},
            "id": {"type": "long"},
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

URLS_PROPERTIES_MAPPING = {
    "urls_properties": {
        "_parent": {
            "type": "urls_data"
        },
        "properties": {
            "resource_type": {
                "type": "string",
                "index": "not_analyzed",
            }
        }
    }
}
