URLS_DATA_MAPPING = {
    "urls": {
        "properties": {
            "url": {
                "type": "string",
                "index": "not_analyzed"
            },
            "url_hash": {"type": "long"},
            "byte_size": {"type": "long"},
            "date_crawled": {"type": "date"},
            "delay1": {"type": "long"},
            "delay2": {"type": "long"},
            "depth": {"type": "long"},
            "gzipped": {"type": "boolean"},
            "host": {
                "type": "string",
                "index": "not_analyzed"
            },
            "http_code": {"type": "long"},
            "id": {"type": "long"},
            "crawl_id": {"type": "long"},
            "patterns": {"type": "long"},
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
                    "description": {
                        "type": "multi_field",
                        "fields": {
                            "description": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "h1": {
                        "type": "multi_field",
                        "fields": {
                            "h1": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "h2": {
                        "type": "multi_field",
                        "fields": {
                            "h2": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "title": {
                        "type": "multi_field",
                        "fields": {
                            "title": {
                                "type": "string"
                            },
                            "untouched": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    }
                }
            },
            "metadata_duplicate_nb": {
                "properties": {
                    "title": {"type": "long"},
                    "description": {"type": "long"},
                    "h1": {"type": "long"}
                }
            },
            "metadata_duplicate": {
                "properties": {
                    "title": {"type": "long"},
                    "description": {"type": "long"},
                    "h1": {"type": "long"}
                }
            },
            "metadata_duplicate_is_first": {
                "properties": {
                    "title": {"type": "boolean"},
                    "description": {"type": "boolean"},
                    "h1": {"type": "boolean"}
                }
            },
            "inlinks_internal_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow_unique": {"type": "long"},
                    "follow": {"type": "long"},
                    "nofollow": {"type": "long"},
                    "nofollow_combinations": {
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "long"}
                        }
                    }
                }
            },
            "inlinks_internal": {"type": "long"},
            "outlinks_internal": {"type": "long"},
            "outlinks_internal_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow_unique": {"type": "long"},
                    "follow": {"type": "long"},
                    "nofollow": {"type": "long"},
                    "nofollow_combinations": {
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "long"}
                        }
                    }
                }
            },
            "outlinks_external_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow": {"type": "long"},
                    "nofollow": {"type": "long"},
                    "nofollow_combinations": {
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "long"}
                        }
                    }
                }
            },
            "path": {
                "type": "string",
                "index": "not_analyzed"
            },
            "protocol": {"type": "string"},
            "query_string": {"type": "string"},
            # can replace this by an custom analyzer
            "query_string_items": {"type": "string"},
            "query_string_keys": {"type": "string"},
            "query_string_keys_order": {"type": "string"},
            "canonical_from_nb": {"type": "long"},
            "canonical_from": {"type": "long"},
            "canonical_to": {
                "properties": {
                    "url": {"type": "string"},
                    "url_id": {"type": "long"}
                }
            },
            "canonical_to_equal": {"type": "boolean"},
            "redirects_to": {
                "properties": {
                    "http_code": {"type": "string"},
                    "url": {"type": "string"},
                    "url_id": {"type": "long"}
                }
            },
            "redirects_from_nb": {"type": "long"},
            "redirects_from": {
                "properties": {
                    "http_code": {"type": "string"},
                    "url_id": {"type": "long"}
                }
            }
        }
    }
}
