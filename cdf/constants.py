from cdf.collections.urls.es_mapping_generation import generate_es_mapping

DEFAULT_FORCE_FETCH = True

URLS_DATA_MAPPING_DEPRECATED = {
    "urls": {
        "_routing": {
            "required": True,
            "path": "crawl_id"
        },
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
                    "title": {"type": "long", "index": "no"},
                    "description": {"type": "long", "index": "no"},
                    "h1": {"type": "long", "index": "no"}
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
                    "total_unique": {"type": "long"},
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
            "inlinks_internal": {"type": "long", "index": "no"},
            "outlinks_internal": {"type": "long", "index": "no"},
            "outlinks_internal_nb": {
                "properties": {
                    "total": {"type": "long"},
                    "follow_unique": {"type": "long"},
                    "total_unique": {"type": "long"},
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
            "protocol": {
                "type": "string",
                "index": "not_analyzed"
            },
            "query_string": {
                "type": "string",
                "index": "not_analyzed"
            },
            "query_string_keys": {
                "type": "string",
                "index": "not_analyzed"
            },
            "canonical_from_nb": {"type": "long"},
            "canonical_from": {"type": "long", "index": "no"},
            "canonical_to": {
                "properties": {
                    "url": {"type": "string", "index": "no"},
                    "url_id": {"type": "long", "index": "no"}
                }
            },
            "canonical_to_equal": {"type": "boolean"},
            "redirects_to": {
                "properties": {
                    "http_code": {"type": "long"},
                    "url": {"type": "string"},
                    "url_id": {"type": "long"}
                }
            },
            "redirects_from_nb": {"type": "long"},
            "redirects_from": {
                "properties": {
                    "http_code": {"type": "long", "index": "no"},
                    "url_id": {"type": "long", "index": "no"}
                }
            },
            "error_links": {
                "properties": {
                    "3xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    },
                    "4xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    },
                    "5xx": {
                        "properties": {
                            "nb": {"type": "long"},
                            "urls": {"type": "long", "index": "no"}
                        }
                    },
                    "any": {
                        "properties": {
                            "nb": {"type": "long"}
                        }
                    }
                }
            }
        }
    }
}


# A intermediate definition of url data format
#
# Keys are represented in a path format
#   - ex. `metadata.h1`
#       This means `metadata` will be an object type and it
#       contains a field named `h1`
#
# Values contains
#   - type: data type of this field
#       - long: for numeric values
#       - string: for string values
#       - struct: struct can contains some inner fields, but these fields
#           won't be visible when querying
#           ex. `something.redirects_from:
#               [{`id`: xx, `http_code`: xx}, {...}, ...]`
#               `redirects_from` is visible, but `redirects_from.id` is not
#           Struct's inner fields have their own types
#       - multi_field: a multi_field type keeps multiple copies of the same
#           data in different format (analyzed, not_analyzed etc.)
#           In case of `multi_field`, `field_type` must be specified for
#           determine the field's type
#
#   - settings (optional): a set of setting flags of this field
#       - not_analyzed: this field should not be tokenized by ES
#       - no_index: this field should not be indexed
#       - list: this field is actually a list of values in ES
#
#   - default_value (optional): the default value if this field does not
#       exist

URLS_DATA_FORMAT_DEFINITION = {
    # url property data
    "url": {
        "type": "string",
        "settings": {
            "not_analyzed"
        }
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
        "settings": {
            "not_analyzed"
        }
    },
    "http_code": {"type": "long"},
    "id": {"type": "long"},
    "crawl_id": {"type": "long"},
    "patterns": {"type": "long"},
    "path": {
        "type": "string",
        "settings": {
            "not_analyzed"
        }
    },
    "protocol": {
        "type": "string",
        "settings": {
            "not_analyzed"
        }
    },
    "query_string": {
        "type": "string",
        "settings": {
            "not_analyzed"
        }
    },
    "query_string_keys": {
        "type": "string",
        "settings": {
            "not_analyzed"
        }
    },

    # metadata numbers
    "metadata_nb.title": {
        "type": "long"
    },
    "metadata_nb.h1": {
        "type": "long"
    },
    "metadata_nb.h2": {
        "type": "long"
    },
    "metadata_nb.description": {
        "type": "long"
    },

    # metadata contents
    "metadata.title": {
        "type": "multi_field",
        "field_type": "string",
        "settings": {
            "list"
        }
    },
    "metadata.h1": {
        "type": "multi_field",
        "field_type": "string",
        "settings": {
            "list"
        }
    },
    "metadata.h2": {
        "type": "multi_field",
        "field_type": "string",
        "settings": {
            "list"
        }
    },
    "metadata.description": {
        "type": "multi_field",
        "field_type": "string",
        "settings": {
            "list"
        }
    },

    # metadata duplication data
    "metadata_duplicate_nb.title": {"type": "long"},
    "metadata_duplicate_nb.description": {"type": "long"},
    "metadata_duplicate_nb.h1": {"type": "long"},

    "metadata_duplicate_is_first.title": {"type": "boolean"},
    "metadata_duplicate_is_first.description": {"type": "boolean"},
    "metadata_duplicate_is_first.h1": {"type": "boolean"},

    "metadata_duplicate.title": {
        "type": "long",
        "settings": {
            "list",
            "no_index"
        }
    },
    "metadata_duplicate.h1": {
        "type": "long",
        "settings": {
            "list",
            "no_index"
        }
    },
    "metadata_duplicate.description": {
        "type": "long",
        "settings": {
            "list",
            "no_index"
        }
    },

    # incoming links data
    "inlinks_internal_nb.total": {"type": "long"},
    "inlinks_internal_nb.follow_unique": {"type": "long"},
    "inlinks_internal_nb.total_unique": {"type": "long"},
    "inlinks_internal_nb.follow": {"type": "long"},
    "inlinks_internal_nb.nofollow": {"type": "long"},
    "inlinks_internal_nb.nofollow_combinations": {
        "type": "struct",
        "values": {
            "key": {"type": "string"},
            "value": {"type": "long"}
        }
    },
    "inlinks_internal": {
        "type": "long",
        "settings": {
            "no_index",
            "list"
        }
    },

    # outgoing links data
    # internal outgoing links
    "outlinks_internal_nb.total": {"type": "long"},
    "outlinks_internal_nb.follow_unique": {"type": "long"},
    "outlinks_internal_nb.total_unique": {"type": "long"},
    "outlinks_internal_nb.follow": {"type": "long"},
    "outlinks_internal_nb.nofollow": {"type": "long"},
    "outlinks_internal_nb.nofollow_combinations": {
        "type": "struct",
        "values": {
            "key": {"type": "string"},
            "value": {"type": "long"}
        }
    },
    "outlinks_internal": {
        "type": "long",
        "settings": {
            "no_index",
            "list"
        }
    },

    # external outgoing links
    "outlinks_external_nb.total": {"type": "long"},
    "outlinks_external_nb.follow": {"type": "long"},
    "outlinks_external_nb.nofollow": {"type": "long"},
    "outlinks_external_nb.nofollow_combinations": {
        "type": "struct",
        "values": {
            "key": {"type": "string"},
            "value": {"type": "long"}
        }
    },


    # incoming canonical links data
    "canonical_from_nb": {"type": "long"},
    "canonical_from": {
        "type": "long",
        "settings": {
            "no_index",
            "list"
        }
    },

    # outgoing canonical link data
    "canonical_to": {
        "type": "struct",
        "default_value": None,
        "values": {
            "url": {"type": "string"},
            "url_id": {"type": "long"},
        },
        "settings": {
            "no_index"
        }
    },
    "canonical_to_equal": {"type": "boolean"},

    # outgoing redirection data
    "redirects_to": {
        "type": "struct",
        "default_value": None,
        "values": {
            "http_code": {"type": "long"},
            "url": {"type": "string"},
            "url_id": {"type": "long"}
        }
    },

    # incoming redirections data
    "redirects_from_nb": {"type": "long"},
    "redirects_from": {
        "type": "struct",
        "values": {
            "http_code": {"type": "long"},
            "url_id": {"type": "long"},
        },
        "settings": {
            "list",
            "no_index"
        }
    },

    # erroneous links data
    "error_links.3xx.nb": {"type": "long"},
    "error_links.4xx.nb": {"type": "long"},
    "error_links.5xx.nb": {"type": "long"},
    "error_links.any.nb": {"type": "long"},

    "error_links.3xx.urls": {
        "type": "long",
        "settings": {
            "no_index",
            "list"
        }
    },
    "error_links.4xx.urls": {
        "type": "long",
        "settings": {
            "no_index",
            "list"
        }
    },
    "error_links.5xx.urls": {
        "type": "long",
        "settings": {
            "no_index",
            "list"
        }
    },
}


# Generated constants
URLS_DATA_FIELDS = URLS_DATA_FORMAT_DEFINITION.keys()
URLS_DATA_MAPPING = generate_es_mapping(URLS_DATA_FORMAT_DEFINITION)
