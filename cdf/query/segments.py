from StringIO import StringIO
from cdf.compat import json


def get_segments_from_query(query, segments_idx):
    """
    :param query : A BSQL query
    :parem segments_idx : A dict of segments (key : hash, value : {"human": ..., "query": ..., "total_urls": 10})
    Return a list of most accurate segments from a BQL query
    """
    query.botify_query["aggs"] = [{
        "group_by": ["pattern_id"],
        "metric": "count",
    }]
    results = []
    # TODO
    return results


def load_segments_idx_from_s3(s3_uri):
    f = StringIO()
    key = get_key_from_s3_uri(s3_uri)
    f.write(key.get_contents_to_string(key))
    f.seek(0)
    segments = load_segments_idx_from_file(f)
    f.close()
    return segments


def load_segments_idx_from_file(f):
    """
    Load segments from file and return it as a dict

    TSV file format is :
    * human readble query
    * Botify Query
    * Query hash
    * Number of url matching this query

    Key is segment hash
    Value is a dict with the following format :
    {
        "human": "host='www.site.com' and path='products/'",
        "query": {
            "and": [
                {"field": "host", "value": "www.site.com"},
                {"field": "path", "value": "products/", "predicate": "startswith"}
            ]
        },
        "total_urls": 10
    }
    """
    segments_idx = {}
    for line in f:
        human, query, _hash, total_urls = line[:-1].split('\t')
        segments_idx[int(_hash)] = {
            "human": human,
            "query": json.loads(query),
            "total_urls": int(total_urls)
        }
    return segments_idx

