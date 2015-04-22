import logging
from elasticsearch import Elasticsearch


logger = logging.getLogger(__name__)


# TODO maybe put it in a util module
def refresh_index(es_location, es_index):
    """Issues a `refresh` request to ElasticSearch cluster

    :param es_location: ElasticSearch cluster location
    :type es_location: str
    :param es_index: name of the index to refresh
    :type es_index: str
    :return: refresh result
    :rtype: dict
    """
    #here we set a timeout of 300 instead of the default 10, because it often
    #happens that the cluster won't synchronize all data under load or while
    #recovering/relocating shards.. common observed values are 30s as of this
    #writing so we should be safe with 300, and we really don't care if this
    #task takes a few minutes to complete
    #TODO: instrument it so we can graph response times
    es = Elasticsearch(es_location, timeout=300)
    return es.indices.refresh(index=es_index)
