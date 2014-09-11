from es_backend_utils import ElasticSearchBackend
from cdf.core.metadata import assemble_data_format

ELASTICSEARCH_BACKEND = ElasticSearchBackend(assemble_data_format())
