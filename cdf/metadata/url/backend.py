from es_backend_utils import ElasticSearchBackend
from cdf.core.metadata.dataformat import assemble_data_format

ELASTICSEARCH_BACKEND = ElasticSearchBackend(assemble_data_format())
