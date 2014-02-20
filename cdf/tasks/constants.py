from cdf.query.es_mapping_generation import generate_es_mapping
from cdf.metadata import URLS_DATA_FORMAT_DEFINITION

# By default, when tasks pull files from s3, local files are ignored
DEFAULT_FORCE_FETCH = True

# ElasticSearch mapping, for creating a new index
ES_MAPPING = generate_es_mapping(URLS_DATA_FORMAT_DEFINITION)