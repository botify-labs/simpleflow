from cdf.core.features import Feature


def get_document_fields_from_features_options(features_options):
    fields = []
    for feature in Feature.get_features():
        if not feature.identifier in features_options:
            continue
        fields += feature.get_document_fields_from_options(features_options[feature.identifier])
    return fields
