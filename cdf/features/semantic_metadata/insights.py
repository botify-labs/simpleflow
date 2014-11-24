from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import EqFilter, GtFilter, AndFilter, OrFilter
from cdf.query.sort import DescendingSort


def get_metadata_insights(metadata):
    """Return the list of insights for a given metadata: title, description or h1
    :param metadata: the input metadata
    :type metadata: str
    :returns: list - the list of corresponding insights_module_name
    """
    result = []

    #unique
    identifier = "meta_{}_unique".format(metadata)
    name = "Unique {} on Compliant URLs".format(metadata.title())
    duplicate_field = "metadata.{}.duplicates.context_aware.nb".format(metadata)
    nb_field = "metadata.{}.nb".format(metadata)
    strategic_field = "strategic.is_strategic"
    additional_fields = ["metadata.{}.contents".format(metadata)]
    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.UP,
            AndFilter([
                EqFilter(duplicate_field, 0),
                GtFilter(nb_field, 0),
                EqFilter(strategic_field, True)
            ]),
            additional_fields=additional_fields
        )
    )

    #not set
    identifier = "meta_{}_not_set".format(metadata)
    name = "Not Set {} on Compliant URLs".format(metadata.title())
    field = "metadata.{}.nb".format(metadata)
    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.DOWN,
            AndFilter([
                EqFilter(field, 0),
                EqFilter(strategic_field, True)
            ])
        )
    )

    #duplicate
    identifier = "meta_{}_duplicate".format(metadata)
    name = "Duplicate {} on Compliant URLs".format(metadata.title())
    field = "metadata.{}.duplicates.context_aware.nb".format(metadata)
    additional_fields = [
        "metadata.{}.contents".format(metadata),
        "metadata.{}.duplicates.context_aware.nb".format(metadata),
        "metadata.{}.duplicates.context_aware.urls".format(metadata)
    ]

    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.DOWN,
            AndFilter([
                GtFilter(field, 0),
                EqFilter(strategic_field, True)
            ]),
            additional_fields=additional_fields,
            sort_by=DescendingSort(field)
        )
    )

    #bad
    identifier = "meta_{}_bad".format(metadata)
    name = "Compliant URLs with Bad {}".format(metadata.title())
    nb_field = "metadata.{}.nb".format(metadata)
    duplicates_field = "metadata.{}.duplicates.context_aware.nb".format(metadata)
    additional_fields = [
        "metadata.{}.contents".format(metadata),
        "metadata.{}.duplicates.context_aware.nb".format(metadata),
        "metadata.{}.duplicates.context_aware.urls".format(metadata),
        "metadata.{}.nb".format(metadata)
    ]
    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.DOWN,
            OrFilter([
                AndFilter([EqFilter(nb_field, 0), EqFilter(strategic_field, True)]),
                AndFilter([GtFilter(duplicates_field, 0), EqFilter(strategic_field, True)])
            ]),
            additional_fields=additional_fields,
            sort_by=DescendingSort(duplicates_field)
        )
    )

    return result


def get_semantic_metadata_insights():
    result = []
    for metadata in ["title", "description", "h1"]:
        result.extend(get_metadata_insights(metadata))
    return result

#actual insight definition
insights = get_semantic_metadata_insights()
