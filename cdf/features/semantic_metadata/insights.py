from cdf.core.insights import Insight, PositiveTrend
from cdf.query.filter import EqFilter, GtFilter, AndFilter
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
    name = "Unique {}".format(metadata.title())
    duplicate_field = "metadata.{}.duplicates.nb".format(metadata)
    nb_field = "metadata.{}.nb".format(metadata)
    additional_fields = ["metadata.{}.contents".format(metadata)]
    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.UP,
            AndFilter([
                EqFilter(duplicate_field, 0),
                GtFilter(nb_field, 0)
            ]),
            additional_fields=additional_fields
        )
    )

    #not set
    identifier = "meta_{}_not_set".format(metadata)
    name = "Not Set {}".format(metadata.title())
    field = "metadata.{}.nb".format(metadata)
    additional_fields = [
        "metadata.{}.contents".format(metadata),
        "metadata.{}.duplicates.nb".format(metadata),
        "metadata.{}.duplicates.urls".format(metadata)
    ]
    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.DOWN,
            EqFilter(field, 0),
            additional_fields=additional_fields
        )
    )

    #duplicate
    identifier = "meta_{}_duplicate".format(metadata)
    name = "Duplicate {}".format(metadata.title())
    field = "metadata.{}.duplicates.nb".format(metadata)
    additional_filter = EqFilter("metadata.{}.is_first".format(metadata), True)
    result.append(
        Insight(
            identifier,
            name,
            PositiveTrend.DOWN,
            GtFilter(field, 0),
            additional_filter=additional_filter,
            sort_by=DescendingSort(field)
        )
    )

    return result

#actual insight definition
insights = []
for metadata in ["title", "description", "h1"]:
    insights.extend(get_metadata_insights(metadata))
