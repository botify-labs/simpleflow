from cdf.core.insights import Insight
from cdf.query.filter import EqFilter, GtFilter


def get_metadata_insights(metadata):
    """Return the list of insights for a given metadata: title, description or h1
    :param metadata: the input metadata
    :type metadata: str
    :returns: list - the list of corresponding insights_module_name
    """
    result = []

    #unique
    identifier = "meta:{}:unique".format(metadata)
    name = "Unique {}".format(metadata.title())
    field = "metadata.{}.duplicates.nb".format(metadata)
    result.append(
        Insight(
            identifier,
            name,
            EqFilter(field, 0)
        )
    )

    #not set
    identifier = "meta:{}:not_set".format(metadata)
    name = "Not Set {}".format(metadata.title())
    field = "metadata.{}.nb".format(metadata)
    result.append(
        Insight(
            identifier,
            name,
            EqFilter(field, 0)
        )
    )

    #duplicate
    identifier = "meta:{}:duplicate".format(metadata)
    name = "Duplicate {}".format(metadata.title())
    field = "metadata.{}.duplicates.nb".format(metadata)
    result.append(
        Insight(
            identifier,
            name,
            GtFilter(field, 0)
        )
    )

    return result

#actual insight definition
insights = []
for metadata in ["title", "description", "h1"]:
    insights.extend(get_metadata_insights(metadata))
