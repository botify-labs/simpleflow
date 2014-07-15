"""
This module defines the workflow that analyzes the data of a crawl. It uses the
dataflow module from the backend to execute tasks asynchronously and define
dependencies between them.

There is currently no task discovery for workers. It means we still have to
declare and import the task handlers in
``botify.saas.backend.process.worker.handler.analysis``.

See https://github.com/sem-io/botify-saas-backend/issues/407 to follow the
evolution of this situation.

The main abstraction of the dataflow module is the ``Future`` object. It holds
the state of a computation that can be:

- PENDING
- RUNNING
- FINISHED

When the computation is finished, the ``Future`` either stores a result or an
exception. The ``Future.exception`` attribute allows to handle errors.

The execution model relies of the replay of the *whole* workflow on each event.
In other words, the code that defines the workflow (here in
``AnalysisWorkflow.run``) is executed from the start when any event (a
task finished or failed for example) occurred.

This requires the workflow's code to be idempotent i.e. it always returns the
same result from the same input, independtly of the number of times it is
executed.

"""

from simpleflow import (
    Workflow,
    futures,
    activity,
)

from . import constants


def as_activity(func):
    """
    This decorator provides default values for the activities's attributes.

    We should rather decorate each activity individually to set the right
    values, especially for timeouts.

    """
    return activity.with_attributes(
        version='2.7',
        task_list='analysis',
        schedule_to_start_timeout=1800,
        start_to_close_timeout=7200,
        schedule_to_close_timeout=9000,
        heartbeat_timeout=300,
        raises_on_failure=True,
    )(func)


from cdf.tasks.suggest.clusters import compute_mixed_clusters
compute_mixed_clusters = as_activity(compute_mixed_clusters)

from cdf.tasks.intermediary_files import (
    make_metadata_duplicates_file,
    make_links_counter_file,
    make_bad_link_file,
    make_bad_link_counter_file,
)
make_metadata_duplicates_file = as_activity(make_metadata_duplicates_file)
make_links_counter_file = as_activity(make_links_counter_file)
make_bad_link_file = as_activity(make_bad_link_file)
make_bad_link_counter_file = as_activity(make_bad_link_counter_file)

from cdf.tasks.url_data import (
    generate_documents,
    push_documents_to_elastic_search
)
generate_documents = as_activity(generate_documents)
push_documents_to_elastic_search = as_activity(
    push_documents_to_elastic_search)

from cdf.tasks.aggregators import (
    compute_aggregators_from_part_id,
    make_suggest_summary_file,
    consolidate_aggregators,
)
compute_aggregators_from_part_id = as_activity(
    compute_aggregators_from_part_id)
make_suggest_summary_file = as_activity(make_suggest_summary_file)
consolidate_aggregators = as_activity(consolidate_aggregators)

from cdf.features.ganalytics.tasks import (
    import_data_from_ganalytics,
    match_analytics_to_crawl_urls
)
import_data_from_ganalytics = as_activity(import_data_from_ganalytics)
match_analytics_to_crawl_urls = as_activity(match_analytics_to_crawl_urls)

from cdf.utils.remote_files import (
    nb_parts_from_crawl_location as enumerate_partitions
)
enumerate_partitions.name = 'enumerate_partitions'
enumerate_partitions = as_activity(enumerate_partitions)


UPDATE_STATUS_TIMEOUTS = {
    'schedule_to_start_timeout': 30,
    'start_to_close_timeout': 30,
    'schedule_to_close_timeout': 60,
    'heartbeat_timeout': 100,
}


@activity.with_attributes(
    name='crawl.update_status',
    version='1.1',
    task_list='crawl.status',
    **UPDATE_STATUS_TIMEOUTS)
def update_crawl_status(crawl_id, instance_id, crawl_endpoint, crawl_status):
    """

    The worker supports this task, so we only need to map the task's name and
    input.

    """
    pass


@activity.with_attributes(
    name='crawler.update_status',
    version='1.1',
    task_list='crawler.status',
    **UPDATE_STATUS_TIMEOUTS)
def update_crawler_status(crawl_id, instance_id, crawler_endpoint, crawler_status):
    """

    The worker supports this task, so we only need to map the task's name and
    input.

    """
    pass


@activity.with_attributes(
    name='revision.update_status',
    version='1.1',
    task_list='revision.status',
    **UPDATE_STATUS_TIMEOUTS)
def update_revision_status(revision_endpoint, revision_status):
    """

    The worker supports this task, so we only need to map the task's name and
    input.

    """
    pass


@activity.with_attributes(
    name='crawl.request_api',
    version='1.1',
    task_list='crawl.status',
    **UPDATE_STATUS_TIMEOUTS)
def request_api(crawl_endpoint, revision_endpoint, api_requests):
    """Make a request to an API.

    We should make all requests asynchronously in the workflow instead of
    scheduling a single task that makes all the requests.

    :param api: description of the request.
    :type  api: dict.

    Example value for *api*:

    {
        "method": "patch",
        "endpoint_url": "revision",
        "endpoint_suffix": "ganalytics/",
        "data": {
            "sample_rate": metadata["sample_rate"],
            "sample_size": metadata["sample_size"],
            "sampled": metadata["sampled"],
            "queries_count": metadata["queries_count"]
        }
    }

    """
    pass


class AnalysisWorkflow(Workflow):
    name = 'analysis'
    version = '2.7'
    task_list = 'analysis'

    def compute_ganalytics(self, context):
        config = context['features_options']['ganalytics']
        s3_uri = context['s3_uri']
        features_flags = context['features_flags']
        ganalytics_result = self.submit(
            import_data_from_ganalytics,
            config['access_token'],
            config['refresh_token'],
            config['ganalytics_site_id'],
            s3_uri,
            features_flags=features_flags)

        if ganalytics_result.finished:
            api_requests = self.submit(
                match_analytics_to_crawl_urls,
                s3_uri,
                context['first_part_id_size'],
                context['part_id_size'],
                features_flags)

            ganalytics_result = self.submit(
                request_api,
                context['crawl_endpoint'],
                context['revision_endpoint'],
                api_requests)

        return [ganalytics_result]

    def run(self, **context):
        # Extract variables from the context.
        crawl_id = context['crawl_id']
        s3_uri = context.get('s3_uri') or context['crawl_location']

        first_part_id_size = context.get(
            'first_part_id_size',
            constants.FIRST_PART_ID_SIZE)
        part_id_size = context.get(
            'part_id_size',
            constants.PART_ID_SIZE)

        revision_number = context['revision_number']
        es_location = context['es_location']
        es_index = context['es_index']
        es_doc_type = context['es_doc_type']

        features_flags = context.get('features_flags', [])

        clusters_result = self.submit(
            compute_mixed_clusters,
            crawl_id,
            s3_uri,
            first_part_id_size,
            part_id_size)

        metadata_dup_result = self.submit(
            make_metadata_duplicates_file,
            crawl_id,
            s3_uri,
            first_part_id_size,
            part_id_size)

        bad_link_result = self.submit(
            make_bad_link_file,
            crawl_id,
            s3_uri,
            first_part_id_size,
            part_id_size)

        partitions = self.submit(enumerate_partitions, s3_uri)

        # ``make_bad_link_counter_file`` depends on ``make_bad_link_file`` but
        # does not take its result (that is None) as an argument. Further below
        # we need to wait for ``bad_link_counter_results``. That's why we
        # define an dummy pending ``Future`` ``bad_link_counter_results``
        # because ``bad_link_result.finished`` will probably be False on the
        # first replay.  We can then wait for the termination of
        # ``bad_link_counter_results``.  When ``bad_link_result.finished`` will
        # be True, ``bad_link_counter_results`` will be replaced by the actual
        # list of futures returned by the call to ``self.startmap()``.
        bad_link_counter_results = [futures.Future()]
        if bad_link_result.finished:
            bad_link_counter_results = self.starmap(
                make_bad_link_counter_file,
                [(crawl_id, s3_uri, part_id) for part_id in
                 xrange(partitions.result)])

        inlinks_results = self.starmap(
            make_links_counter_file,
            [(crawl_id, s3_uri, part_id, 'in') for part_id in
             xrange(partitions.result)])

        outlinks_results = self.starmap(
            make_links_counter_file,
            [(crawl_id, s3_uri, part_id, 'out') for part_id in
             xrange(partitions.result)])

        # Group all the futures that need to terminate before computing the
        # aggregations and generating documents.
        intermediary_files = ([
            clusters_result,
            metadata_dup_result,
            bad_link_result] +
            bad_link_counter_results +
            inlinks_results +
            outlinks_results)

        if 'ganalytics' in features_flags:
            intermediary_files.extend(self.compute_ganalytics(context))

        futures.wait(*intermediary_files)

        aggregators_results = self.starmap(
            compute_aggregators_from_part_id,
            [(crawl_id, s3_uri, part_id) for part_id in
             xrange(partitions.result)])

        futures.wait(*aggregators_results)
        consolidate_result = self.submit(
            consolidate_aggregators,
            crawl_id,
            s3_uri)

        documents_results = self.starmap(
            generate_documents,
            [(crawl_id, s3_uri, part_id) for part_id in
             xrange(partitions.result)])

        elastic_search_results = [futures.Future()]
        if all(result.finished for result in documents_results):
            elastic_search_results = self.starmap(
                push_documents_to_elastic_search,
                [(crawl_id, s3_uri, es_location, es_index, es_doc_type,
                  part_id) for part_id in
                 xrange(partitions.result)])

        futures.wait(*(elastic_search_results + [consolidate_result]))

        suggest_summary_result = self.submit(
            make_suggest_summary_file,
            crawl_id,
            s3_uri,
            es_location,
            es_index,
            es_doc_type,
            revision_number)
        futures.wait(suggest_summary_result)

        crawl_status_result = self.submit(
            update_crawl_status,
            crawl_id,
            context['instance_id'],
            context['crawl_endpoint'],
            'FINISHED')
        revision_status_result = self.submit(
            update_revision_status,
            context['revision_endpoint'],
            'FINISHED')

        result = {}
        result.update(crawl_status_result.result)
        result.update(revision_status_result.result)
        return result
