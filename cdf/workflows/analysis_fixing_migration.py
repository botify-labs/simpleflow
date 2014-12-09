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

import logging

from simpleflow import (
    Workflow,
    futures,
    activity,
)

from . import constants

logger = logging.getLogger(__name__)


def as_activity(func):
    """
    This decorator provides default values for the activities's attributes.

    We should rather decorate each activity individually to set the right
    values, especially for timeouts.

    """
    return activity.with_attributes(
        version='2.7',
        task_list='analysis',
        schedule_to_start_timeout=54000,  # 15h
        start_to_close_timeout=21600,     # 6h
        schedule_to_close_timeout=75600,  # 21h
        heartbeat_timeout=300,
        retry=1,
        raises_on_failure=True,
    )(func)


from cdf.features.links.tasks import (
    make_bad_link_file,
    make_bad_link_counter_file,
    make_links_to_non_compliant_file,
    make_links_to_non_compliant_counter_file,
    make_top_domains_files,
    make_inlinks_percentiles_file
)

make_bad_link_file = as_activity(make_bad_link_file)
make_bad_link_counter_file = as_activity(make_bad_link_counter_file)
make_links_to_non_compliant_file = as_activity(make_links_to_non_compliant_file)
make_links_to_non_compliant_counter_file = as_activity(make_links_to_non_compliant_counter_file)
make_top_domains_files = as_activity(make_top_domains_files)
make_inlinks_percentiles_file = as_activity(make_inlinks_percentiles_file)

from cdf.tasks.url_data import (
    generate_documents,
    push_documents_to_elastic_search
)
generate_documents = as_activity(generate_documents)

from cdf.utils.remote_files import enumerate_partitions
enumerate_partitions = as_activity(enumerate_partitions)

from cdf.features.comparison.tasks import match_documents
match_documents = as_activity(match_documents)

from cdf.tasks.insights import (
    refresh_index,
    get_feature_options
)
from cdf.tasks.insights import compute_insights as compute_insights_task
refresh_index = as_activity(refresh_index)
get_feature_options = as_activity(get_feature_options)
compute_insights_task = as_activity(compute_insights_task)

push_documents_to_elastic_search = activity.with_attributes(
    version='2.7',
    task_list='analysis',
    schedule_to_start_timeout=54000,  # 15h
    start_to_close_timeout=36000,     # 10h
    schedule_to_close_timeout=90000,  # 25h
    heartbeat_timeout=300,
    retry=0,  # retry is handled per bulk
    raises_on_failure=True,
)(push_documents_to_elastic_search)

UPDATE_STATUS_TIMEOUTS = {
    'schedule_to_start_timeout': 14400,  # 4h
    'start_to_close_timeout': 60,
    'schedule_to_close_timeout': 14460,  # 4h01
    'heartbeat_timeout': 180,
}


@activity.with_attributes(
    name='crawl.update_status',
    version='1.1',
    task_list='crawl.status',
    retry=1,
    **UPDATE_STATUS_TIMEOUTS)
def update_crawl_status(crawl_id, instance_id, crawl_endpoint, crawl_status):
    """

    The worker supports this task, so we only need to map the task's name and
    input.

    """
    return {}


@activity.with_attributes(
    name='crawler.update_status',
    version='1.1',
    task_list='crawler.status',
    retry=1,
    **UPDATE_STATUS_TIMEOUTS)
def update_crawler_status(crawl_id, instance_id, crawler_endpoint, crawler_status):
    """

    The worker supports this task, so we only need to map the task's name and
    input.

    """
    return {}


@activity.with_attributes(
    name='revision.update_status',
    version='1.1',
    task_list='revision.status',
    retry=1,
    **UPDATE_STATUS_TIMEOUTS)
def update_revision_status(revision_endpoint, revision_status):
    """

    The worker supports this task, so we only need to map the task's name and
    input.

    """
    return {}


@activity.with_attributes(
    name='crawl.request_api',
    version='1.1',
    task_list='crawl.status',
    retry=1,
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
    return {}


class AnalysisFixingMigrationWorkflow(Workflow):
    name = 'analysis_fixing_migration'
    version = '0.1'
    task_list = 'analysis'

    def compute_insights(self, context):
        """Compute insight values
        :param context: the analysis context
        :type context: dict
        :returns: future
        """
        crawl_id = context['crawl_id']
        s3_uri = context['crawl_location']
        config_endpoint = context['config_endpoint']

        elastic_search_ready = self.submit(
            refresh_index,
            context["es_location"],
            context["es_index"],
        )

        futures.wait(elastic_search_ready)

        #consider current analysis configuration.
        crawl_configurations = [[crawl_id,  config_endpoint, s3_uri]]
        if 'comparison' in context['features_options']:
            crawl_configurations.extend(context['features_options']['comparison']['history'])

        insights_result = self.submit(
            compute_insights_task,
            crawl_configurations,
            context["es_location"],
            context["es_index"],
            context["es_doc_type"],
            s3_uri
        )
        return insights_result

    def run(self, **context):
        # Extract variables from the context.
        crawl_id = context['crawl_id']

        # The variable is called ``s3_uri`` because all tasks use this name
        # However the workflow calls it ``crawl_location`` because we may
        # use another storage backend to store data files.
        s3_uri = context['crawl_location']

        tmp_dir = context.get('tmp_dir', None)
        first_part_id_size = context.get(
            'first_part_id_size',
            constants.FIRST_PART_ID_SIZE)
        part_id_size = context.get(
            'part_id_size',
            constants.PART_ID_SIZE)

        # ES params
        es_params = {
            'es_location': context['es_location'],
            'es_index': context['es_index'],
            'es_doc_type': context['es_doc_type']
        }

        partitions = self.submit(enumerate_partitions,
                                 s3_uri)

        crawled_partitions = self.submit(enumerate_partitions,
                                         s3_uri,
                                         only_crawled_urls=True)

        revision_number = context['revision_number']
        features_flags = context.get('features_flags', [])
        has_comparison = 'comparison' in features_flags

        bad_link_result = self.submit(
            make_bad_link_file,
            crawl_id,
            s3_uri,
            first_part_id_size,
            part_id_size,
            tmp_dir=tmp_dir)


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
            bad_link_counter_results = [
                self.submit(
                    make_bad_link_counter_file,
                    crawl_id=crawl_id,
                    s3_uri=s3_uri,
                    tmp_dir=tmp_dir,
                    part_id=part_id,
                )
                for part_id in crawled_partitions.result
            ]

        links_to_non_compliant_urls = self.submit(
            make_links_to_non_compliant_file,
            s3_uri,
            first_part_id_size=first_part_id_size,
            part_id_size=part_id_size,
            tmp_dir=tmp_dir
        )

        links_to_non_compliant_urls_counter_results = [futures.Future()]
        if links_to_non_compliant_urls.finished:
            links_to_non_compliant_urls_counter_results = [
                self.submit(
                    make_links_to_non_compliant_counter_file,
                    s3_uri=s3_uri,
                    tmp_dir=tmp_dir,
                    part_id=part_id,
                )
                for part_id in crawled_partitions.result
            ]
        # Group all the futures that need to terminate before computing the
        # aggregations and generating documents.
        intermediary_files = (
            bad_link_counter_results +
            links_to_non_compliant_urls_counter_results
        )

        futures.wait(*intermediary_files)

        # inlink percentiles depends on link counters
        percentile_results = self.submit(
            make_inlinks_percentiles_file,
            s3_uri=s3_uri,
            first_part_id_size=first_part_id_size,
            part_id_size=part_id_size,
            tmp_dir=tmp_dir
        )
        futures.wait(percentile_results)

        documents_results = [
            self.submit(
                generate_documents,
                crawl_id=crawl_id,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                part_id=part_id,
            )
            for part_id in partitions.result
        ]

        # document merging for comparison
        # need to wait for documents generation
        futures.wait(*documents_results)
        if has_comparison:
            previous_analysis = context['features_options']['comparison']['history'][0]
            _, _, ref_s3_uri = previous_analysis
            comparison = self.submit(
                match_documents,
                new_s3_uri=s3_uri,
                ref_s3_uri=ref_s3_uri,
                new_crawl_id=crawl_id
            )

        # wait document matching
        # if `comparison` feature is activated
        if has_comparison:
            futures.wait(comparison)

        elastic_search_result = self.submit(
            push_documents_to_elastic_search,
            crawl_id,
            s3_uri,
            comparison=has_comparison,
            tmp_dir=tmp_dir,
            **es_params
        )
        futures.wait(elastic_search_result)

        insights_result = self.compute_insights(context)

        futures.wait(
            insights_result
        )

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

        futures.wait(crawl_status_result, revision_status_result)
        update_status_errors = []
        if crawl_status_result.exception:
            update_status_errors.append('crawl status: {}'.format(
                crawl_status_result.exception))
        if revision_status_result.exception:
            update_status_errors.append('revision status: {}'.format(
                revision_status_result.exception))
        if update_status_errors:
            self.fail('Cannot update {}'.format(
                ' and '.join(update_status_errors)))

        result = {}
        result.update(crawl_status_result.result)
        result.update(revision_status_result.result)
        return result

    def on_failure(self, history, reason, details=None):
        input = getattr(history.events[0], 'input', {})
        context = input.get('kwargs')
        if not context:
            logger.warning('No context for failure: {}'.format(reason))
            return

        try:
            logger.error(
                'Workflow for crawl #{} failed: {}'.format(
                    context['crawl_id'],
                    reason))
        except:
            pass
