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


def optional_activity(func, task_name, feature):
    act = activity.with_attributes(
        version='2.7',
        task_list='analysis',
        schedule_to_start_timeout=54000,  # 15h
        start_to_close_timeout=21600,     # 6h
        schedule_to_close_timeout=75600,  # 21h
        heartbeat_timeout=300,
        retry=1,
        # do not raise on optional tasks
        raises_on_failure=False,
    )(func)

    # patch additional task tracking info
    act.tracking = True
    act.task_name = task_name
    act.feature = feature

    return act


from cdf.features.main.tasks import compute_suggested_patterns
compute_suggested_patterns = optional_activity(
    compute_suggested_patterns, 'segment', 'main')
from cdf.tasks.aggregators import (
    compute_aggregators_from_part_id,
    make_suggest_summary_file,
    consolidate_aggregators,
)
compute_aggregators_from_part_id = optional_activity(
    compute_aggregators_from_part_id, 'segment', 'main')
make_suggest_summary_file = optional_activity(make_suggest_summary_file, 'segment', 'main')
consolidate_aggregators = optional_activity(consolidate_aggregators, 'segment', 'main')

from cdf.features.main.tasks import compute_zones, compute_compliant_urls
compute_zones = as_activity(compute_zones)
compute_compliant_urls = as_activity(compute_compliant_urls)

from cdf.features.semantic_metadata.tasks import (
    compute_metadata_count,
    make_metadata_duplicates_file,
    make_context_aware_metadata_duplicates_file,
)

from cdf.features.links.tasks import (
    make_links_counter_file,
    make_bad_link_file,
    make_bad_link_counter_file,
    make_links_to_non_compliant_file,
    make_links_to_non_compliant_counter_file,
    make_top_domains_files,
    make_inlinks_percentiles_file
)
compute_metadata_count = as_activity(compute_metadata_count)
make_metadata_duplicates_file = as_activity(make_metadata_duplicates_file)
make_context_aware_metadata_duplicates_file = as_activity(
    make_context_aware_metadata_duplicates_file
)
make_links_counter_file = as_activity(make_links_counter_file)
make_bad_link_file = as_activity(make_bad_link_file)
make_bad_link_counter_file = as_activity(make_bad_link_counter_file)
make_links_to_non_compliant_file = as_activity(make_links_to_non_compliant_file)
make_links_to_non_compliant_counter_file = as_activity(make_links_to_non_compliant_counter_file)
make_top_domains_files = optional_activity(make_top_domains_files, 'top_domain', 'links')
make_inlinks_percentiles_file = as_activity(make_inlinks_percentiles_file)

from cdf.tasks.url_data import (
    generate_documents,
    push_documents_to_elastic_search
)
generate_documents = as_activity(generate_documents)

from cdf.features.ganalytics.tasks import (
    import_data_from_ganalytics,
    match_analytics_to_crawl_urls
)
import_data_from_ganalytics = optional_activity(
    import_data_from_ganalytics, 'document', 'ganalytics')
match_analytics_to_crawl_urls = optional_activity(
    match_analytics_to_crawl_urls, 'document', 'ganalytics')

from cdf.features.sitemaps.tasks import (
    download_sitemap_files,
    match_sitemap_urls,
)
download_sitemap_files = optional_activity(
    download_sitemap_files, 'document', 'sitemaps')
match_sitemap_urls = optional_activity(
    match_sitemap_urls, 'document', 'sitemaps')

from cdf.features.rel.tasks import (
    convert_rel_out_to_rel_compliant_out
)
convert_rel_out_to_rel_compliant_out = as_activity(convert_rel_out_to_rel_compliant_out)

from cdf.utils.remote_files import enumerate_partitions
enumerate_partitions = as_activity(enumerate_partitions)

from cdf.features.comparison.tasks import match_documents
match_documents = as_activity(match_documents)

from cdf.tasks.analysis import refresh_index
refresh_index = as_activity(refresh_index)

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

    :param crawl_endpoint: URL of the crawl endpoint in the Botify Analytics
                           API.
    :type  crawl_endpoint: str.
    :param revision_endpoint: URL of the revision endpoint in the Botify
                              Analytics API.
    :type  revision_endpoint: str.
    :param api_requests: list of requests to send to the Botify Analytics API.
    :type  api_requests: [{
        'method': str,
        'endpoint_url': str,
        'endpoint_suffix': str,
        'data': {
            ...,
            }
        }]


    We should make all requests asynchronously in the workflow instead of
    scheduling a single task that makes all the requests.

    :param api: description of the request.
    :type  api: dict.

    Example value for *api_requests*:

    [{
        "method": "patch",
        "endpoint_url": "revision",
        "endpoint_suffix": "ganalytics/",
        "data": {
            "sample_rate": metadata["sample_rate"],
            "sample_size": metadata["sample_size"],
            "sampled": metadata["sampled"],
            "queries_count": metadata["queries_count"]
        }
    }]

    Please notice the enclosing brackets ``[...]``, it's a list of several
    requests. Even a single request must be inside a list.

    """
    return {}


class FeatureTaskRegistry(object):
    """Task registry for a single feature, used by `TaskRegistry`
    """
    def __init__(self):
        self.registry = {}

    def register(self, future, task_name):
        self.registry.setdefault(task_name, []).append(future)

    def get_task_status(self):
        return {
            task: all(f.exception is None for f in futures) for
            task, futures in self.registry.iteritems()
        }


class TaskRegistry(object):
    """Task registry that associates tasks and their execution status

    It aggregates tracked tasks' status for each feature.
    All logical related tasks should be given the same name so that they are
    tracked as the same unity.
    """
    def __init__(self):
        self.registry = {}

    def register(self, future, task_name, feature):
        """Register a task
        """
        self.registry.setdefault(
            feature, FeatureTaskRegistry()).register(future, task_name)

    def get_task_status(self):
        """Conclude the tasks' status
        """
        status = []
        for feature, registry in self.registry.iteritems():
            s = registry.get_task_status()
            for task, ss in s.iteritems():
                status.append({
                    'task': task,
                    'feature': feature,
                    'success': ss
                })
        return status


class AnalysisWorkflow(Workflow):
    name = 'analysis'
    version = '2.7'
    task_list = 'analysis'

    def __init__(self, executor):
        super(AnalysisWorkflow, self).__init__(executor)
        self.task_registry = TaskRegistry()

    def submit(self, func, *args, **kwargs):
        """Override `submit` to allow register tracked tasks
        """
        # submit the task to executor
        future = super(AnalysisWorkflow, self).submit(func, *args, **kwargs)

        if hasattr(func, 'tracking'):
            # this is a decorated task, need to be tracked
            self.task_registry.register(future, func.task_name, func.feature)

        return future

    def compute_ganalytics(self, context):
        """
        Import and compute data from Google Analytics.

        :param context: passed to the workflow.
        :type  context: dict | collections.Mapping.

        *context* must contain:

        - features_options
        - crawl_location
        - first_part_id_size
        - part_id_size
        - crawl_endpoint
        - revision_endpoint
        - access_token
        - refresh_token
        - ganalytics_site_id

        :returns:
            :rtype: [Future]


        """
        config = context['features_options']['ganalytics']
        s3_uri = context['crawl_location']

        ganalytics_result = self.submit(
            import_data_from_ganalytics,
            config['access_token'],
            config['refresh_token'],
            config['ganalytics_site_id'],
            s3_uri,
            config['date_start'],
            config['date_end'],
        )

        # Explicit dependency because we cannot use an argument to express the
        # dependency between ``import_data_from_ganalytics`` and
        # ``match_analytics_to_crawl_urls``. Empty future, by default with
        # state ``PENDING``, to return until the previous task is finished.
        if ganalytics_result.finished and ganalytics_result.exception is None:
            ganalytics_result = self.submit(
                match_analytics_to_crawl_urls,
                s3_uri,
                context['first_part_id_size'],
                context['part_id_size'])

            # Check if the future is finished to avoid blocking on
            # ganalytics_result.result below.
            if ganalytics_result.finished and ganalytics_result.exception is None:
                # We don't return the future returned by the call below because
                # we don't want to break the workflow if there is an error when
                # calling the API. As the result of this task will be stored in
                # the workfow's history, we can extract it and fix the values
                # manually (which should occur only exceptionally).
                self.submit(
                    request_api,
                    context['crawl_endpoint'],
                    context['revision_endpoint'],
                    ganalytics_result.result['api_requests'])

        return [ganalytics_result]

    def compute_sitemaps(self, context):
        config = context['features_options']['sitemaps']
        s3_uri = context['crawl_location']
        sitemaps_result = self.submit(
            download_sitemap_files,
            config['urls'],
            s3_uri,
            context["settings"]["http"]["user_agent"])
        if sitemaps_result.finished and sitemaps_result.exception is None:
            sitemaps_result = self.submit(
                match_sitemap_urls,
                s3_uri,
                context["settings"]["hostnames"]["valid"],
                context["settings"]["hostnames"]["blacklist"],
                context['first_part_id_size'],
                context['part_id_size'])

        return [sitemaps_result]

    @classmethod
    def has_segments(cls, context):
        """Checks if segment (suggested pattern) feature is enabled"""
        return context['features_options']['main'].get(
            'suggested_patterns', True)

    def compute_zone_compliant_dependent(self, crawled_partitions, **context):
        s3_uri = context["s3_uri"]
        first_part_id_size = context["first_part_id_size"]
        part_id_size = context["part_id_size"]
        tmp_dir = context["tmp_dir"]

        context_aware_metadata_dup_result = self.submit(
            make_context_aware_metadata_duplicates_file,
            s3_uri=s3_uri,
            first_part_id_size=first_part_id_size,
            part_id_size=part_id_size,
            tmp_dir=tmp_dir
        )

        rel_out_to_rel_compliant_out_result = self.submit(
            convert_rel_out_to_rel_compliant_out,
            s3_uri=s3_uri,
            first_part_id_size=first_part_id_size,
            part_id_size=part_id_size,
            crawled_partitions=crawled_partitions.result,
            tmp_dir=tmp_dir
        )

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

        return (
            [
                context_aware_metadata_dup_result,
                rel_out_to_rel_compliant_out_result
            ] +
            links_to_non_compliant_urls_counter_results
        )

    def run(self, **context):
        # Extract variables from the context.
        crawl_id = context['crawl_id']

        # The variable is called ``s3_uri`` because all tasks use this name
        # However the workflow calls it ``crawl_location`` because we may
        # use another storage backend to store data files.
        s3_uri = context['crawl_location']
        context['s3_uri'] = context['crawl_location']

        tmp_dir = context.setdefault("tmp_dir", None)
        first_part_id_size = context.setdefault(
            'first_part_id_size',
            constants.FIRST_PART_ID_SIZE)
        part_id_size = context.setdefault(
            'part_id_size',
            constants.PART_ID_SIZE)

        # ES params
        es_params = {
            'es_location': context['es_location'],
            'es_index': context['es_index'],
            'es_doc_type': context['es_doc_type']
        }

        partitions = self.submit(enumerate_partitions,
                                 s3_uri,
                                 first_part_id_size,
                                 part_id_size)

        crawled_partitions = self.submit(enumerate_partitions,
                                         s3_uri,
                                         first_part_id_size,
                                         part_id_size,
                                         only_crawled_urls=True)

        revision_number = context['revision_number']
        features_flags = context.get('features_flags', [])
        has_comparison = 'comparison' in features_flags

        if 'push_to_elastic_search_only' in context:
            # Quickfix for big failure of ES
            # We assume that documents are already generated and available
            # on S3
            elastic_search_result = self.submit(
                push_documents_to_elastic_search,
                crawl_id,
                s3_uri,
                comparison=has_comparison,
                tmp_dir=tmp_dir,
                **es_params
            )
            futures.wait(elastic_search_result)
            return

        # intermediary analysis results
        intermediary_files = []

        # suggested_pattern task can be skipped
        if self.has_segments(context):
            clusters_result = self.submit(
                compute_suggested_patterns,
                crawl_id,
                s3_uri,
                first_part_id_size,
                part_id_size,
                tmp_dir=tmp_dir)
            intermediary_files.append(clusters_result)

        metadata_dup_result = self.submit(
            make_metadata_duplicates_file,
            crawl_id,
            s3_uri,
            first_part_id_size,
            part_id_size,
            tmp_dir=tmp_dir)

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

        inlinks_results = [
            self.submit(
                make_links_counter_file,
                crawl_id=crawl_id,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                link_direction='in',
                part_id=part_id,
            )
            for part_id in partitions.result
        ]

        outlinks_results = [
            self.submit(
                make_links_counter_file,
                crawl_id=crawl_id,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                link_direction='out',
                part_id=part_id,
            )
            for part_id in crawled_partitions.result
        ]

        filled_metadata_count_results = [
            self.submit(
                compute_metadata_count,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                part_id=part_id
            )
            for part_id in crawled_partitions.result
        ]

        zone_results = [
            self.submit(
                compute_zones,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                part_id=part_id
            )
            for part_id in crawled_partitions.result
        ]

        compliant_urls_results = [
            self.submit(
                compute_compliant_urls,
                crawl_id=crawl_id,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                part_id=part_id
            )
            for part_id in crawled_partitions.result
        ]

        nb_top_domains = 100  # TODO get it from context.
        top_domains_result = self.submit(
            make_top_domains_files,
            crawl_id=crawl_id,
            s3_uri=s3_uri,
            nb_top_domains=nb_top_domains,
            crawled_partitions=crawled_partitions.result
        )

        # Intermediate files
        # Group all the futures that need to terminate before computing the
        # aggregations and generating documents.
        intermediary_files += ([
            metadata_dup_result,
            bad_link_result] +
            bad_link_counter_results +
            inlinks_results +
            outlinks_results +
            zone_results +
            compliant_urls_results +
            filled_metadata_count_results
        )

        if 'ganalytics' in features_flags:
            intermediary_files.extend(self.compute_ganalytics(context))

        if 'sitemaps' in features_flags:
            intermediary_files.extend(self.compute_sitemaps(context))

        if (all(r.finished for r in zone_results) and
            all(r.finished for r in compliant_urls_results)):
            intermediary_files += self.compute_zone_compliant_dependent(crawled_partitions, **context)

        # execute in parallel
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

        # suggested pattern related aggregation tasks
        # TODO should be replaced by dynamic aggregation queries in ES
        if self.has_segments(context):
            aggregators_results = [
                self.submit(
                    compute_aggregators_from_part_id,
                    crawl_id=crawl_id,
                    s3_uri=s3_uri,
                    tmp_dir=tmp_dir,
                    part_id=part_id,
                )
                for part_id in crawled_partitions.result
            ]
            futures.wait(*aggregators_results)

            consolidate_result = self.submit(
                consolidate_aggregators,
                crawl_id,
                s3_uri,
                tmp_dir=tmp_dir)
            futures.wait(consolidate_result)

            suggest_summary_result = self.submit(
                make_suggest_summary_file,
                crawl_id=crawl_id,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                revision_number=revision_number,
                **es_params
            )
            futures.wait(suggest_summary_result)


        # resolve all existing partitions
        all_partitions = set()
        all_partitions.update(crawled_partitions.result)
        all_partitions.update((i.result for i in inlinks_results if i is not None))
        all_partitions.update((i.result for i in outlinks_results if i is not None))
        all_partitions.discard(None)

        documents_results = [
            self.submit(
                generate_documents,
                crawl_id=crawl_id,
                s3_uri=s3_uri,
                tmp_dir=tmp_dir,
                part_id=part_id,
            )
            for part_id in all_partitions
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
            futures.wait(comparison)

        elastic_search_result = self.submit(
            push_documents_to_elastic_search,
            crawl_id,
            s3_uri,
            first_part_id_size=first_part_id_size,
            part_id_size=part_id_size,
            comparison=has_comparison,
            tmp_dir=tmp_dir,
            **es_params
        )
        futures.wait(elastic_search_result)

        # Waiting for ES index to be refreshed
        elastic_search_ready = self.submit(
            refresh_index,
            context["es_location"],
            context["es_index"],
        )
        futures.wait(elastic_search_ready)

        # wait for independent tasks
        # they should be finished before status update
        futures.wait(top_domains_result)

        # conclude all tracked tasks
        task_status = self.task_registry.get_task_status()

        crawl_status_result = self.submit(
            update_crawl_status,
            crawl_id,
            context['instance_id'],
            context['crawl_endpoint'],
            'FINISHED')

        revision_status_result = self.submit(
            request_api,
            context['crawl_endpoint'],
            context['revision_endpoint'],
            [{
                "method": "patch",
                "endpoint_url": "revision",
                "endpoint_suffix": "",
                "data": {
                    "status": "FINISHED",
                    "task_status": task_status,
                }
            }]
        )

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
