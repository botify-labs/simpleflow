from cdf.features.main.streams import IdStreamDef
from cdf.features.main.utils import get_url_to_id_dict_from_stream
from cdf.features.ganalytics.streams import RowVisitsStreamDef, VisitsStreamDef
from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


@with_temporary_dir
def transform_visits_to_local_ids(raw_data_file, crawl_storage_uri, first_part_id_size, part_id_size,
                                  protocol='http', tmp_dir=None, force_fetch=False):
    """
    :param raw_data_file : the google analytics raw data file
    :pram crawl_storage_uri : the root storage uri where to find the given crawl dataset

    Transform a row file like :
    //www.site.com/my_url organic google 12
    //www.site.com/my_another_url organic google 50

    To :
    576 organic google 12
    165 organic google 50
    """
    url_to_id = get_url_to_id_dict_from_stream(IdStreamDef.get_stream_from_storage(crawl_storage_uri, tmp_dir=tmp_dir))
    dataset = VisitsStreamDef.create_temporary_dataset()

    for url, medium, source, nb_visits in RowVisitsStreamDef.get_stream_from_path(raw_data_file):
        if not url.startswith('http'):
            url = '{}://{}'.format(protocol, url)

        url_id = url_to_id.get(url, None)
        if url_id:
            dataset.append(url_id, medium, source, nb_visits)

    dataset.persist_to_storage(crawl_storage_uri, first_part_id_size=first_part_id_size, part_id_size=part_id_size)
