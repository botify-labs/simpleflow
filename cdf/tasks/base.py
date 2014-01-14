import os


def make_tmp_dir_from_crawl_id(crawl_id):
    _dir = os.path.join('/tmp', 'crawl_{}'.format(crawl_id))
    try:
        os.makedirs(_dir)
    except:
        pass
    return _dir
