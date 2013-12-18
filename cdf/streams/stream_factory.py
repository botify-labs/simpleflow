import os
import re
import gzip
import itertools
import json

from urlparse import urlsplit, parse_qs

from cdf.log import logger

from cdf.exceptions import MalformedFileNameError
from cdf.streams.mapping import (STREAMS_HEADERS,
                                 STREAMS_FILES,
                                 CONTENT_TYPE_NAME_TO_ID)
from cdf.streams.caster import Caster
from cdf.streams.utils import split_file, idx_from_stream


def get_part_id_from_filename(filename):
    """Return the part id from a filename
    If the part id can not be extracted raise a MalformedFileNameError
    """
    regex = re.compile(".*txt.([\d]+).gz")
    m = regex.match(filename)
    if not m:
        raise MalformedFileNameError(
            "%s does not contained any part id." % filename
        )
    return int(m.group(1))


class FileStreamFactory(object):
    """Factory that produces a stream out of files of the same type.
    To generate a stream, the class :
    - locate all the corresponding files
    - sort them by part_id
    - generate a stream for each of them
    - chain the streams together
    """

    def __init__(self, dirpath, content, part_id=None):
        """Initiate a factory

        :param content: basename of a content, eg. urlids
        :param part_id: select a partition, if None, stream all
            partitions in order
        """
        self.dirpath = dirpath
        self.part_id = part_id

        # pre-check on file basename
        if content in STREAMS_FILES:
            self.content = content
        else:
            raise Exception("{} is not a known raw file basename".format(content))

    def _get_file_regexp(self):
        """Return a string representing a regex for the filenames
        that correspond to the desired content and part_id"""
        template = '{}.txt.{}.gz'
        if self.part_id:
            return template.format(self.content, self.part_id)
        else:
            return template.format(self.content, '*')

    def _get_json_file_content(self):
        """Return the content of files.json"""
        filename = os.path.join(self.dirpath, "files.json")
        with open(filename) as f:
            json_content = json.load(f)
        return json_content

    def _get_file_list(self, json_content):
        """Return the list of files to stream
        json_content : the content of the file files.json
        Return the list of filepaths to stream"""
        if not self.content in json_content:
            logger.warning("No entry for %s found", self.content)
            return []
        file_list = json_content[self.content]
        file_list = [os.path.basename(f) for f in file_list]
        file_regexp = self._get_file_regexp()
        file_list = [f for f in file_list if re.match(file_regexp, f)]

        file_list = [os.path.join(self.dirpath, f) for f in file_list]
        return file_list

    def _get_stream_from_file(self, input_file):
        """Build the stream corresponding to a file
        input_file : a file object
        Return the stream corresponding to the input file
        with each field correctly casted.
        """
        stream_identifier = STREAMS_FILES[self.content]
        cast = Caster(STREAMS_HEADERS[stream_identifier.upper()]).cast
        return cast(split_file(input_file))

    def get_stream(self):
        """Return the desired generator"""
        regexp = self._get_file_regexp()
        logger.info('Streaming files with regexp {}'.format(regexp))

        json_content = self._get_json_file_content()
        files = self._get_file_list(json_content)

        # sort files by part_id
        # assume file name format to be `basename.txt.part_id.gz`
        ordered_files = sorted(files, key=get_part_id_from_filename)

        streams = []
        for filename in ordered_files:
            f = gzip.open(filename)
            streams.append(self._get_stream_from_file(f))
        return itertools.chain(*streams)

    def get_max_crawled_urlid(self):
        """Return the highest urlid that correspond to an url
        that was actually crawled.
        """
        json_content = self._get_json_file_content()
        result = json_content["max_uid_we_crawled"]
        result = int(result)
        return result


class DataStreamFactory(object):
    """An abstract class for data stream factories
    Data stream factories create streams for given type of data :
    host, query_strings, etc.
    They take raw file streams and filter and process the stream
    to extract relevant data
    """
    def __init__(self, file_stream_factory):
        """Constructor
        file_stream_factory : the factory that generate the raw file streams
        """
        self._file_stream_factory = file_stream_factory

    def set_file_stream_factory(self, stream_factory):
        """A setter for the stream factory.
        This function was implemented for test purpose
        """
        self._file_stream_factory = stream_factory

    def get_stream(self):
        """A method that generate a stream on the desired data"""
        raise NotImplementedError()


class HostStreamFactory(DataStreamFactory):
    def __init__(self, dirpath, part_id=None):
        file_stream_factory = FileStreamFactory(dirpath,
                                                "urlids",
                                                part_id)
        super(self.__class__, self).__init__(file_stream_factory)

    def get_stream(self):
        """Create a generator for the hosts
        The generator creates tuples (urlid, host)
        """
        base_stream = self._file_stream_factory.get_stream()
        max_crawled_urlid = self._file_stream_factory.get_max_crawled_urlid()
        for url in base_stream:
            urlid = url[idx_from_stream("PATTERNS", "id")]
            host = url[idx_from_stream("PATTERNS", "host")]
            host = unicode(host, encoding="utf-8")
            if urlid > max_crawled_urlid:
                raise StopIteration
            else:
                yield urlid, host


class PathStreamFactory(DataStreamFactory):
    def __init__(self, dirpath, part_id=None):
        file_stream_factory = FileStreamFactory(dirpath,
                                                "urlids",
                                                part_id)
        super(self.__class__, self).__init__(file_stream_factory)

    def get_stream(self):
        """Create a generator for the paths
        The generator creates tuples (urlid, path)
        """
        base_stream = self._file_stream_factory.get_stream()
        max_crawled_urlid = self._file_stream_factory.get_max_crawled_urlid()
        for url in base_stream:
            urlid = url[idx_from_stream("PATTERNS", "id")]
            path = url[idx_from_stream("PATTERNS", "path")]
            path = unicode(path, encoding="utf-8")
            parsed_path = urlsplit(path)
            path = parsed_path.path
            if urlid > max_crawled_urlid:
                raise StopIteration
            else:
                yield urlid, path


class QueryStringStreamFactory(DataStreamFactory):
    def __init__(self, dirpath, part_id=None):
        file_stream_factory = FileStreamFactory(dirpath,
                                                "urlids",
                                                part_id)
        super(self.__class__, self).__init__(file_stream_factory)

    def get_stream(self):
        """Create a generator for the query strings
        The generator creates tuples (urlid, query_string_dict)
        where query_string_dict is a dict: param->list of values
        """
        base_stream = self._file_stream_factory.get_stream()
        max_crawled_urlid = self._file_stream_factory.get_max_crawled_urlid()
        for url in base_stream:
            urlid = url[idx_from_stream("PATTERNS", "id")]
            query_string_index = idx_from_stream("PATTERNS", "query_string")
            query_string = {}
            if len(url) >= query_string_index + 1:
                query_string = url[query_string_index]
                query_string = unicode(query_string, encoding="utf-8")
                query_string = query_string[1:]
                query_string = parse_qs(query_string)
                if urlid > max_crawled_urlid:
                    raise StopIteration
                else:
                    yield urlid, query_string


class MetadataStreamFactory(DataStreamFactory):
    def __init__(self, dirpath, content_type, part_id=None):
        """Constructor.
        content_type : a string representing
        the kind of metadata: "title", "h1", etc.
        that we want to figure in the generated streams
        """
        file_stream_factory = FileStreamFactory(dirpath,
                                                "urlcontents",
                                                part_id)
        super(self.__class__, self).__init__(file_stream_factory)
        #init class specific attributes
        self._content_type = content_type
        self._content_type_code = CONTENT_TYPE_NAME_TO_ID[self._content_type]

    @property
    def content_type(self):
        return self._content_type

    def get_stream(self):
        """Create a generator for the metadata
        The generator creates tuples (urlid, list_metadata)
        """
        base_stream = self._file_stream_factory.get_stream()
        max_crawled_urlid = self._file_stream_factory.get_max_crawled_urlid()
        for urlid, lines in itertools.groupby(base_stream,
                                              key=lambda url: url[0]):
            result = []
            for line in lines:
                metadata_code = line[idx_from_stream("CONTENTS", "content_type")]
                if metadata_code != self._content_type_code:
                    continue
                metadata = line[idx_from_stream("CONTENTS", "txt")]
                metadata = unicode(metadata, encoding="utf-8")
                result.append(metadata)
            if urlid > max_crawled_urlid:
                raise StopIteration
            if len(result) == 0:
                #if we do not have corresponding metadata do not generate
                #an element for this urlid
                continue
            yield urlid, result


def get_number_pages(data_directory_path):
    """Return the number of available pages
    data_directory_path: the path to the directory that contains the crawl data
    """
    urlinfos_stream_factory = FileStreamFactory(data_directory_path,
                                                "urlinfos")
    max_crawled_urlid = urlinfos_stream_factory.get_max_crawled_urlid()
    return _get_number_pages_from_stream(urlinfos_stream_factory.get_stream(),
                                         max_crawled_urlid)


def _get_number_pages_from_stream(urlinfos_stream, max_crawled_urlid):
    """Helper function (mainly here to make tests easier
    Return the number of available pages
    urlinfos_stream : a stream from the urlinfos files
    max_crawled_urlid : the highest urlid corresponding to a crawled page
    """
    result = 0
    for urlinfo in urlinfos_stream:
        urlid = urlinfo[idx_from_stream("INFOS", "id")]
        httpcode = urlinfo[idx_from_stream("INFOS", "http_code")]
        if urlid > max_crawled_urlid:
            break  # there will be no more crawled url
        if httpcode == 0:
            continue
        result += 1
    return result
