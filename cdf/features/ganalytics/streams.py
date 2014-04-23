from cdf.core.features import StreamDefBase


class RowVisitsStreamDef(StreamDefBase):
    FILE = 'analytics_raw_data'
    HEADERS = (
        ('url', str),
        ('medium', str),
        ('source', str),
        ('nb_visits', int),
    )


class VisitsStreamDef(StreamDefBase):
    FILE = 'analytics_data'
    HEADERS = (
        ('url_id', int),
        ('medium', str),
        ('source', str),
        ('nb_visits', int),
    )

    def process_document(self):
        pass
