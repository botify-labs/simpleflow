from cdf.features.main.streams import CompliantUrlStreamDef
from cdf.features.rel.streams import (
    RelStreamDef,
    RelCompliantStreamDef
)
from cdf.features.main.compliant_url import make_compliant_bitarray

from cdf.tasks.decorators import TemporaryDirTask as with_temporary_dir


@with_temporary_dir
def convert_rel_out_to_rel_compliant_out(s3_uri, first_part_id_size, part_id_size,
                                         crawled_partitions, tmp_dir=None, force_fetch=False):
    """
    Take rel out stream and add compliancy flag for each destination url
    """
    compliant_stream = CompliantUrlStreamDef.load(s3_uri, tmp_dir=tmp_dir)
    size = first_part_id_size + part_id_size * max(crawled_partitions)
    compliant_bitarray = make_compliant_bitarray(compliant_stream, size)

    rc = RelCompliantStreamDef.create_temporary_dataset()
    for l in RelStreamDef.load(s3_uri, tmp_dir=tmp_dir):
        url_id_dest = l[3]
        if url_id_dest == -1:
            dest_compliant = ''
        elif compliant_bitarray[url_id_dest]:
            dest_compliant = '1'
        else:
            dest_compliant = '0'
        rc.append(*l + [dest_compliant])
    rc.persist(s3_uri, first_part_size=first_part_id_size, part_size=part_id_size)
