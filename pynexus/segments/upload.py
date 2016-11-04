"""
How to run:
    - Make sure that the db and stats table exists (cf stats.py)
    - Run `python -m segments.upload --year 2016 --month 7 --day 11 --db="test"`

Batch segment upload:
  - https://wiki.appnexus.com/display/api/Batch+Segment+Service

"""

import gzip

from ..ve_utils import clock
from ..settings import APPNEXUS_ACCOUNT

from .api import SegmentAPI


METRICS = ['num_valid', 'num_invalid_user', 'num_unauth_segment',
           'num_other_error', 'num_inactive_segment', 'num_valid_user',
           'num_invalid_segment', 'num_invalid_timestamp', 'num_invalid_format',
           'num_past_expiration']


@clock()
def upload_segment(segment_id, user_ids, member_id, verbose=False, metrics=METRICS):
    """
    Upload a segment with a list of user_ids to AppNexus
    :return:
    """
    api = SegmentAPI(**APPNEXUS_ACCOUNT, verbose=verbose)

    data = '\n'.join([str(x) for x in user_ids])
    data_fmt = format_data(data, segment_id)

    metrics = api.upload_segment(member_id, data_fmt, metrics=metrics)

    return metrics


def format_data(data, segment_id, encoding='utf-8'):
    """
    Format the data to be compressed and compliant with AppNexus specs
    :param data: text file
    :param segment_id: the segment_id of the file
    :param encoding: the encoding to use
    :return: compressed data (bytes)
    """
    data_fmt = (data.replace('\n', ',%d:0\n' % segment_id)
                    .encode(encoding))
    return gzip.compress(data_fmt)
