import gzip

from ..ve_utils import clock
from ..settings import APPNEXUS_ACCOUNT

from .api import SegmentAPI


METRICS = ['num_valid', 'num_invalid_user', 'num_unauth_segment',
           'num_other_error', 'num_inactive_segment', 'num_valid_user',
           'num_invalid_segment', 'num_invalid_timestamp', 'num_invalid_format',
           'num_past_expiration']


@clock()
def upload_segment(segment_id, user_ids, verbose=False, metrics=METRICS, member_id=None):
    """
    Upload a segment with a list of user_ids to AppNexus
    :return:
    """
    api = SegmentAPI(**APPNEXUS_ACCOUNT, verbose=verbose)

    data = '\n'.join([str(x) for x in user_ids])
    data_fmt = format_data(data, segment_id)

    metrics = api.upload_segment(data_fmt, metrics=metrics, member_id=member_id)

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
