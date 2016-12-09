import time
from io import BytesIO

from pynexus.base_api import BaseAPI
from pynexus import logs


class SegmentUploadError(Exception):
    """
    raised when an ERROR is raised by the AppNexus server when uploading the segments
    """
    pass


class SegmentAPI(BaseAPI):
    batch_segment_url = "{}/batch-segment".format(BaseAPI.base_url)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_segment_job_id(self, member_id=None):
        return self._make_request(method='POST', url=self.batch_segment_url,
                                  params={"member_id": member_id or self.member_id})

    def _get_segment_job_id(self, member_id=None):
        member_id = member_id or self.member_id
        resp = self._make_request(method='POST', url=self.batch_segment_url,
                                  params={"member_id": member_id})
        if resp.get('error_code') == "DB_UNKNOWN":
            raise ValueError('Invalid member_id. Error: %s' % resp['error'])

        return resp['batch_segment_upload_job']['upload_url'], resp['batch_segment_upload_job']['job_id']

    def _upload_segment(self, url, data):
        if isinstance(data, BytesIO):
            data.seek(0)

        resp = self._make_request(method='POST', url=url,
                                  headers={'Content-Type': 'application/octet-stream'},
                                  data=data)
        return resp['segment_upload']['job_id']

    def _get_segment_upload_progress(self, job_id, member_id=None):
        member_id = member_id or self.member_id
        return self._make_request(method='GET', url=self.batch_segment_url,
                                  params={'member_id': member_id, 'job_id': job_id})

    def upload_segment(self, data, metrics=None, member_id=None):
        """
        Upload a segment to AppNexus

        :param member_id: the member_id
        :param data: formatted data
        :param metrics: specify the metrics to extract
        :return: dictionnary containing the metrics or if not specified  the full result
        """
        member_id = member_id or self.member_id

        upload_url, job_id = self._get_segment_job_id(member_id)
        self._upload_segment(upload_url, data)

        resp = None
        for i in range(0, self.max_retry):
            resp = self._get_segment_upload_progress(job_id=job_id, member_id=member_id)
            try:
                if resp['status'] == "ERROR":
                    raise SegmentUploadError("[{error_code}]: {error}".format(error_code=resp['error_code'],
                                                                              error=resp['errors'][0]))
                # resp['batch_segment_upload_job']['phase'] == 'completed':
                elif resp['batch_segment_upload_job']["percent_complete"] == 100:
                    break
                else:
                    pass
            except KeyError as e:
                logs.logger.warning("Key '%s' not found in %s" % (e.args[0], resp))
            finally:
                time.sleep(2)

        if not resp or 'batch_segment_upload_job' not in resp:
            raise SegmentUploadError(resp)

        if metrics:
            return {x: resp['batch_segment_upload_job'].get(x) for x in metrics}
        else:
            return resp['batch_segment_upload_job']
