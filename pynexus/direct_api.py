from .base_api import BaseAPI


class AppNexusDirectAPI(BaseAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_segment(self, member_id, segment_id=None, only_names=True, **kwargs):
        """ https://wiki.appnexus.com/display/adnexusdocumentation/Segment+Service """
        params_url = "{}".format(member_id) if not segment_id else "{}/{}".format(member_id, segment_id)
        params = BaseAPI._get_params(**kwargs)

        resp = self._make_request(method="GET",
                                  url='{}/segment/{}'.format(self.base_url, params_url),
                                  params=params)

        resp = self._get_names(resp, 'segment') if only_names else resp
        return resp

    def add_segment(self, segment):
        """ cf https://wiki.appnexus.com/display/api/Segment+Service
        :param segment: segment
        :return:
        """
        # Getting the segment_id
        resp = self._make_request(method="POST",
                                  url="{}/segment/{}".format(self.base_url_adnxs, segment['member_id']),
                                  json={"segment": segment})
        try:
            segment_id = resp['response']['id']
        except KeyError:
            print(resp)
            raise

        return segment_id

    def modify_segment(self, member_id, segment_id):
        raise NotImplementedError()

    def delete_segment(self, member_id, segment_id):
        raise NotImplementedError()
