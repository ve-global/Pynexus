from .base_api import BaseAPI


class AppNexusAPI(BaseAPI):

    campaign_url = "%s/campaign" % BaseAPI.base_url
    pixel_url = "%s/pixel" % BaseAPI.base_url
    device_url = '%s/device-model' % BaseAPI.base_url
    advertiser_url = '%s/advertiser' % BaseAPI.base_url
    line_item_url = '%s/line-item' % BaseAPI.base_url
    insertion_order_url = '%s/insertion-order' % BaseAPI.base_url
    segment_url = '%s/segment' % BaseAPI.base_url
    publisher_url = "%s/publisher" % BaseAPI.base_url
    inventory_resold_url = "%s/inventory-resold" % BaseAPI.base_url
    creative_url = "%s/creative" % BaseAPI.base_url
    operating_system = "%s/operating-system" % BaseAPI.base_url
    operating_system_extended = "%s/operating-system-extended" % BaseAPI.base_url
    change_log_url = '%s/change-log' % BaseAPI.base_url
    change_log_detail_url = '%s/change-log-detail' % BaseAPI.base_url
    country_url = "%s/country" % BaseAPI.base_url
    city_url = "%s/city" % BaseAPI.base_url
    browser_url = '%s/browser' % BaseAPI.base_url

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_campaign(self, ids=None, one_id=None, advertiser_id=None, only_names=True, **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Campaign+Service"""
        params = BaseAPI._get_params(ids=ids, one_id=one_id, advertiser_id=advertiser_id, **kwargs)
        resp = self._make_request(method="GET", url=self.campaign_url,
                                  params=params)
        resp = self._get_names(resp, 'campaign') if only_names else resp
        return resp

    def get_pixel(self, ids=None, one_id=None, advertiser_id=None,
                  advertiser_code=None, pixel_code=None, only_names=True, **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Conversion+Pixel+Service """
        params = BaseAPI._get_params(ids=ids, one_id=one_id, advertiser_id=advertiser_id,
                                     advertiser_code=advertiser_code, code=pixel_code, **kwargs)
        resp = self._make_request(method="GET", url=self.pixel_url, params=params)
        resp = self._get_names(resp, 'pixel') if only_names else resp

        return resp

    def get_device(self, one_id=None, device_type=None, only_names=True, **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Device+Model+Service """
        # TODO: implement meta
        params = BaseAPI._get_params(one_id=one_id, device_type=device_type,  **kwargs)

        resp = self._make_request(method="GET", url=self.device_url, params=params)
        resp = self._get_names(resp, 'device-model') if only_names else resp
        return resp

    def get_advertiser(self, ids=None, one_id=None, search_term=None, only_names=True, **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Advertiser+Service"""
        params = BaseAPI._get_params(ids=ids, one_id=one_id, search_term=search_term, **kwargs)
        resp = self._make_request(method="GET", url=self.advertiser_url,
                                  params=params)
        resp = self._get_names(resp, 'advertiser') if only_names else resp
        return resp

    def get_line_item(self, ids=None, one_id=None, advertiser_id=None, only_names=True, **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Line+Item+Service"""
        params = BaseAPI._get_params(ids=ids, one_id=one_id, advertiser_id=advertiser_id, **kwargs)
        resp = self._make_request(method="GET", url=self.line_item_url, params=params)
        resp = self._get_names(resp, 'line-item') if only_names else resp

        return resp

    def get_insertion_order(self, ids=None, one_id=None, advertiser_id=None, search_term=None, only_names=True,
                            **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Insertion+Order+Service"""
        params = BaseAPI._get_params(ids=ids, one_id=one_id,
                                     advertiser_id=advertiser_id, search_term=search_term, **kwargs)
        resp = self._make_request(method="GET", url=self.insertion_order_url, params=params)
        resp = self._get_names(resp, 'insertion-order') if only_names else resp

        return resp

    def get_publisher(self, ids=None, one_id=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(ids=ids, one_id=one_id, **kwargs)
        resp = self._make_request(method="GET", url=self.publisher_url, params=params)
        resp = self._get_names(resp, 'publisher') if only_names else resp
        return resp

    def get_resold_inventory(self, type='publisher', category_type=None, ids=None,
                             one_id=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(ids=ids, one_id=one_id, **kwargs)
        params.update({'type': type})
        if category_type:
            params['category_type'] = category_type
        resp = self._make_request(method="GET", url=self.inventory_resold_url, params=params)
        resp = self._get_names(resp, 'inventory-resold') if only_names else resp
        return resp

    def get_creative(self, ids=None, one_id=None, advertiser_id=None, publisher_id=None,
                     publisher_code=None, code=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(ids=ids, one_id=one_id, advertiser_id=advertiser_id,
                                     publisher_id=publisher_id, publisher_code=publisher_code,
                                     code=code, **kwargs)
        resp = self._make_request(method="GET", url=self.creative_url, params=params)
        resp = self._get_names(resp, 'creative') if only_names else resp
        return resp

    def get_operating_system(self, one_id=None, search_term=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(one_id=one_id, search_term=search_term, **kwargs)
        resp = self._make_request(method="GET", url=self.operating_system, params=params)
        resp = self._get_names(resp, 'operating-system') if only_names else resp
        return resp

    def get_browser(self, one_id=None, search_term=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(one_id=one_id, search_term=search_term, **kwargs)
        resp = self._make_request(method="GET", url=self.browser_url, params=params)
        resp = self._get_names(resp, 'browser') if only_names else resp
        return resp

    def get_country(self, one_id=None, name=None, code=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(one_id=one_id, country_code=code, name=name, **kwargs)
        resp = self._make_request(method="GET", url=self.country_url, params=params)
        resp = self._get_names(resp, 'country') if only_names else resp
        return resp

    def get_operating_system_extended(self, one_id=None, search_term=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(one_id=one_id, search_term=search_term, **kwargs)
        resp = self._make_request(method="GET", url=self.operating_system_extended, params=params)
        resp = self._get_names(resp, 'operating-systems-extended') if only_names else resp
        return resp

    def get_change_log(self, service='campaign', resource_id=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(resource_id=resource_id, service=service, **kwargs)
        resp = self._make_request(method="GET", url=self.change_log_url, params=params)
        resp = self._get_names(resp, 'change-log') if only_names else resp
        return resp

    def get_change_log_detail(self, service='campaign', resource_id=None, only_names=True,
                              transaction_id=None, **kwargs):
        params = BaseAPI._get_params(resource_id=resource_id, service=service, transaction_id=transaction_id, **kwargs)
        resp = self._make_request(method="GET", url=self.change_log_detail_url, params=params)
        resp = self._get_names(resp, 'change-log-detail') if only_names else resp
        return resp

    def get_city(self, country_code=None, country_name=None, dma_id=None, dma_name=None, one_id=None,
                 name=None, only_names=True, **kwargs):
        params = BaseAPI._get_params(country_code=country_code, country_name=country_name, dma_id=dma_id,
                                     dma_name=dma_name, one_id=one_id, name=name, **kwargs)
        resp = self._make_request(method="GET", url=self.city_url, params=params)
        resp = self._get_names(resp, 'city') if only_names else resp
        return resp

    def get_segment(self, one_id=None, ids=None, advertiser_id=None, advertiser_code=None, code=None,
                    only_names=True, **kwargs):
        """ cf https://wiki.appnexus.com/display/api/Segment+Service"""
        params = BaseAPI._get_params(ids=ids, one_id=one_id,
                                     advertiser_id=advertiser_id, code=code,
                                     advertiser_code=advertiser_code, **kwargs)
        resp = self._make_request(method="GET", url=self.segment_url,
                                  params=params)
        resp = self._get_names(resp, 'segment') if only_names else resp
        return resp

    def add_segment(self, segment):
        resp = self._make_request(method="POST", url=self.segment_url,
                                  json=segment)
        return resp

    def adv_segment_to_network(self, one_id, member_id):
        data = {"segment": {"advertiser_id": None}}
        params = BaseAPI._get_params(one_id=one_id, member_id=member_id)
        resp = self._make_request(method="PUT", url=self.segment_url, params=params,
                                  json=data)
        return resp

    def add_advertiser_segment(self):
        raise NotImplementedError()

    def modify_segment(self):
        raise NotImplementedError()

    def modify_advertiser_segment(self):
        raise NotImplementedError()

    def delete_segment(self, id):
        params = BaseAPI._get_params(one_id=id)
        resp = self._make_request(method="DELETE", url=self.segment_url,
                                  params=params)
        return resp
