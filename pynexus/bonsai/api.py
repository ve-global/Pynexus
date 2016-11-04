import base64

from ..api import BaseAPI
from .. import logs


class BonsaiAPI(BaseAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_models(self):
        r = self._make_request(method="GET", url='%s/custom-model' % self.base_url)
        return r['custom_models']

    def get_model(self, model_id):
        params = {'id': model_id}
        r = self._make_request(method="GET", url='%s/custom-model' % self.base_url, params=params)
        return r

    def add_model(self, model_name, member_id, advertiser_id, model_output, model_str):
        logs.logger.info("1. Validating the model")
        model = self.get_model_checked(model_name, member_id, advertiser_id, model_output, model_str)
        if not model:
            return
        logs.logger.info('2. Uploading the model')
        response = self._make_request(method="POST", url="%s/custom-model" % self.base_url, json=model)
        try:
            logs.logger.error("{error_code}: {error}".format(**response))
        except KeyError:
            logs.logger.info("Status: %s" % response['status'])

    def modify_model(self, model_name, member_id, advertiser_id, model_str):
        logs.logger.info("1. Validating the model")
        model = self.get_model_checked(model_name, member_id, advertiser_id, model_str)
        if not model:
            return
        logs.logger.info("2. Uploading new model to '%s'" % model_name)

        try:
            model_id = [x['id'] for x in self.get_models() if x['name'] == model_name][0]
        except IndexError:
            logs.logger.error("Model name '%s' not found" % model_name)
            return
        params = {'id': model_id}
        response = self._make_request(method='PUT', url='%s/custom-model' % self.base_url, params=params, json=model)
        try:
            logs.logger.error("Error model modification: {error_code} - {error}".format(**response))
        except KeyError:
            logs.logger.info("Status: %s" % response['status'])

    def delete_model(self, model_id):
        params = {"id": model_id}
        r = self._make_request(method='DELETE', url='%s/custom-model' % self.base_url, params=params)
        try:
            logs.logger.info("Status: ", r['status'])
        except KeyError:
            logs.logger.info("Error: model_id: '%s' not found" % str(model_id))

    def check_model(self, model_str):
        model = base64.b64encode(model_str.encode('utf-8')).decode('utf-8')
        data = {'custom-model-parser': {'model_text': model}}
        response = self._make_request(method='POST', url='%s/custom-model-parser' % self.base_url, json=data)
        try:
            logs.logger.error("Invalid model: {error_code} - {error}".format(**response))
            return False, model
        except KeyError:
            return True, model

    def get_model_checked(self, model_name, member_id, advertiser_id, model_output, model_str):
        is_valid, model = self.check_model(model_str)
        if not is_valid:
            return

        model = {
            "custom_model": {
                "name": model_name,
                "member_id": member_id,
                "advertiser_id": advertiser_id,
                "custom_model_structure": "decision_tree",
                "model_output": model_output,
                "model_text": model
            }
        }
        return model
