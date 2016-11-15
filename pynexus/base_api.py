import logging
import time
import requests
import math
from io import BytesIO

from . import logs
from .ve_utils import get_chunks, is_notebook

if is_notebook():
    from tqdm import tqdm_notebook as tqdm_list
    from tqdm import tnrange as trange
else:
    from tqdm import tqdm as tqdm_list
    from tqdm import trange


class InvalidLoginError(Exception):
    """
    raised when the login provided is not the good one
    """
    pass


class TooManyRequestsError(Exception):
    """
    raised when too many retry or made (depending on `max_retry`)
    """
    pass


class InvalidParamsError(Exception):
    """
    raised when the parameters provided for an API call or not correct
    (except AUTH related errors)
    """
    pass


class NoTransactionDataError(Exception):
    """
    raised when no data is found when calling change-log
    """
    pass


class BaseAPI(object):
    """
    Handler for the AppNexus API
    """
    base_url = "https://api.appnexus.com"
    base_url_adnxs = "https://api.adnxs.com/v1.17"
    auth_url = "{}/auth".format(base_url)
    auth_url_adnx = "{}/auth".format(base_url_adnxs)

    max_elems =100

    def __init__(self, username, password, session=None, max_retry=10, timeout=5,
                 sleep_time=None, verbose=False):
        """ The API time out @ ~ 15 min
        :param username: the AppNexus API username
        :param password: the AppNexus API password
        :param session: a requests.Session() to use
        :param max_retry: the number of times the API will try to complete the request if not successful
        :param timeout: timeout is second
        :param verbose: run in verbose mode
        :param sleep_time: sleep_time between each requests
        """
        self.user = {"username": username, "password": password}
        self.session = session or requests.Session()
        self.max_retry = max_retry
        self.sleep_time = sleep_time
        self.timeout = timeout
        self._verbose = verbose

        if not self._verbose:
            logging.getLogger("requests").setLevel(logging.WARNING)

        self._member_id = None

    @property
    def member_id(self):
        if not self._member_id:
            self.load_member_id()
        return self._member_id

    @property
    def verbose(self):
        return self._verbose

    @verbose.setter
    def verbose(self, value):
        lvl = logging.INFO if value else logging.WARNING
        logging.getLogger("requests").setLevel(lvl)
        self._verbose = value

    def _make_request(self, is_json=True, max_retry=None, *args, **kwargs):
        """Make a request to the AppNexus API. Sign-In if necessary to the App.

        :param is_json: the expected result is a json or not
        :param max_retry: the number of times the API will try to complete the request if not successful
        :param args: args for requests
        :param kwargs: kwargs for requests
        :return:
        """
        resp = None
        max_retry = max_retry or self.max_retry

        if self.sleep_time:
            time.sleep(self.sleep_time)

        for i in range(1, max_retry):
            try:
                resp = self.session.request(timeout=self.timeout, *args, **kwargs)
            except (requests.Timeout, requests.ConnectionError) as e:
                logs.logger.warning('(%s)... retrying (%d/%d)' % (e.args[0], i + 1, max_retry))
                time.sleep(2 * max_retry)
            else:
                try:
                    response = resp.json()['response']
                except KeyError:
                    raise KeyError('response not in %s' % resp.json())

                if resp.status_code == 401 or 'error_id' in response:
                    if response.get('error_code') == 'INVALID_LOGIN':
                        raise InvalidLoginError('Login is not a valid one')
                    elif response['error_id'] == 'NOAUTH':
                        url = self.base_url if self.base_url in kwargs['url'] else self.base_url_adnxs
                        _ = self._make_request(method='POST', url="{}/auth".format(url),
                                               json={"auth": self.user})
                    elif response.get('error_code') == 'RATE_EXCEEDED':
                        logs.logger.warning('%s...  sleeping 60sec, retrying (%d/%d)' % (response['error'],
                                                                                         i + 1, max_retry))
                        time.sleep(60)
                    else:
                        if response.get('error_message') == 'no transaction data is found':
                            raise NoTransactionDataError('No data found')
                        raise InvalidParamsError("{error_id} : {error}".format(**response))
                else:
                    break

        if not resp:
            raise TooManyRequestsError()

        return resp.json()['response'] if is_json else resp

    def _download_file(self, url, path=None, chunk_size=1024, file_size=None):
        """Download the file at the given `url` and write it to `path`

        :param url: url of the file to download
        :param path: path where to write the file, is None specified, writes to BytesIO
        :param chunk_size: how much of the content to read per iteration
        :param file_size: the size of the file (in bytes)

        :return: BytesIO if no path is specified otherwise nothing
        """
        response = self.session.get(url, stream=True)
        if response.status_code != 200:
            return response

        file_size = file_size or response.headers.get('Content-Length')
        total = math.floor(float(file_size) / chunk_size) if file_size else 1

        f = open(path, 'wb') if path else BytesIO()

        for chunk in tqdm_list(response.iter_content(chunk_size=chunk_size),
                               total=total, leave=False, desc='file'):
            if chunk:
                f.write(chunk)

        if not isinstance(f, BytesIO):
            f.close()

        return f if not path else None

    def load_member_id(self):
        resp = self._make_request(url="{}/member".format(self.base_url), method='GET')
        self._member_id = resp['member']['id']

    @staticmethod
    def _get_params(ids=None, one_id=None, advertiser_id=None, search_term=None,
                    advertiser_code=None, code=None, device_type=None, member_id=None,
                    publisher_code=None, publisher_id=None,
                    resource_id=None, service=None, transaction_id=None,
                    country_code=None, country_name=None, dma_id=None, dma_name=None,
                    name=None,
                    start_element=None, num_elements=None, filters=None):
        """
        Format the parameters for a request
        :param ids:
        :param one_id:
        :param advertiser_id:
        :param search_term:
        :param advertiser_code:
        :param device_type:
        :return:
        """
        params = {}
        if ids:
            params = {"id": ','.join([str(x) for x in ids])}
        elif one_id and not advertiser_id:
            params = {"id": one_id}
        elif one_id and advertiser_id:
            params = {"id": one_id, "advertiser_id": advertiser_id}
        elif one_id and member_id:
            params = {'id': one_id, "member_id": member_id}
        elif advertiser_id:
            params = {"advertiser_id": advertiser_id}
        elif search_term:
            params = {"search": search_term}
        elif advertiser_code:
            params = {"advertiser_code": advertiser_code}
        elif advertiser_code and code:
            params = {"advertiser_code": advertiser_code, "code": code}
        elif publisher_code:
            params = {'publisher_code': publisher_code}
        elif publisher_id:
            params = {'publisher_id': publisher_id}
        elif device_type:
            params = {"device_type": device_type}
        elif code:
            params = {"code": code}
        elif service and resource_id and transaction_id:
            params = {"service": service, "resource_id": resource_id, "transaction_id": transaction_id}
        elif service and resource_id:
            params = {"service": service, "resource_id": resource_id}
        elif country_code:
            params = {"country_code": country_code}
        elif country_name:
            params = {"country_name": country_code}
        elif dma_id:
            params = {"dma_id": country_code}
        elif dma_name:
            params = {"dma_name": country_code}
        elif name:
            params = {'name': name}
        else:
            # raise ValueError('No parameters specified')
            pass

        if start_element:
            params['start_element'] = start_element
        if num_elements:
            params['num_elements'] = num_elements
        if filters:
            params.update(filters)

        return params

    @staticmethod
    def _get_names(response, key):
        """
        Extract the names from a response
        :param response:
        :param key:
        :return:
        """
        names = {}

        key_plural = "{}s".format(key)

        if key_plural in response:
            names = {x['id']: x.get('name') or x.get('short_name') for x in response[key_plural]}
        elif key in response:
            if isinstance(response[key], list):
                names = {x['id']: x.get('name') or x.get('short_name') for x in response[key]}
            else:
                names = {response[key]['id']: response[key].get('name') or response[key].get('short_name')}
        else:
            pass
        return names

    @staticmethod
    def bulk_requests(func, ids, **kwargs):
        """
        Given a list of ids, make as many requests as necessary to get a matching for all the ids
        as we are limited by 100 items per answer.

        :param func:
        :param ids:
        :return:
        """
        not_only_names = kwargs.get('only_names') is False
        data = {} if not not_only_names else []
        for id_chunk in tqdm_list(get_chunks(ids, 100), total=math.ceil(len(ids) / 100)):
            res = func(ids=id_chunk, **kwargs)
            data.update(res) if not not_only_names else data.append(res)
        return data

    @staticmethod
    def bulk_request_get_all(func, only_names=True, limit=None, **kwargs):
        """
        Get all the results available using pagination for a given call that may returns
        more than 100 results

        :param func: the get function
        :param only_names: returns only names or not
        :param limit: limit for the number of calls
        :param kwargs: arguments to pass to the get function
        :return:
        """
        resp = func(**kwargs, start_element=0, num_elements=1, only_names=False)

        count = resp['count']
        logs.logger.info('%d elements found' % count)
        total_calls = math.ceil(count / BaseAPI.max_elems)

        results = {} if only_names else []
        for i in trange(0, total_calls):

            if limit and i >= limit:
                logs.logger.info('\t%d/%d, breaking loop' % (i, limit))
                break

            res = func(**kwargs, only_names=only_names,
                       start_element=i * BaseAPI.max_elems, num_elements=BaseAPI.max_elems)

            if isinstance(results, dict):
                results.update(res)
            else:
                results.append(res)

        return results


