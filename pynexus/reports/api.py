import os
import time
import datetime as dt
from collections import namedtuple

from ..ve_utils import clock, zip_files
from ..base_api import BaseAPI, InvalidParamsError, tqdm_list, trange


class ReportNotDownloadedError(Exception):
    """
    raised if the report could not be downloaded.
    This typucally can happen if it took more than 15min for the AppNexus server to compute the report
    """
    pass


Report = namedtuple('Report', ['type', 'path', 'file'])


class ReportsAPI(BaseAPI):
    report_url = "{}/report".format(BaseAPI.base_url)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _write_report(self, report, path, report_type):
        """
        Makes calls to get the report `report_type` with params `report` and write it to `path`
        :param report: a dict containing the parameters of the report
        :param path: where to write the report
        :param report_type: the type of the report
        :return: the file
        """
        file, resp = None, None

        def part_1():
            nonlocal resp
            response = self._make_request(method='POST', url=self.report_url,
                                          json=report)
            if "error" in response:
                raise InvalidParamsError("[%s]: %s" % (report_type, response['error']))

            resp = response

        def part_2():
            nonlocal resp
            max_retry_wait = 500  # 15min
            success = False
            for _ in trange(max_retry_wait, desc="pending state", leave=False):
                # Workaround for the bar to leave
                if success:
                    continue

                response = self._make_request(url=self.report_url, method='GET',
                                              params={'id': resp['report_id']})
                if response['execution_status'] != "pending":
                    success = True
                time.sleep(2)

            if not success:
                raise ReportNotDownloadedError('Report could not be downloaded')
            resp = response

        def part_3():
            nonlocal file

            download_url = "{base_url}/{url}".format(base_url=BaseAPI.base_url,
                                                     url=resp['report']['url'])
            file = self._download_file(url=download_url, path=path,
                                       file_size=resp['report']['report_size'])

        processes = [part_1, part_2, part_3]
        for f in tqdm_list(processes, desc="Progress", leave=False):
            f()

        return file

    @clock()
    def get_report(self, report_fields):
        """
        Get a report and returns it
        :param report_fields:
        :return: Report namedtuple
        """
        report_type = report_fields['report']['report_type']
        file = self._write_report(report_fields, None, report_type)
        return Report(report_type, None, file)

    @clock()
    def get_reports(self, reports_fields):
        """
        Get the reports an return them
        :param reports_fields: a dict containing the name of the reports and the parameters of the reports
        :return: dict of the reports
        """
        reports = {}
        for report_name, report_field in tqdm_list(reports_fields.items(), desc="Reports", leave=False):
            reports[report_name] = self.get_report(report_field)

        return reports

    @clock()
    def save_report(self, report_name, report_fields, reports_folder):
        """Refer to Refer to https://wiki.appnexus.com/display/api/Report+Service
        :param report_name: name of the report
        :param reports_folder: folder to write the results to
        :param report_fields: the report parameters
        :return: the type of the report and the path of the file
        """
        report_type = report_fields['report']['report_type']
        reports_folder = reports_folder or os.getcwd()

        path = "{folder}/{report_name}.csv".format(
            folder=reports_folder,
            report_name=report_name)

        self._write_report(report_fields, path, report_type)

        return Report(report_type, path, None)

    @clock()
    def save_reports(self, reports_fields, reports_folder, zip_reports=True, zip_name=None):
        """
        Refer to Refer to https://wiki.appnexus.com/display/api/Report+Service
        :param reports_folder: folder to write the reports to
        :param reports_fields: a dict containing the name of the reports and the parameters of the reports
        :param zip_reports: zip the reports to or not
        :param zip_name: the name of the zip file. If not specified, the name is set to
                        `"reports_{}".format(dt.datetime.now().date())`
        :return:
        """
        if zip_reports:
            reports = self.get_reports(reports_fields)
            result = self.zip_reports(reports, reports_folder, zip_name)
        else:
            reports = {}
            for report_name, report_field in tqdm_list(reports_fields.items(), desc="Reports", leave=False):
                report_result = self.save_report(report_name, report_field,
                                                 reports_folder)
                reports[report_result.path] = report_result
            result = reports

        return result

    def get_reports_meta(self):
        response = self._make_request(method='GET', url="%s?meta" % self.report_url)
        return response

    def get_reports_hist(self):
        response = self._make_request(method="GET", url=self.report_url)
        return response

    @staticmethod
    def zip_reports(reports, reports_folder, zip_name=None):
        zip_name = zip_name or "reports_{}".format(dt.datetime.now().date())
        zip_file = zip_files({k: report.file for k, report in reports.items()})
        zip_path = "{reports_folder}/{zip_name}.zip".format(reports_folder=reports_folder, zip_name=zip_name)
        with open(zip_path, 'wb') as f:
            f.write(zip_file.read())
        return zip_path
