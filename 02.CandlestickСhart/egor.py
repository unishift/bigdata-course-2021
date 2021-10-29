#!/usr/bin/python3

"""
Author: Egor Sklyarov
Date: 25/10/2021
"""

import json
import argparse
import requests


class HueAPI:
    def __init__(self, args):
        self.host = args.host
        self.user = args.user
        self.password = args.password
        self.session = requests.Session()

    def _url(self, query: str):
        return "{}/{}".format(self.host.rstrip('/'), query.lstrip('/'))

    def _post(self, url: str, data: dict, headers=None, **kwargs):
        if headers is None:
            headers = {}
        headers['X-CSRFToken'] = self.session.cookies['csrftoken']
        return self.session.post(url, data=data, headers=headers, **kwargs)

    def auth(self):
        login_url = self._url("accounts/login")
        self.session.get(login_url)  # get csrf token first time
        return self._post(login_url, data=dict(username=self.user, password=self.password))

    def jobbrowser_jobs(self):
        data = dict(
            interface=json.dumps("jobs"),
            filters=json.dumps([
                {"text": "user:{}".format(self.user)},
                {"time": {"time_value": 7, "time_unit": "days"}},
                {"states": []},
                {"pagination": {"page": 1, "offset": 1, "limit": 100}}
            ])
        )
        r = self._post(self._url("jobbrowser/api/jobs/jobs"), data=data)
        return r

    def jobbrowser_profile(self, app_id: str, app_type: str):
        data = dict(
            app_id=json.dumps(app_id),
            interface=json.dumps("jobs"),
            app_type=json.dumps(app_type),
            app_property=json.dumps("tasks"),
            app_filters=json.dumps([{"text": ""}, {"states": []}, {"types": []}])
        )
        return self._post(self._url("jobbrowser/api/job/profile"), data=data)

    def filebrowser(self, view: str = None, pagesize: int = 1000, pagenum: int = 1):
        if view is None:
            view = '/user/{}'.format(self.user)  # home catalog
        return self.session.get(self._url("/filebrowser/view={}?pagesize={}&pagenum={}&filter=&sortby=name&descending=false&format=json").format(view, pagesize, pagenum))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user')
    parser.add_argument('-p', '--password')
    parser.add_argument('--host', default="http://users.bigdata.local:8888/")
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default='-')
    args = parser.parse_args()

    hue = HueAPI(args)
    hue.auth()

    jobs = []
    mapred_expected = True
    job = None
    for app in hue.jobbrowser_jobs().json()['apps']:
        if app['status'] != 'SUCCEEDED':
            continue
        if mapred_expected and app['type'] != 'MAPREDUCE':
            continue
        if mapred_expected:  # MAPREDUCE
            job = dict(id=app['id'], duration=app['duration'], submitted=app['submitted'])
            task_list = hue.jobbrowser_profile(app['id'], app['type']).json()['tasks']['task_list']
            job['map'] = [
                dict(elapsed=task['elapsedTime'], started=task['startTime'], id=task['id'])
                for task in task_list if task['type'] == 'MAP'
            ]
            job['reduce'] = [
                dict(elapsed=task['elapsedTime'], started=task['startTime'], id=task['id'])
                for task in task_list if task['type'] == 'REDUCE'
            ]
        else:  # oozie launcher: to obtain saved query name
            job['name'] = app['name']
            jobs.append(job)
            job = None
        mapred_expected = not mapred_expected

    json.dump(jobs, args.output, indent=2)
    return hue


if __name__ == '__main__':
    hue = main()
