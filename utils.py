from datetime import datetime, timedelta
from hubstorage import HubstorageClient


def run_job(project, timeout, auth, **kwargs):
    hc = HubstorageClient(auth=auth)
    project = hc.get_project(project)
    key = project.push_job('py:run_pipeline.py', **kwargs).key

    running = True
    stop_at = datetime.now() + timedelta(seconds=timeout)
    while running:
        running = project.get_job(key).metadata['state'] in ('pending', 'running')
        print('Still running')

        if datetime.now() > stop_at:
            print('Timeout exceeded')
            running = False
    print('Finished')
