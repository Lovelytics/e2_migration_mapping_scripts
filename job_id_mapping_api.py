#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
import pandas as pd
import argparse
import configparser
from os import path

class dbclient: 
    def __init__(self, profile): 
        login = self.get_login_credentials(profile)
        url = login['host']
        token = login['token']  
        self.url = self.url_validation(url)
        self.token = token
    
    def url_validation(self, url):
        if '/?o=' in url:
            # if the workspace_id exists, lets remove it from the URL
            url = re.sub("\/\?o=.*", '', url)
        elif 'net/' == url[-4:]:
            url = url[:-1]
        elif 'com/' == url[-4:]:
            url = url[:-1]
        return url.rstrip("/")
    
    def get_login_credentials(self, profile='DEFAULT'):
        creds_path = '~/.databrickscfg'
        config = configparser.ConfigParser()
        abs_creds_path = path.expanduser(creds_path)
        config.read(abs_creds_path)
        try:
            current_profile = dict(config[profile])
            if not current_profile:
                raise ValueError(f"Unable to find a defined profile to run this tool. Profile \'{profile}\' not found.")
            return current_profile
        except KeyError:
            raise ValueError(
                'Unable to find credentials to load for profile. Profile only supports tokens.')

    def get_url_token(self): 
        return self.url, self.token

def main(ST, E2, STTOKEN, E2TOKEN):
    if ST[-1] == '/':
        st_url = ST + "api/2.0/jobs/list"
    else:
        st_url = ST + "/api/2.0/jobs/list"
    st_token = STTOKEN
    st_headers = {
        "Authorization": f"Bearer {st_token}"
    }

    st_response = requests.get(st_url, headers = st_headers)


    if st_response.status_code == 200:
        st_jobs = st_response.json()['jobs']
    else:
        print(st_response.content)

    if E2[-1] == '/':
        e2_url = E2 + "api/2.0/jobs/list"
    else:
        e2_url = E2 + "/api/2.0/jobs/list"
    e2_token = E2TOKEN
    e2_headers = {
        "Authorization": f"Bearer {e2_token}"
    }

    e2_response = requests.get(e2_url, headers = e2_headers)

    if e2_response.status_code == 200:
        e2_jobs = e2_response.json()['jobs']
    else:
        print(e2_jobs.content)

    st_job_ids = []
    job_names = []
    e2_job_ids = []
    for i in e2_jobs:
        for j in st_jobs:
            if i['settings']['name'] == j['settings']['name']:
                st_job_ids.append(j['job_id'])
                e2_job_ids.append(i['job_id'])
                job_names.append(i['settings']['name'])

    pd.DataFrame.from_dict({"ST Job ID": st_job_ids,
                            "Job Name": job_names,
                            "E2 Job ID": e2_job_ids,
                           }).to_csv("Job_ID_Mapping.csv")


# In[ ]:


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move sample jobs for an E2 Migration.")
    parser.add_argument('--old-profile', dest="old", help="Profile of the old workspace. ")
    parser.add_argument('--new-profile', dest="new", help="Profile of the new workspace. ")
    # parser.add_argument("--E2workspace", "--E2", dest="E2", help="URL to the E2 workspace")
    # parser.add_argument("--E2token", dest="E2TOKEN", help="E2 token for access.")
    # parser.add_argument("--STworkspace", "--ST", dest="ST", help="URL to the ST workspace")
    # parser.add_argument("--STtoken", dest="STTOKEN", help="ST token for access.")
    parser = parser.parse_args()

    old_profile = parser.old
    new_profile = parser.new

    old_dbclient = dbclient(profile=old_profile)
    ST, STTOKEN = old_dbclient.get_url_token()

    new_dbclient = dbclient(profile=new_profile)
    E2, E2TOKEN = new_dbclient.get_url_token()

    main(ST, E2, STTOKEN, E2TOKEN)

