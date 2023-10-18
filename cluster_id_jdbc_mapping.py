import requests
import argparse
import pandas as pd
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

def main(ST, E2, STTOKEN, E2TOKEN, ID):
    st_url = ST + "/api/2.0/clusters/list"
    st_token = STTOKEN
    st_headers = {
        "Authorization": f"Bearer {st_token}"
    }

    st_response = requests.get(st_url, headers = st_headers)


    if st_response.status_code == 200:
        st_clusters = st_response.json()['clusters']
    else:
        print('Failed to retrieve list of ST clusters.')
        return


    e2_url = E2 + "/api/2.0/clusters/list"
    e2_token = E2TOKEN
    e2_headers = {
        "Authorization": f"Bearer {e2_token}"
    }

    e2_response = requests.get(e2_url, headers = e2_headers)

    if e2_response.status_code == 200:
        e2_clusters = e2_response.json()['clusters']

    else:
        print('Failed to retrieve list of E2 clusters.')
        return

    st_cluster_ids = []
    st_jdbc_urls = []
    cluster_names = []
    e2_cluster_ids = []
    e2_jdbc_urls = []
    init = "jdbc:spark://"
    transportMode = ":443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/"
    auth = ";AuthMech=3;UID=token;PWD=<personal-access-token>"
    
    
    for i in e2_clusters:
        for j in st_clusters:
            if i['cluster_name'] == j['cluster_name']:
                st_cluster_ids.append(j['cluster_id'])
                e2_cluster_ids.append(i['cluster_id'])
                cluster_names.append(i['cluster_name'])
                st_jdbc_urls.append(init + ST + transportMode + '0/' + j['cluster_id'] + auth)
                e2_jdbc_urls.append(init + E2 + transportMode + ID + '/' + i['cluster_id'] + auth)
                break

    pd.DataFrame.from_dict({"ST cluster ID": st_cluster_ids,
                            "ST JDBC URL": st_jdbc_urls,
                            "cluster Name": cluster_names,
                            "E2 cluster ID": e2_cluster_ids,
                            "E2 JDBC URL": e2_jdbc_urls,
                           }).to_csv("Cluster_ID_Mapping.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate cluster ID and JDBC URL mapping for an E2 Migration.")
    parser.add_argument('--old-profile', dest="old", help="Profile of the old workspace. ")
    parser.add_argument('--new-profile', dest="new", help="Profile of the new workspace. ")
    parser.add_argument("--E2id", dest="ID", help="E2 Workspace ID.")
    parser = parser.parse_args()

    old_profile = parser.old
    new_profile = parser.new
    ID = parser.ID

    old_dbclient = dbclient(profile=old_profile)
    ST, STTOKEN = old_dbclient.get_url_token()

    new_dbclient = dbclient(profile=new_profile)
    E2, E2TOKEN = new_dbclient.get_url_token()

    # parser.add_argument("--E2workspace", "--E2", dest="E2", help="URL to the E2 workspace")
    # parser.add_argument("--E2token", dest="E2TOKEN", help="E2 token for access.")
    # parser.add_argument("--STworkspace", "--ST", dest="ST", help="URL to the ST workspace")
    # parser.add_argument("--STtoken", dest="STTOKEN", help="ST token for access.")
    
    # parser = parser.parse_args()
    main(ST, E2, STTOKEN, E2TOKEN, ID)