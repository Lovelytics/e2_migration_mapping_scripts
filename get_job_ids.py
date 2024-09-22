import requests
import csv
import os
import configparser
import argparse

# Function to load Databricks host and token from a specific profile in ~/.databrickscfg
def load_databricks_config(profile):
    config_file_path = os.path.expanduser("~/.databrickscfg")
    
    # Ensure the configuration file exists
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Databricks config file not found at {config_file_path}")

    # Parse the config file
    config = configparser.ConfigParser()
    config.read(config_file_path)

    if profile not in config:
        raise ValueError(f"Profile '{profile}' not found in Databricks CLI config.")

    # Retrieve the host and token for the given profile
    host = config[profile].get("host")
    token = config[profile].get("token")

    if not host or not token:
        raise ValueError(f"Host or token not found in profile '{profile}'.")

    return host, token

# Function to list all jobs in the Databricks workspace
def list_all_jobs(host, token):
    api_url = f"{host}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {token}"}
    
    job_ids = []
    has_more = True
    offset = 0
    
    while has_more:
        params = {'limit': 25, 'offset': offset}  # Paginate with limit and offset
        response = requests.get(api_url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            jobs = data.get('jobs', [])
            
            for job in jobs:
                job_ids.append(job['job_id'])
            
            has_more = data.get('has_more', False)
            offset += 25
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

    return job_ids

# Function to write job IDs to a CSV file
def write_job_ids_to_csv(job_ids, output_file):
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Job ID'])  # Writing header
        for job_id in job_ids:
            writer.writerow([job_id])  # Writing each job ID

# Main function to handle the process
def main():
    # Parse command-line arguments to get the profile
    parser = argparse.ArgumentParser(description="Fetch and store Databricks job IDs using a specified profile.")
    parser.add_argument('--profile', type=str, required=True, help="The Databricks CLI profile to use.")
    parser.add_argument('--output_csv', type=str, default="databricks_job_ids.csv", help="Output CSV file name.")
    args = parser.parse_args()

    # Load Databricks credentials from the specified profile
    host, token = load_databricks_config(args.profile)

    # Fetch all job IDs
    job_ids = list_all_jobs(host, token)

    # Write the job IDs to a CSV file
    write_job_ids_to_csv(job_ids, args.output_csv)

    print(f"Job IDs have been written to {args.output_csv}")

# Run the script
if __name__ == "__main__":
    main()
