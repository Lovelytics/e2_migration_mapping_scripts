import subprocess
import json
import argparse
import csv

def get_all_jobs(profile):
    """Fetches the list of all jobs in the workspace using the Databricks CLI."""
    try:
        # List all jobs using Databricks CLI
        result = subprocess.run(
            ['databricks', '--profile', profile, 'jobs', 'list'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error fetching job list: {result.stderr}")
            return None
        
        # Parse the output (which will be JSON if the output format is specified)
        jobs_list = json.loads(result.stdout)
        return jobs_list

    except Exception as e:
        print(f"An error occurred while fetching jobs: {e}")
        return None

def get_job_details(profile, job_id):
    """Fetches details of a specific job using the Databricks CLI."""
    try:
        # Get job details using the Databricks CLI
        result = subprocess.run(
            ['databricks', '--profile', profile, 'jobs', 'get', '--job-id', str(job_id)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error fetching job details for job_id {job_id}: {result.stderr}")
            return None
        
        # Parse the job details JSON
        job_info = json.loads(result.stdout)
        return job_info

    except Exception as e:
        print(f"An error occurred while fetching job details for job_id {job_id}: {e}")
        return None

def save_jobs_to_csv(profile, csv_file):
    """Fetch all jobs and save their details to a CSV file."""
    # Fetch the list of all jobs
    jobs_list = get_all_jobs(profile)

    if jobs_list:
        # Open the CSV file to write
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            # Write the header row
            writer.writerow(['Job ID', 'Job Name', 'Cluster Policy', 'Instance Profile'])
            
            # Iterate through jobs and write details
            for job in jobs_list.get('jobs', []):
                job_id = job.get('job_id')
                job_name = job.get('settings', {}).get('name', 'N/A')
                
                # Get detailed information for each job
                job_info = get_job_details(profile, job_id)
                
                if job_info:
                    # Extract cluster policy and instance profile
                    cluster_spec = job_info.get('settings', {}).get('new_cluster', {})
                    cluster_policy = cluster_spec.get('policy_id', 'N/A')
                    instance_profile = cluster_spec.get('aws_attributes', {}).get('instance_profile_arn', 'N/A')

                    # Write job details to CSV
                    writer.writerow([job_id, job_name, cluster_policy, instance_profile])
        
        print(f"Job details saved to {csv_file}")
    else:
        print("No jobs found.")

def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Get Databricks job information using Databricks CLI and save to CSV")
    parser.add_argument('--profile', required=True, help='Databricks CLI profile to use')
    parser.add_argument('--csv-file', required=True, help='Path to the output CSV file')

    args = parser.parse_args()

    # Save jobs details to CSV
    save_jobs_to_csv(args.profile, args.csv_file)

if __name__ == "__main__":
    main()
