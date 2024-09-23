import json

def remove_instance_profile_arn(file_path, output_path):
    # Open and load the JSON file
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # Iterate through each row in the JSON file
    for row in data:
        try:
            # Check if 'aws_attributes' exists and remove 'instance_profile_arn'
            if 'aws_attributes' in row['settings']['new_cluster']:
                row['settings']['new_cluster']['aws_attributes'].pop('instance_profile_arn', None)
        except KeyError as e:
            print(f"Skipping row due to missing keys: {e}")

    # Write the updated data to the output file
    with open(output_path, 'w') as file:
        json.dump(data, file, indent=4)
    
    print(f"Instance profile ARNs removed. Updated file saved as {output_path}")

# Usage example
input_file = 'job.log'  # Path to the input JSON file
output_file = 'remove_ip_job.log'  # Path to the output JSON file
remove_instance_profile_arn(input_file, output_file)