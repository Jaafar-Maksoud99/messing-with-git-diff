import base64
import os
import requests
import json

"""
    This file is used to deploy a single databricks notebook at a time. Before this there are a couple of devops yaml files that run to 
    determine the environment in which we need to deploy to. The entry point for the azure devops pipeline is the azure-pipelines.yml. From there
    a series of checks happen to determine the target branch, if we need to run automated test, and identifying the changed notebooks. Those notebooks 
    names will be written to the workflow_config_files_changed.txt file. See the notebook-deployment.yml file for how that works. Once we determine
    which env those variables from the variable groups in devops will be injected into this scrip so we deploy to the create databricks workspace.

    At a high level the changed notebooks name will look something like 

    {Root within Shared Directory}/{n number of directories}/{notebook}.py

    We will need to create the directory first if it does not exist so we can split on the / and strip out the {notebook}.py
    Then we hit the databricks api to create the directory and then move on to deploying the notebook. AUTO and overwrite are very 
    useful in the notebook deployment params as they allow databricks to handle some small logic for us. See the below links for more info on the API: 

    https://docs.databricks.com/api/azure/workspace/workspace/mkdirs
    https://docs.databricks.com/api/azure/workspace/workspace/import
"""

file_path = os.path.join(
    os.environ['WORKING_DIRECTORY'], 'workflowchanges/workflow_config_files_changed.txt')

# Variables for API Call to Databricks
HEADERS = {"Authorization": f"Bearer {os.environ['PAT']}"}
IMPORT_WORKSPACE_OBJ_ENDPOINT = '/api/2.0/workspace/import'
CREATE_DIRECTORY_ENDPOINT = '/api/2.0/workspace/mkdirs'
DELETE_WORKSPACE_OBJECT_ENDPOINT = '/api/2.0/workspace/delete'
WORKSPACE_URL = os.environ['WORKSPACE_URL']

notebooks_to_deploy = []
notebooks_to_rename = []
notebooks_to_delete = []

with open(filepath, 'r') as file:
    for line in file:
        if line[:1] == "R":
            notebooks_to_rename.append(line.split("|")[1:])
        elif line[:1] == "D":
            notebooks_to_delete.append(line.split("|")[1])
        else:
            notebooks_to_deploy.append(line.split("|")[1])
print(notebooks_to_deploy)
print(notebooks_to_rename)
print(notebooks_to_delete)

# If there are no changes to deploy
if len(notebooks_to_deploy) == 0 and len(notebooks_to_rename) == 0 and len(notebooks_to_delete) == 0:
    print('############################################################################')
    print('################## NO CHANGES DETECTED TO DEPLOY ###########################')
    print('############################################################################')
    # raise ValueError('FAILED - NO NOTEBOOKS DETECTED TO DEPLOY')
    exit

# For each changed file
for file in notebooks_to_deploy:
    print('####################################################################')
    print('Starting deployment of notebook: ' + file)

    # Ignore this file as its not a notebook but ends in .py
    if 'notebook-deployment' in file:
        print('Skipping this file as its not a databricks notebook')
        continue

    print('Base 64 Encoding the notebook')
    # Base 64 encode the notebook for the databricks API
    with open(file, 'rb') as notebook:
        encoded_notebook = base64.b64encode(notebook.read())

    # Create the directory if it doesn't exist. Not sure why this isn't built in to the notebook import api but CLV
    dir = '/'.join(file.split('/')[:-1])
    print(
        f"Attempting to create directory ({dir}) in case it does not exist")

    # Add the shared folder. So this will create the directory in Workspace/Shared/{dir}
    create_dir_body = {
        "path": f"/Shared/{dir}"
    }

    # Post to the API to create the directory
    create_dir_response = requests.post(
        WORKSPACE_URL + CREATE_DIRECTORY_ENDPOINT, data=json.dumps(create_dir_body), headers=HEADERS)

    # Validate the API call succeeded. Intentionally fail the pipeline if it does not
    if int(create_dir_response.status_code) != 200:
        print(create_dir_response.content)
        raise ValueError('FAILED - Unable to Create Directory')

    print('Directory has been successfully created or validated')
    print('Attempting to deploy notebook')

    # Parameters for deploying the notebook. AUTO is great because it will automatically strip the .py from the file name for us
    deploy_notebook_params = {
        "path": f"/Shared/{file}",
        "format": "AUTO",
        "language": "PYTHON",
        "content": encoded_notebook.decode('utf-8'),
        "overwrite": True
    }

    # Deploy the notebook to the workspace
    deploy_notebook_response = requests.post(
        WORKSPACE_URL + IMPORT_WORKSPACE_OBJ_ENDPOINT, data=json.dumps(deploy_notebook_params), headers=HEADERS)

    # Validate the notebook successfully deployed
    if int(deploy_notebook_response.status_code) != 200:
        print(deploy_notebook_response.content)
        raise ValueError('FAILED - Unable to Deploy Notebook ')

    print('Notebook Deployment Completed Successfully')


print('All notebooks have been successfully deployed')

for notebook in notebooks_to_delete:
    
    delete_notebook_params = {
        "path": f"/Shared/{notebook}",
        "recursive": "true"
    }

    # Delete the notebook 
    delete_notebook_response = requests.post(
        WORKSPACE_URL + DELETE_WORKSPACE_OBJECT_ENDPOINT, data=json.dumps(delete_notebook_params), headers=HEADERS)
    

    # Validate the notebook successfully deleted
    if int(delete_notebook_response.status_code) != 200:
        print(delete_notebook_response.content)
        raise ValueError('FAILED - Unable to delete Notebook ')
    
    print(f'{notebook} Deletion Completed Successfully')









