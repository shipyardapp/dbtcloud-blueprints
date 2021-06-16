from httprequest_blueprints import execute_request, download_file
import argparse
import requests
import os
import code
import json
import time


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--account-id', dest='account_id', required=True)
    parser.add_argument('--job-id', dest='job_id', required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='',
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    args = parser.parse_args()
    return args


def write_json_to_file(json_object, file_name):
    with open(file_name, 'w') as f:
        f.write(
            json.dumps(
                json_object,
                ensure_ascii=False,
                indent=4))


def execute_job(account_id, job_id, header):
    execute_job_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/'
    body = {"cause": "Run from Postman"}
    print(f'Kicking off job {job_id} on account {account_id}')
    job_run_req = execute_request.execute_request(
        'POST', execute_job_url, header, body)
    job_run_response = json.loads(job_run_req.text)
    write_json_to_file(
        job_run_response, 'dbt/job_run_response.json')
    return job_run_response


def get_run_details(account_id, run_id, header):
    get_run_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/?include_related=[\'run_steps\',\'debug_logs\']'
    print(f'Grabbing run details for run {run_id}')
    run_details_req = execute_request.execute_request(
        'GET', get_run_details_url, header)
    run_details_response = json.loads(run_details_req.text)
    write_json_to_file(run_details_response, 'dbt/run_details_response.json')
    return run_details_response


def get_artifact_details(account_id, run_id, header):
    get_artifact_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/'
    print(f'Grabbing artifact details for run {run_id}')
    artifact_details_req = execute_request.execute_request(
        'GET', get_artifact_details_url, header)
    artifact_details_response = json.loads(artifact_details_req.text)
    write_json_to_file(
        artifact_details_response,
        'dbt/artifact_details_response.json')
    return artifact_details_response


def download_artifact(account_id, run_id, artifact_name, header):
    get_artifact_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{artifact_name}'
    artifact_details_req = download_file.download_file(
        get_artifact_details_url, f'dbt/{artifact_name}', header)


def main():
    args = get_args()
    account_id = args.account_id
    job_id = args.job_id
    api_key = args.api_key
    bearer_string = f'Bearer {api_key}'
    header = {'Authorization': bearer_string}

    job_run_response = execute_job(account_id, job_id, header)
    run_id = job_run_response['data']['id']

    is_complete = False
    while not is_complete:
        run_details_response = get_run_details(account_id, run_id, header)
        is_complete = run_details_response['data']['is_complete']
        if not is_complete:
            print(
                f'Run {run_id} is not complete. Waiting 30 seconds and trying again.')
            time.sleep(30)

    for step in run_details_response['data']['run_steps']:
        step_id = step['id']
        write_json_to_file(step, f'dbt/step_{step_id}_response.json')
        with open('dbt.log', 'a') as f:
            f.write(step['debug_logs'])
        with open('dbt_console_output.txt', 'a') as f:
            f.write(step['logs'])

    artifacts = get_artifact_details(account_id, run_id, header)
    for artifact in artifacts['data']:
        download_artifact(account_id, run_id, artifact, header)

    code.interact(local=locals())
    execute_request.write_response_to_file(req, 'output.json')


if __name__ == '__main__':
    main()
