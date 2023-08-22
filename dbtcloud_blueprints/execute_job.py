from httprequest_blueprints import execute_request, download_file

import argparse
import os
import json
import time
import platform
import sys
import shipyard_utils as shipyard
import code

# Handle import difference between local and github install
try:
    import check_run_status
    import download_logs_artifacts
except BaseException:
    from . import check_run_status
    from . import download_logs_artifacts

EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_RESOURCE = 201

EXIT_CODE_STATUS_INCOMPLETE = 210
EXIT_CODE_FINAL_STATUS_ERRORED = 211
EXIT_CODE_FINAL_STATUS_CANCELLED = 212


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--account-id', dest='account_id', required=True)
    parser.add_argument('--job-id', dest='job_id', required=True)
    parser.add_argument(
        '--download-artifacts',
        dest='download_artifacts',
        default='TRUE',
        choices={
            'TRUE',
            'FALSE'},
        required=False)
    parser.add_argument(
        '--download-logs',
        dest='download_logs',
        default='TRUE',
        choices={
            'TRUE',
            'FALSE'},
        required=False)
    parser.add_argument(
        '--wait-for-completion',
        dest='wait_for_completion',
        default='TRUE',
        required=False)
    return parser.parse_args()


def determine_connection_status(run_details_response):
    status_code = run_details_response['status']['code']
    user_message = run_details_response['status']['user_message']
    if status_code == 401:
        if 'Invalid token' in user_message:
            print('The Service Token provided was invalid. Check to make sure there are no typos or preceding/trailing spaces.')
            print(f'dbt API says: {user_message}')
            sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
        else:
            print(
                f'An unknown error occurred with a status code of {status_code}')
            print(f'dbt API says: {user_message}')
            sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    if status_code == 404:
        if 'requested resource not found':
            print('The Account ID, Job ID, or Run ID provided was either invalid or your API Key doesn\'t have access to it. Check to make sure there are no typos or preceding/trailing spaces.')
            print(f'dbt API says: {user_message}')
            sys.exit(EXIT_CODE_INVALID_RESOURCE)
        else:
            print(
                f'An unknown error occurred with a status code of {status_code}')
            print(f'dbt API says: {user_message}')
            sys.exit(EXIT_CODE_UNKNOWN_ERROR)


def execute_job(
        account_id,
        job_id,
        headers,
        folder_name,
        file_name='job_details_response.json'):
    execute_job_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/'

    source_information = f'Fleet ID: {os.environ.get("SHIPYARD_FLEET_ID")} Vessel ID: {os.environ.get("SHIPYARD_VESSEL_ID")} Log ID: {os.environ.get("SHIPYARD_LOG_ID")}' if os.environ.get(
        'USER') == 'shipyard' else f'Run on {platform.platform()}'

    body = {"cause": f"Run by {os.environ['USER']} - {source_information}"}
    print(f'Kicking off job {job_id} on account {account_id}')
    job_run_req = execute_request.execute_request(
        'POST', execute_job_url, headers, body)
    job_run_response = json.loads(job_run_req.text)
    shipyard.files.create_folder_if_dne(folder_name)
    combined_name = shipyard.files.combine_folder_and_file_name(
        folder_name, file_name)
    shipyard.files.write_json_to_file(
        job_run_response, combined_name)
    return job_run_response


def main():
    args = get_args()
    account_id = args.account_id
    job_id = args.job_id
    api_key = args.api_key
    download_artifacts = shipyard.args.convert_to_boolean(
        args.download_artifacts)
    download_logs = shipyard.args.convert_to_boolean(args.download_logs)
    wait_for_completion = shipyard.args.convert_to_boolean(args.wait_for_completion)
    bearer_string = f'Bearer {api_key}'
    headers = {'Authorization': bearer_string}

    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'dbtcloud')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    job_run_response = execute_job(
        account_id,
        job_id,
        headers,
        folder_name=artifact_subfolder_paths['responses'],
        file_name=f'job_{job_id}_response.json')

    determine_connection_status(job_run_response)

    run_id = job_run_response['data']['id']
    shipyard.logs.create_pickle_file(
        artifact_subfolder_paths, 'run_id', run_id)

    if wait_for_completion:
        is_complete = False
        while not is_complete:
            run_details_response = check_run_status.get_run_details(
                account_id,
                run_id,
                headers,
                folder_name=f'{base_folder_name}/responses',
                file_name=f'run_{run_id}_response.json')
            is_complete = run_details_response['data']['is_complete']
            if not is_complete:
                print(
                    f'Run {run_id} is not complete. Waiting 30 seconds and trying again.')
                time.sleep(60)
        # Quick solution to prevent pulling logs at the same moment the job
        # completes.
        time.sleep(30)
        exit_code = check_run_status.determine_run_status(run_details_response)

        if download_logs:
            download_logs_artifacts.log_step_details(
                run_details_response, folder_name=base_folder_name)

        if download_artifacts:
            artifacts = download_logs_artifacts.get_artifact_details(
                account_id,
                run_id,
                headers,
                folder_name=f'{base_folder_name}/responses',
                file_name=f'artifacts_{run_id}_response.json')
            if download_logs_artifacts.artifacts_exist(artifacts):
                for index, artifact in enumerate(artifacts['data']):
                    print(
                        f"Downloading file {index+1} of {len(artifacts['data'])}")
                    download_logs_artifacts.download_artifact(
                        account_id, run_id, artifact, headers, folder_name=f'{base_folder_name}/artifacts')

        sys.exit(exit_code)


if __name__ == '__main__':
    main()
