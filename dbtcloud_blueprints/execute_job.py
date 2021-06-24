from httprequest_blueprints import execute_request, download_file

import argparse
import os
import json
import time
import platform
import pickle

# Handle import difference between local and github install
try:
    import check_run_status
    import download_logs_artifacts
except BaseException:
    from . import check_run_status
    from . import download_logs_artifacts


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
        '--check-status',
        dest='check_status',
        default='TRUE',
        choices={
            'TRUE',
            'FALSE'},
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
    print(f'Response stored at {file_name}')


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
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(
        job_run_response, combined_name)
    return job_run_response


def main():
    args = get_args()
    account_id = args.account_id
    job_id = args.job_id
    api_key = args.api_key
    download_artifacts = execute_request.convert_to_boolean(
        args.download_artifacts)
    download_logs = execute_request.convert_to_boolean(args.download_logs)
    check_status = execute_request.convert_to_boolean(args.check_status)
    bearer_string = f'Bearer {api_key}'
    headers = {'Authorization': bearer_string}

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/dbtcloud-blueprints/')

    job_run_response = execute_job(
        account_id,
        job_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'job_{job_id}_response.json')

    run_id = job_run_response['data']['id']
    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name, 'run_id.pickle')
    with open(pickle_file_name, 'wb') as f:
        pickle.dump(run_id, f)

    if check_status:
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
                time.sleep(30)
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
