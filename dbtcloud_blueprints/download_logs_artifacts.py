from httprequest_blueprints import execute_request, download_file
import argparse
import os
import json
import pickle
import sys

# Handle import difference between local and github install
try:
    import check_run_status
except BaseException:
    from . import check_run_status

EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_INVALID_RESOURCE = 202
EXIT_CODE_FINAL_STATUS_ERRORED = 204
EXIT_CODE_FINAL_STATUS_CANCELLED = 205
EXIT_CODE_STATUS_INCOMPLETE = 206


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--account-id', dest='account_id', required=True)
    parser.add_argument('--run-id', dest='run_id', required=False)
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


def log_step_details(run_details_response, folder_name):
    if run_details_response['data']['run_steps'] == []:
        print(
            f'No logs to download for run {run_details_response["data"]["id"]}')
    else:
        execute_request.create_folder_if_dne(f'{folder_name}/responses/')
        execute_request.create_folder_if_dne(f'{folder_name}/logs/')
        debug_log_name = execute_request.combine_folder_and_file_name(
            f'{folder_name}/logs/', 'dbt.log')
        output_log_name = execute_request.combine_folder_and_file_name(
            f'{folder_name}/logs/', 'dbt_console_output.txt')
        number_of_steps = len(run_details_response['data']['run_steps'])
        for index, step in enumerate(
                run_details_response['data']['run_steps']):
            step_id = step['id']
            print(
                f'Grabbing step details for step {step_id} ({index+1} of {number_of_steps})')
            step_file_name = execute_request.combine_folder_and_file_name(
                f'{folder_name}/responses/', f'step_{step_id}_response.json')

            write_json_to_file(
                step, step_file_name)

            with open(debug_log_name, 'a') as f:
                f.write(step['debug_logs'])

            with open(output_log_name, 'a') as f:
                f.write(step['logs'])
        print(f'Successfully wrote logs to {output_log_name}')
        print(f'Successfully wrote debug_logs to {debug_log_name}')


def get_artifact_details(
        account_id,
        run_id,
        headers,
        folder_name,
        file_name=f'artifacts_details_response.json'):
    get_artifact_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/'
    print(f'Grabbing artifact details for run {run_id}')
    artifact_details_req = execute_request.execute_request(
        'GET', get_artifact_details_url, headers)
    artifact_details_response = json.loads(artifact_details_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(
        artifact_details_response,
        combined_name)
    return artifact_details_response


def artifacts_exist(artifact_details_response):
    if artifact_details_response['data'] is None:
        artifacts_exist = False
        print(
            'No artifacts exist for this run.')
    else:
        artifacts_exist = True
        print(
            f"There are {len(artifact_details_response['data'])} artifacts to download.")
    return artifacts_exist


def download_artifact(
        account_id,
        run_id,
        artifact_name,
        headers,
        folder_name):
    get_artifact_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{artifact_name}'
    artifact_file_name = artifact_name.split('/')[-1]
    artifact_folder = artifact_name.replace(artifact_name.split('/')[-1], '')

    full_folder = execute_request.combine_folder_and_file_name(
        folder_name, artifact_folder)
    execute_request.create_folder_if_dne(full_folder)

    full_file_name = execute_request.combine_folder_and_file_name(
        full_folder, artifact_file_name)
    try:
        artifact_details_req = download_file.download_file(
            get_artifact_details_url, full_file_name, headers)
    except BaseException:
        print('Failed to download file {get_artifact_details_url}')


def determine_connection_status(run_details_response):
    status_code = run_details_response['status']['code']
    user_message = run_details_response['status']['user_message']
    if status_code == 401:
        if 'Invalid token' in user_message:
            print('The API Key provided was invalid. Check to make sure there are no typos or preceding/trailing spaces.')
            print(user_message)
            sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
        else:
            print(
                f'An unknown error occurred with a status code of {status_code}')
            print(user_message)
            sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    if status_code == 404:
        if 'requested resource not found':
            print('The Account ID, Job ID, or Run ID provided was either invalid or your API Key doesn\'t have access to it. Check to make sure there are no typos or preceding/trailing spaces.')
            print(user_message)
            sys.exit(EXIT_CODE_INVALID_RESOURCE)
        else:
            print(
                f'An unknown error occurred with a status code of {status_code}')
            print(user_message)
            sys.exit(EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    account_id = args.account_id
    run_id = args.run_id
    api_key = args.api_key
    download_artifacts = execute_request.convert_to_boolean(
        args.download_artifacts)
    download_logs = execute_request.convert_to_boolean(args.download_logs)
    bearer_string = f'Bearer {api_key}'
    headers = {'Authorization': bearer_string}

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/dbtcloud-blueprints/')

    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name, 'run_id.pickle')

    if args.run_id:
        run_id = args.run_id
    else:
        with open(pickle_file_name, 'rb') as f:
            run_id = pickle.load(f)

    run_details_response = check_run_status.get_run_details(
        account_id,
        run_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'run_{run_id}_response.json')
    determine_connection_status(run_details_response)

    if download_logs:
        log_step_details(run_details_response, folder_name=base_folder_name)

    if download_artifacts:
        artifacts = get_artifact_details(
            account_id,
            run_id,
            headers,
            folder_name=f'{base_folder_name}/responses',
            file_name=f'artifacts_{run_id}_response.json')
        if artifacts_exist(artifacts):
            for index, artifact in enumerate(artifacts['data']):
                print(
                    f"Downloading file {index+1} of {len(artifacts['data'])}")
                download_artifact(
                    account_id,
                    run_id,
                    artifact,
                    headers,
                    folder_name=f'{base_folder_name}/artifacts')


if __name__ == '__main__':
    main()
