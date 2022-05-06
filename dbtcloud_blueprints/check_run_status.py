from httprequest_blueprints import execute_request
import argparse
import os
import json
import sys
import pickle

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


def get_run_details(
        account_id,
        run_id,
        headers,
        folder_name,
        file_name=f'run_details_response.json'):
    get_run_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/'
    params = {"include_related": "['run_steps','debug_logs']"}
    print(f'Grabbing run details for run {run_id}.')
    run_details_req = execute_request.execute_request(
        'GET', get_run_details_url, headers=headers, params=params)
    run_details_response = json.loads(run_details_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(run_details_response, combined_name)
    return run_details_response


def determine_run_status(run_details_response):
    run_id = run_details_response['data']['id']
    if run_details_response['data']['is_complete']:
        if run_details_response['data']['is_error']:
            print(f'dbt Cloud reports that the run {run_id} errored.')
            exit_code = EXIT_CODE_FINAL_STATUS_ERRORED
        elif run_details_response['data']['is_cancelled']:
            print(f'dbt Cloud reports that run {run_id} was cancelled.')
            exit_code = EXIT_CODE_FINAL_STATUS_CANCELLED
        else:
            print(f'dbt Cloud reports that run {run_id} was successful.')
            exit_code = EXIT_CODE_FINAL_STATUS_SUCCESS
    else:
        print(f'dbt Cloud reports that the run {run_id} is not yet completed.')
        exit_code = EXIT_CODE_STATUS_INCOMPLETE
    return exit_code


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


def main():
    args = get_args()
    account_id = args.account_id
    api_key = args.api_key
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

    run_details_response = get_run_details(
        account_id,
        run_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'run_{run_id}_response.json')
    determine_connection_status(run_details_response)
    sys.exit(determine_run_status(run_details_response))


if __name__ == '__main__':
    main()
