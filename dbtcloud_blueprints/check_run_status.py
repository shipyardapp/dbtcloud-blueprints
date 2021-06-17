from httprequest_blueprints import execute_request
import argparse
import os
import json
import time
import sys


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--account-id', dest='account_id', required=True)
    parser.add_argument('--run-id', dest='run_id', required=True)
    args = parser.parse_args()
    return args


def write_json_to_file(json_object, file_name):
    with open(file_name, 'w') as f:
        f.write(
            json.dumps(
                json_object,
                ensure_ascii=False,
                indent=4))


def get_run_details(
        account_id,
        run_id,
        header,
        folder_name,
        file_name=f'run_details_response.json'):
    get_run_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/?include_related=[\'run_steps\',\'debug_logs\']'
    print(f'Grabbing run details for run {run_id}.')
    run_details_req = execute_request.execute_request(
        'GET', get_run_details_url, header)
    run_details_response = json.loads(run_details_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(run_details_response, combined_name)
    return run_details_response


def determine_run_status(run_details_response):
    run_id = run_details_response['data']['id']
    if run_details_response['data']['is_error']:
        sys.exit(f'dbt Cloud reports that the run {run_id} errored.')
    if run_details_response['data']['is_cancelled']:
        sys.exit(f'dbt Cloud reports that run {run_id} was cancelled.')
    else:
        print(f'dbt Cloud reports that run {run_id} was successful.')


def main():
    args = get_args()
    account_id = args.account_id
    run_id = args.run_id
    api_key = args.api_key
    bearer_string = f'Bearer {api_key}'
    header = {'Authorization': bearer_string}

    org_id = os.environ.get("SHIPYARD_ORG_ID") if os.environ.get(
        'USER') == 'shipyard' else account_id
    log_id = os.environ.get("SHIPYARD_LOG_ID") if os.environ.get(
        'USER') == 'shipyard' else run_id
    base_folder_name = f'dbt-blueprint-logs/{org_id}/{log_id}'

    run_details_response = get_run_details(
        account_id,
        run_id,
        header,
        folder_name=base_folder_name,
        file_name='run_{run_id}_response.json')
    determine_run_status(run_details_response)


if __name__ == '__main__':
    main()
