from httprequest_blueprints import execute_request, download_file
import check_run_status
import argparse
import os
import json
import code


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


def log_step_details(run_details_response, folder_name):
    if run_details_response['data']['run_steps'] == []:
        print(
            f'No logs to download for run {run_details_response["data"]["id"]}')
    else:
        for step in run_details_response['data']['run_steps']:
            step_id = step['id']
            execute_request.create_folder_if_dne(f'{folder_name}/responses/')
            execute_request.create_folder_if_dne(f'{folder_name}/logs/')
            step_file_name = execute_request.combine_folder_and_file_name(
                f'{folder_name}/responses/', f'step_{step_id}_response.json')
            debug_log_name = execute_request.combine_folder_and_file_name(
                f'{folder_name}/logs/', 'dbt.log')
            output_log_name = execute_request.combine_folder_and_file_name(
                f'{folder_name}/logs/', 'dbt_console_output.txt')
            write_json_to_file(
                step, step_file_name)
            with open(debug_log_name, 'a') as f:
                f.write(step['debug_logs'])
            with open(output_log_name, 'a') as f:
                f.write(step['logs'])


def get_artifact_details(
        account_id,
        run_id,
        header,
        folder_name=f'dbt-blueprint-logs',
        file_name=f'artifacts_details_response.json'):
    get_artifact_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/'
    print(f'Grabbing artifact details for run {run_id}')
    artifact_details_req = execute_request.execute_request(
        'GET', get_artifact_details_url, header)
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
    return artifacts_exist


def download_artifact(
        account_id,
        run_id,
        artifact_name,
        header,
        folder_name=f'dbt-blueprint-logs/{os.environ.get("SHIPYARD_ORG_ID","orgid")}/{os.environ.get("SHIPYARD_LOG_ID","logid")}/artifacts'):
    get_artifact_details_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{artifact_name}'
    artifact_file_name = artifact_name.split('/')[-1]
    artifact_folder = artifact_name.replace(artifact_name.split('/')[-1], '')

    full_folder = execute_request.combine_folder_and_file_name(
        folder_name, artifact_folder)
    execute_request.create_folder_if_dne(full_folder)

    full_file_name = execute_request.combine_folder_and_file_name(
        full_folder, artifact_file_name)
    artifact_details_req = download_file.download_file(
        get_artifact_details_url, full_file_name, header)


def main():
    args = get_args()
    account_id = args.account_id
    run_id = args.run_id
    api_key = args.api_key
    bearer_string = f'Bearer {api_key}'
    header = {'Authorization': bearer_string}
    folder_name = f'dbt-blueprint-logs/{os.environ.get("SHIPYARD_ORG_ID","orgid")}/{os.environ.get("SHIPYARD_LOG_ID","logid")}'

    run_details_response = check_run_status.get_run_details(
        account_id, run_id, header, folder_name, file_name='run_{run_id}_response.json')

    log_step_details(run_details_response, folder_name)

    artifacts = get_artifact_details(
        account_id,
        run_id,
        header,
        folder_name=f'{folder_name}/artifacts',
        file_name=f'artifacts_{run_id}_response.json')
    if artifacts_exist(artifacts):
        for artifact in artifacts['data']:
            download_artifact(account_id, run_id, artifact, header)


if __name__ == '__main__':
    main()
