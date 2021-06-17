from httprequest_blueprints import execute_request, download_file
import check_run_status
import download_logs_artifacts
import argparse
import os
import json


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
        '--execute-only',
        dest='execute_only',
        default='FALSE',
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


def execute_job(
        account_id,
        job_id,
        header,
        folder_name='dbt-blueprint-logs',
        file_name='job_details_response.json'):
    execute_job_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/'
    body = {"cause": "Run from Postman"}
    print(f'Kicking off job {job_id} on account {account_id}')
    job_run_req = execute_request.execute_request(
        'POST', execute_job_url, header, body)
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
    execute_only = execute_request.convert_to_boolean(args.execute_only)
    bearer_string = f'Bearer {api_key}'
    header = {'Authorization': bearer_string}
    folder_name = f'dbt-blueprint-logs/{os.environ.get("SHIPYARD_ORG_ID","orgid")}/{os.environ.get("SHIPYARD_LOG_ID","logid")}'

    job_run_response = execute_job(
        account_id,
        job_id,
        header,
        folder_name=f'{folder_name}/responses',
        file_name=f'job_{job_id}_response.json')

    if not execute_only:
        run_id = job_run_response['data']['id']
        run_details_response = check_run_status.check_run_status(
            account_id,
            run_id,
            header,
            folder_name,
            file_name=f'run_{run_id}_response.json')

        if download_logs:
            download_logs_artifacts.log_step_details(
                run_details_response, folder_name)

        if download_artifacts:
            artifacts = download_logs_artifacts.get_artifact_details(
                account_id,
                run_id,
                header,
                folder_name=f'{folder_name}/artifacts',
                file_name=f'artifacts_{run_id}_response.json')
            if download_logs_artifacts.artifacts_exist(artifacts):
                for artifact in artifacts['data']:
                    download_logs_artifacts.download_artifact(
                        account_id, run_id, artifact, header)


if __name__ == '__main__':
    main()
