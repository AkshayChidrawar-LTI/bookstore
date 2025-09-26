
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole

ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.storage_credentials.create(
        name='sc_bookstorerawdata',
        aws_iam_role=AwsIamRole(role_arn='arn:aws:iam::302263052839:role/databricks-s3-ingest-3fcda-db_s3_iam')
        )
    logger.info(f"\nSuccess: CREATE storage_credential 'sc_bookstorerawdata'")
except Exception as e:
    logger.critical(f"\nFailure: CREATE storage_credential 'sc_bookstorerawdata': \n{e}")
