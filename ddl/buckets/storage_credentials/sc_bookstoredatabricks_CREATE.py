
ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.storage_credentials.create(
        name='sc_bookstoredatabricks',
        aws_iam_role=AwsIamRole(role_arn='arn:aws:iam::302263052839:role/databricks-s3-ingest-f00cb-db_s3_iam')
        )
    print(f"\nSuccess: CREATE storage_credential 'sc_bookstoredatabricks'")
except Exception as e:
    print(f"\nFailure: CREATE storage_credential 'sc_bookstoredatabricks': \n{e}")
