
ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.storage_credentials.create(
        name='sc_bookstorerawdata',
        aws_iam_role=AwsIamRole(role_arn='arn:aws:iam::302263052839:role/databricks-s3-ingest-3fcda-db_s3_iam')
        )
    print(f"\nstorage credential 'sc_bookstorerawdata' is created.")
except Exception as e:
    print(f"\nException occured while creating storage credential 'sc_bookstorerawdata': \n{e}")
