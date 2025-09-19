
ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.external_locations.create(
        name='el_bookstoredatabricks'
        ,url='s3://bookstoredatabricks/'
        ,credential_name='sc_bookstoredatabricks'
        )
    print(f"\nSuccess: CREATE external_location 'el_bookstoredatabricks': \n's3://bookstoredatabricks/'")
except Exception as e:
    print(f"\nFailure: CREATE external_location 'el_bookstoredatabricks': \n{e}")
