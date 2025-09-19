
ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.storage_credentials.delete(name='sc_bookstoredatabricks')
    print(f"\nSuccess: DROP storage_credential 'sc_bookstoredatabricks'")
except Exception as e:
    print(f"\nFailure: DROP storage_credential 'sc_bookstoredatabricks': \n{e}")
