
ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.external_locations.delete(name='el_bookstorerawdata')
    print(f"\nSuccess: DROP external_location 'el_bookstorerawdata'")
except Exception as e:
    print(f"\nFailure: DROP external_location 'el_bookstorerawdata': \n{e}")
