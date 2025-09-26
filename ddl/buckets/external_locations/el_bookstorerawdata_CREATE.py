
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.external_locations.create(
        name='el_bookstorerawdata'
        ,url='s3://bookstorerawdata/'
        ,credential_name='sc_bookstorerawdata'
        )
    logger.info(f"\nSuccess: CREATE external_location 'el_bookstorerawdata': \n's3://bookstorerawdata/'")
except Exception as e:
    logger.critical(f"\nFailure: CREATE external_location 'el_bookstorerawdata': \n{e}")
