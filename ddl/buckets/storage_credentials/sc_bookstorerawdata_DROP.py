
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.storage_credentials.delete(name='sc_bookstorerawdata')
    logger.info(f"\nSuccess: DROP storage_credential 'sc_bookstorerawdata'")
except Exception as e:
    logger.critical(f"\nFailure: DROP storage_credential 'sc_bookstorerawdata': \n{e}")
