
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='https://dbc-134e30eb-a774.cloud.databricks.com/',token = 'dapi269ac03c813a54aa5c3ceb72e67a0c3b')
try:
    ws.external_locations.delete(name='el_bookstoredatabricks')
    logger.info(f"\nSuccess: DROP external_location 'el_bookstoredatabricks'")
except Exception as e:
    logger.critical(f"\nFailure: DROP external_location 'el_bookstoredatabricks': \n{e}")
