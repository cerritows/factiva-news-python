from factiva import APIKeyUser
from factiva.news.snapshot import Snapshot, SnapshotQuery

# [MB] This file needs to test mainly constructor outputs. Specific
# operations are tested in separate files.

# [MB] TODO: This requires to use a valid USER_KEY
aku = APIKeyUser('abcd1234abcd1234abcd1234abcd1234')

# [MB] USER-CENTRIC tests

# [MB] TODO: Add this test
# s = Snapshot(query="publication_datetime >= '2021-01-01'")
# Assert an initialised Snapshot using the USERKEY from the ENV variables and with no info.
# Also assert the query is created from the string.

# [MB] TODO: Add this test
# s = Snapshot(query="publication_datetime >= '2021-01-01'", api_user='abcd1234abcd1234abcd1234abcd1234')
# Assert an initialised Snapshot using the provided api_user and with no info.
# Also assert the query is created from the string.

# [MB] TODO: Add this test
# s = Snapshot(query="publication_datetime >= '2021-01-01'", request_userinfo=True)
# Assert an initialised Snapshot using the USERKEY from the ENV variables and with account info.
# Also assert the query is created from the string.

# [MB] TODO: Add this test
# s = Snapshot(query="publication_datetime >= '2021-01-01'", api_user='abcd1234abcd1234abcd1234abcd1234', request_userinfo=True)
# Assert an initialised Snapshot using the provided api_user and with account info.
# Also assert the query is created from the string.

# [MB] TODO: Add this test
# s = Snapshot(query="publication_datetime >= '2021-01-01'", api_user=aku)
# Assert an initialised Snapshot using the provided api_user object
# Also assert the query is created from the string.


# [MB] QUERY-CENTRIC tests

# [MB] Test variables initialisation
# where_statement = "publication_datetime >= '2021-01-01'"
# q = SnapshotQuery(where_statement)

# [MB] TODO: Add this test
# s = Snapshot(query=q)
# Assert an initialised Snapshot using the USERKEY from the ENV variables and with no info.
# Also assert the query is created from the provided object

# More Query-Specific tests in the file snapshotquery.py



# [MB] EXISTING-SNAPSHOT

# [MB] TODO: Add this test. Need to identify a valid ID for the test account.
# s = Snapshot(snapshot_id='tthb9cxch9')
# Assert the Snapshot object is created and the extraction_job object is fetched from the API


# [MB] EXCEPTION TEST
# Add tests for Snapshot-relevant raised exceptions.
