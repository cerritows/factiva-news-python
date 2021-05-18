from factiva.news.snapshot.jobs import UpdateJob

# test_snapshot = Snapshot(snapshot_id='xuvmdvbgqs')
# test_snapshot.process_update(update_type='replacements')
# print(test_snapshot)

test_update = UpdateJob(update_id='xuvmdvbgqs-deletes-20210513060214')
test_update.download_job_files()
print(test_update)
print('Done!')