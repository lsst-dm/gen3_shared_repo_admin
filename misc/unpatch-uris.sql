-- This script fixes DC2 rerun URIs that were converted before we had logic
-- to expand out symlink in direct-ingest paths.  It should not be necessary
-- for new conversions after that has been fixed.

UPDATE
    file_datastore_records
SET
    path = REPLACE(path, 'patched/2021-02-10', 'v19.0.0-v1')
WHERE
    dataset_id IN (
        SELECT id FROM dataset WHERE run_name IN (
            '2.2i/runs/DP0.1/coadd/wfd/dr6/v1/u',
            '2.2i/runs/DP0.1/coadd/wfd/dr6/v1/grizy',
            '2.2i/runs/DP0.1/calexp/v1',
            '2.2i/runs/DP0.1/coadd/wfd/dr6/v1',
            'refcats/PREOPS-301'
        )
    )
;
