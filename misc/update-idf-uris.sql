-- Rule for raws.  These start with the usual, unpatched path in the NCSA
-- database, but they go somewhere special at the IDF.
UPDATE
    file_datastore_records
SET
    path = REPLACE(path, 'file:///datasets/DC2/DR6/Run2.2i/v19.0.0-v1', 's3://curation-us-central1-desc-dc2-run22i')
WHERE
    dataset_id IN (
        SELECT id FROM dataset WHERE run_name IN (
            '2.2i/raw/all'
        )
    )
;

-- Rule for reruns and refcats: these start with the usual, unpatched path in
-- the NCSA database, and go in the main IDF location.
UPDATE
    file_datastore_records
SET
    path = REPLACE(path, 'file:///datasets/DC2/DR6/Run2.2i/v19.0.0-v1', 's3://butler-us-central1-dp01-desc-dr6')
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

-- Rule for calibs: these start with a different (patched) path in the NCSA
-- database, but go in the main IDF location.
UPDATE
    file_datastore_records
SET
    path = REPLACE(path, 'file:///datasets/DC2/DR6/Run2.2i/patched/2021-02-10', 's3://butler-us-central1-dp01-desc-dr6')
WHERE
    dataset_id IN (
        SELECT id FROM dataset WHERE run_name IN (
            '2.2i/calib/gen2/20220806T000000Z',
            '2.2i/calib/gen2/20220101T000000Z'
        )
    )
;

-- Rule for skyMap and curated calibrations (i.e. camera).  These have relative
-- paths in the NCSA database because they're directly written rather than
-- ingested from some existing file, and we convert that into an absolute path
-- at the IDF.
UPDATE
    file_datastore_records
SET
    path = CONCAT('s3://butler-us-central1-dp01-desc-dr6/', path)
WHERE
    dataset_id IN (
        SELECT id FROM dataset WHERE run_name IN (
            'skymaps',
            '2.2i/calib/PREOPS-301/unbounded'
        )
    )
;
