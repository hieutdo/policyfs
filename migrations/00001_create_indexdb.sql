-- +goose Up

CREATE TABLE IF NOT EXISTS files (
    storage_id          TEXT    NOT NULL,
    path                TEXT    NOT NULL,
    real_path           TEXT    NOT NULL DEFAULT '',
    parent_dir          TEXT    NOT NULL,
    name                TEXT    NOT NULL,
    is_dir              INTEGER NOT NULL DEFAULT 0,

    size                INTEGER,
    mtime               INTEGER NOT NULL,
    mode                INTEGER NOT NULL,
    uid                 INTEGER NOT NULL,
    gid                 INTEGER NOT NULL,

    deleted             INTEGER DEFAULT 0,
    last_seen_run_id    INTEGER,

    file_count          INTEGER NOT NULL DEFAULT 0,
    total_files         INTEGER NOT NULL DEFAULT 0,
    total_bytes         INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (storage_id, path)
);

CREATE INDEX IF NOT EXISTS idx_files_parent_live ON files (storage_id, parent_dir) WHERE deleted = 0;
CREATE INDEX IF NOT EXISTS idx_files_deleted ON files (storage_id, deleted) WHERE deleted = 1;
CREATE INDEX IF NOT EXISTS idx_files_run_id ON files (storage_id, last_seen_run_id);
CREATE INDEX IF NOT EXISTS idx_files_real_path_pending ON files (storage_id, real_path) WHERE real_path != path;

CREATE TABLE IF NOT EXISTS file_meta (
    storage_id   TEXT    NOT NULL,
    path         TEXT    NOT NULL,
    meta_mtime   INTEGER,
    meta_mode    INTEGER,
    meta_uid     INTEGER,
    meta_gid     INTEGER,

    PRIMARY KEY (storage_id, path),
    FOREIGN KEY (storage_id, path)
        REFERENCES files(storage_id, path)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS indexer_state (
    storage_id       TEXT PRIMARY KEY,
    current_run_id   INTEGER NOT NULL DEFAULT 0,
    last_completed   INTEGER,
    last_duration_ms INTEGER,
    file_count       INTEGER,
    total_bytes      INTEGER
);

-- +goose Down

DROP TABLE IF EXISTS file_meta;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS indexer_state;
