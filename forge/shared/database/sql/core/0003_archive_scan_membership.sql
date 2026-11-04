-- A selection (including a base coin or family) owns a versioned search result.
-- Membership avoids guessing concrete scopes from current instrument listings.
CREATE TABLE historical.discovery_scan_archives (
    scan_id BIGINT NOT NULL REFERENCES historical.discovery_scans(id) ON DELETE CASCADE,
    remote_archive_id BIGINT NOT NULL REFERENCES historical.remote_archives(id),
    PRIMARY KEY (scan_id, remote_archive_id)
);
CREATE INDEX discovery_scan_archives_remote_idx
    ON historical.discovery_scan_archives(remote_archive_id);
-- Legacy scans did not record their observations; rediscover before reusing them.
UPDATE historical.discovery_scans SET status = 'partial' WHERE status = 'complete';
