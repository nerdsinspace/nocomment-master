CREATE TABLE servers
(
    id       SERIAL PRIMARY KEY,
    hostname TEXT NOT NULL UNIQUE
);

CREATE TABLE dimensions
(
    ordinal INTEGER PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE
);

INSERT INTO dimensions (ordinal, name)
VALUES (-1, 'Nether'),
       (0, 'Overworld'),
       (1, 'End');

CREATE TABLE players
(
    id       SERIAL PRIMARY KEY,
    uuid     UUID NOT NULL UNIQUE,
    username TEXT
);

CREATE EXTENSION btree_gist;
CREATE TABLE player_sessions
(
    player_id INTEGER NOT NULL,
    server_id INTEGER NOT NULL,
    "join"    BIGINT  NOT NULL,
    "leave"   BIGINT,
    range     INT8RANGE GENERATED ALWAYS AS (
                  CASE
                      WHEN "leave" IS NULL THEN
                          INT8RANGE("join", ~(1::BIGINT << 63), '[)')
                      ELSE
                          INT8RANGE("join", "leave", '[]')
                      END
                  ) STORED,
    legacy    BOOLEAN NOT NULL DEFAULT FALSE,
    EXCLUDE USING GiST (server_id WITH =, player_id WITH =, range WITH &&),
    FOREIGN KEY (player_id) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX player_sessions_range ON player_sessions USING GiST (server_id, range);
CREATE INDEX player_sessions_by_leave ON player_sessions (server_id, player_id, UPPER(range));

CREATE TABLE hits
(
    id         BIGSERIAL PRIMARY KEY, -- this could VERY PLAUSIBLY hit 2^31. downright likely
    created_at BIGINT  NOT NULL,
    x          INTEGER NOT NULL,
    z          INTEGER NOT NULL,
    dimension  INTEGER NOT NULL,
    server_id  INTEGER NOT NULL,
    legacy     BOOLEAN NOT NULL DEFAULT FALSE,
    -- NOTE: AN EXTRA COLUMN IS ADDED LATER. can't be added here due to cyclic references

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX hits_time_and_place ON hits (server_id, created_at, dimension); -- yes, this ordering is intentional, no, dimension shouldn't be before created_at

CREATE TABLE tracks
(
    id            BIGSERIAL PRIMARY KEY, -- this hitting 2^32 is but a faint possibility, but might as well make it a long
    first_hit_id  BIGINT  NOT NULL,
    last_hit_id   BIGINT  NOT NULL,      -- most recent
    updated_at    BIGINT  NOT NULL,      -- this is a duplicate of the created_at in last_hit_id for indexing purposes
    prev_track_id BIGINT,                -- for example, if this is an overworld track from the nether when we lost them, this would be the track id in the nether that ended
    dimension     INTEGER NOT NULL,
    server_id     INTEGER NOT NULL,
    legacy        BOOLEAN NOT NULL DEFAULT FALSE,

    FOREIGN KEY (first_hit_id) REFERENCES hits (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (last_hit_id) REFERENCES hits (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (prev_track_id) REFERENCES tracks (id)
        ON UPDATE CASCADE ON DELETE SET NULL,
    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE hits
    ADD COLUMN
        track_id BIGINT -- nullable
            REFERENCES tracks (id)
                ON UPDATE CASCADE ON DELETE SET NULL;

CREATE INDEX hits_tracks ON hits (track_id);

CREATE INDEX track_endings ON tracks (server_id, updated_at); -- to query what tracks ended in a server at a particular time
ALTER TABLE tracks
    CLUSTER ON track_endings;
CLUSTER tracks;

CREATE TABLE dbscan
(
    id             SERIAL PRIMARY KEY,
    cnt            INTEGER NOT NULL,
    x              INTEGER NOT NULL,
    z              INTEGER NOT NULL,
    dimension      INTEGER NOT NULL,
    server_id      INTEGER NOT NULL,
    needs_update   BOOLEAN NOT NULL,
    is_core        BOOLEAN NOT NULL,
    cluster_parent INTEGER, -- nullable
    disjoint_rank  INTEGER NOT NULL,

    FOREIGN KEY (cluster_parent) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE SET NULL,
    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX dbscan_cluster_roots ON dbscan (server_id, dimension, id) WHERE cluster_parent IS NULL AND is_core;
CREATE UNIQUE INDEX dbscan_ingest ON dbscan (server_id, dimension, x, z);
CREATE INDEX dbscan_process ON dbscan USING GiST (server_id, dimension, CIRCLE(POINT(x, z), 32)) WHERE cnt > 3;
CREATE INDEX dbscan_to_update ON dbscan (needs_update) WHERE needs_update;
CREATE INDEX dbscan_disjoint_traversal ON dbscan (cluster_parent) WHERE cluster_parent IS NOT NULL;

CREATE TABLE dbscan_progress
(
    last_processed_hit_id BIGINT NOT NULL
);

INSERT INTO dbscan_progress(last_processed_hit_id)
VALUES (0);
