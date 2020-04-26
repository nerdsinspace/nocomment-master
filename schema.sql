CREATE TABLE servers
(
    id       SMALLINT PRIMARY KEY, -- NOTE: this is serial, but there is no datatype for SMALLINT SERIAL. so i "made my own" by creating a serial then altering its type to smallint lol
    hostname TEXT NOT NULL UNIQUE
);

CREATE TABLE dimensions
(
    ordinal SMALLINT PRIMARY KEY,
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
    player_id INTEGER  NOT NULL,
    server_id SMALLINT NOT NULL,
    "join"    BIGINT   NOT NULL,
    "leave"   BIGINT,
    range     INT8RANGE GENERATED ALWAYS AS (
                  CASE
                      WHEN "leave" IS NULL THEN
                          INT8RANGE("join", ~(1::BIGINT << 63), '[)')
                      ELSE
                          INT8RANGE("join", "leave", '[]')
                      END
                  ) STORED,
    legacy    BOOLEAN  NOT NULL DEFAULT FALSE,

    -- there USED to be a really based

    -- EXCLUDE USING GiST (server_id WITH =, player_id WITH =, range WITH &&),

    -- right here, but sadly it takes WAY TOO LONG to generate and keep up to date
    -- talking over an hour to recreate it after I dropped it :(
    -- also the server guarantees this anyway so
    -- also it took multiple gigabytes of disk

    FOREIGN KEY (player_id) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX player_sessions_range ON player_sessions USING GiST (range);
CREATE INDEX player_sessions_by_leave ON player_sessions (server_id, player_id, UPPER(range));

CREATE TABLE hits
(
    id         BIGSERIAL PRIMARY KEY, -- this could VERY PLAUSIBLY hit 2^31. downright likely
    created_at BIGINT   NOT NULL,
    x          INTEGER  NOT NULL,
    z          INTEGER  NOT NULL,
    dimension  SMALLINT NOT NULL,
    server_id  SMALLINT NOT NULL,
    legacy     BOOLEAN  NOT NULL DEFAULT FALSE,
    -- NOTE: AN EXTRA COLUMN IS ADDED LATER. can't be added here due to cyclic references

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX hits_loc_interesting ON hits (x, z) WHERE ABS(x) > 100 AND ABS(z) > 100 AND ABS(ABS(x) - ABS(z)) > 100 AND
                                                       x::bigint * x::bigint + z::bigint * z::bigint > 1000 * 1000;

CREATE TABLE last_by_server
(
    server_id  SMALLINT PRIMARY KEY,
    created_at BIGINT NOT NULL,

    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION new_hit() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    --INSERT INTO last_by_server (server_id, created_at)
    --VALUES (NEW.server_id, NEW.created_at)
    --ON CONFLICT ON CONSTRAINT last_by_server_pkey
    --    DO UPDATE SET created_at = excluded.created_at
    --WHERE excluded.created_at > last_by_server.created_at;
    UPDATE last_by_server SET created_at = NEW.created_at WHERE created_at < NEW.created_at AND server_id = NEW.server_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER new_hit_trigger
    AFTER INSERT OR UPDATE OR DELETE
    ON hits
    FOR EACH ROW
EXECUTE PROCEDURE new_hit();

CREATE TABLE tracks
(
    id            SERIAL PRIMARY KEY,
    first_hit_id  BIGINT   NOT NULL,
    last_hit_id   BIGINT   NOT NULL, -- most recent
    updated_at    BIGINT   NOT NULL, -- this is a duplicate of the created_at in last_hit_id for indexing purposes
    prev_track_id INTEGER,           -- for example, if this is an overworld track from the nether when we lost them, this would be the track id in the nether that ended
    dimension     SMALLINT NOT NULL,
    server_id     SMALLINT NOT NULL,
    legacy        BOOLEAN  NOT NULL DEFAULT FALSE,

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

CREATE INDEX tracks_legacy ON tracks (last_hit_id) WHERE legacy;

ALTER TABLE hits
    ADD COLUMN
        track_id INTEGER -- nullable
            REFERENCES tracks (id)
                ON UPDATE CASCADE ON DELETE SET NULL;

CREATE INDEX hits_by_track_id ON hits (track_id) WHERE track_id IS NOT NULL;

CREATE INDEX track_endings ON tracks (updated_at);
ALTER TABLE tracks
    CLUSTER ON track_endings;
CLUSTER tracks;

CREATE TABLE dbscan
(
    id             SERIAL PRIMARY KEY,
    cnt            INTEGER  NOT NULL,
    x              INTEGER  NOT NULL,
    z              INTEGER  NOT NULL,
    dimension      SMALLINT NOT NULL,
    server_id      SMALLINT NOT NULL,
    is_core        BOOLEAN  NOT NULL,
    cluster_parent INTEGER, -- nullable
    disjoint_rank  INTEGER  NOT NULL,
    disjoint_size  INTEGER  NOT NULL,

    FOREIGN KEY (cluster_parent) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE SET NULL,
    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX dbscan_cluster_roots ON dbscan (server_id, dimension, id) WHERE cluster_parent IS NULL AND disjoint_rank > 0;
CREATE UNIQUE INDEX dbscan_ingest ON dbscan (server_id, dimension, x, z);
CREATE INDEX dbscan_process ON dbscan USING GiST (server_id, dimension, CIRCLE(POINT(x, z), 32)) WHERE cnt > 3;
CREATE INDEX dbscan_disjoint_traversal ON dbscan (cluster_parent) WHERE cluster_parent IS NOT NULL;

CREATE TABLE dbscan_progress
(
    last_processed_hit_id BIGINT NOT NULL
);

INSERT INTO dbscan_progress(last_processed_hit_id)
VALUES (0);

CREATE TABLE dbscan_to_update
(
    dbscan_id INTEGER PRIMARY KEY,

    FOREIGN KEY (dbscan_id) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE associations
(
    cluster_id  INTEGER          NOT NULL,
    player_id   INTEGER          NOT NULL,
    association DOUBLE PRECISION NOT NULL,

    UNIQUE (cluster_id, player_id),

    FOREIGN KEY (player_id) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (cluster_id) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX associations_player_id ON associations (player_id);
CREATE INDEX associations_cluster_id ON associations (cluster_id);

CREATE TABLE track_associator_progress
(
    max_updated_at_processed BIGINT NOT NULL
);

INSERT INTO track_associator_progress
VALUES (0);