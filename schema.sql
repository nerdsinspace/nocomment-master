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

CREATE INDEX players_by_username
    ON players (username);

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
                          INT8RANGE("join", ~(1:: BIGINT << 63), '[)')
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
CREATE INDEX player_sessions_range
    ON player_sessions
        USING GiST (server_id, range);
CREATE INDEX player_sessions_by_leave
    ON player_sessions (server_id, player_id, UPPER(range));

CREATE TABLE hits
(
    id         BIGSERIAL PRIMARY KEY, -- this could VERY PLAUSIBLY hit 2^31. downright likely
    created_at BIGINT   NOT NULL,
    x          INTEGER  NOT NULL,
    z          INTEGER  NOT NULL,
    dimension  SMALLINT NOT NULL,
    server_id  SMALLINT NOT NULL,
    legacy     BOOLEAN  NOT NULL DEFAULT FALSE,
    -- NOTE: TRACK_ID COLUMN IS ADDED LATER. can't be added here due to cyclic references

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX hits_loc_interesting
    ON hits (x, z) WHERE ABS(x) > 100 AND ABS(z) > 100 AND ABS(ABS(x) - ABS(z)) > 100 AND
                         x :: bigint * x :: bigint + z :: bigint * z :: bigint > 1000 * 1000;

CREATE TABLE last_by_server
(
    server_id  SMALLINT PRIMARY KEY,
    created_at BIGINT NOT NULL,

    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION new_hit()
    RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    --INSERT INTO last_by_server (server_id, created_at)
    --VALUES (NEW.server_id, NEW.created_at)
    --ON CONFLICT ON CONSTRAINT last_by_server_pkey
    --    DO UPDATE SET created_at = excluded.created_at
    --WHERE excluded.created_at > last_by_server.created_at;
    UPDATE last_by_server
    SET created_at = NEW.created_at
    WHERE created_at < NEW.created_at
      AND server_id = NEW.server_id;
    RETURN NEW;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER new_hit_trigger
    AFTER INSERT OR UPDATE OR DELETE
    ON hits
    FOR EACH ROW
EXECUTE PROCEDURE new_hit();

CREATE OR REPLACE FUNCTION new_server()
    RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    INSERT INTO last_by_server (server_id, created_at)
    VALUES (NEW.server_id, 0)
    ON CONFLICT ON CONSTRAINT last_by_server_pkey DO NOTHING;
    RETURN NEW;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER new_server_trigger
    AFTER INSERT OR UPDATE OR DELETE
    ON servers
    FOR EACH ROW
EXECUTE PROCEDURE new_server();

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

CREATE INDEX tracks_legacy
    ON tracks (last_hit_id) WHERE legacy;

ALTER TABLE hits
    ADD COLUMN
        track_id INTEGER -- nullable
            REFERENCES tracks (id)
                ON UPDATE CASCADE ON DELETE SET NULL;

CREATE INDEX hits_by_track_id
    ON hits (track_id) WHERE track_id IS NOT NULL;

CREATE INDEX track_endings
    ON tracks (updated_at);
ALTER TABLE tracks
    CLUSTER ON track_endings;
CLUSTER tracks;

CREATE TABLE dbscan
(
    id              SERIAL PRIMARY KEY,

    x               INTEGER     NOT NULL,
    z               INTEGER     NOT NULL,
    dimension       SMALLINT    NOT NULL,
    server_id       SMALLINT    NOT NULL,

    is_node         BOOLEAN     NOT NULL,
    is_core         BOOLEAN     NOT NULL,
    cluster_parent  INTEGER,              -- nullable
    disjoint_rank   INTEGER     NOT NULL, -- rank based combination of disjoint set
    disjoint_size   INTEGER     NOT NULL, -- used in traversal

    root_updated_at BIGINT,               -- only updated for parent nodes, ideally the cluster core

    ts_ranges       INT8RANGE[] NOT NULL,
    last_init_hit   BIGINT      NOT NULL, -- contains most recent hit, potentially beyond the end of ts_ranges
    first_init_hit  BIGINT      NOT NULL, -- will only be used for analytics / reporting, not used by dbscan


    FOREIGN KEY (cluster_parent) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE SET NULL,
    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX dbscan_new_cluster_roots
    ON dbscan (id) WHERE cluster_parent IS NULL AND disjoint_rank > 0 AND root_updated_at IS NULL;
CREATE INDEX dbscan_cluster_roots
    ON dbscan (server_id, dimension, id) WHERE cluster_parent IS NULL AND disjoint_rank > 0;
CREATE UNIQUE INDEX dbscan_ingest
    ON dbscan (server_id, dimension, x, z);
CREATE INDEX dbscan_process
    ON dbscan
        USING GiST (CIRCLE(POINT(x, z), 32)) WHERE is_node;
CREATE INDEX dbscan_disjoint_traversal
    ON dbscan (cluster_parent, disjoint_rank, id) WHERE cluster_parent IS NOT NULL;

CREATE OR REPLACE FUNCTION _range_union_cardinality(int8range[])
    RETURNS bigint AS
$$

DECLARE
    _range    int8range;
    _current  int8range;
    _duration bigint;

BEGIN

    _current := (SELECT x FROM unnest($1) x ORDER BY x LIMIT 1);
    _duration := 0::bigint;

    FOR _range IN SELECT unnest($1) x ORDER BY x
        LOOP
            IF _range && _current OR _range -|- _current THEN
                _current := _current + _range;
            ELSE
                _duration := _duration + UPPER(_current) - LOWER(_current);
                _current := _range;
            END IF;
        END LOOP;

    RETURN _duration + UPPER(_current) - LOWER(_current);
END;

$$ LANGUAGE plpgsql;

CREATE AGGREGATE range_union_cardinality (int8range) (
    stype = int8range[],
    sfunc = array_append,
    finalfunc = _range_union_cardinality
    );

CREATE TABLE dbscan_progress
(
    last_processed_hit_id BIGINT NOT NULL
);
INSERT INTO dbscan_progress(last_processed_hit_id)
VALUES (0);

CREATE TABLE dbscan_to_update
(
    dbscan_id       INTEGER PRIMARY KEY,
    updatable_lower BIGINT NOT NULL,
    updatable_upper BIGINT NOT NULL,

    FOREIGN KEY (dbscan_id) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX dbscan_to_update_by_schedule ON dbscan_to_update (LEAST(updatable_lower, updatable_upper));

CREATE TABLE associations
(
    cluster_id  INTEGER          NOT NULL,
    player_id   INTEGER          NOT NULL,
    association DOUBLE PRECISION NOT NULL,
    created_at  BIGINT           NOT NULL,

    FOREIGN KEY (player_id) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (cluster_id) REFERENCES dbscan (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX associations_player_id
    ON associations (player_id, created_at);
CREATE INDEX associations_cluster_id
    ON associations (cluster_id, created_at);
CREATE INDEX associations_player_and_cluster
    ON associations (player_id, cluster_id);

CREATE VIEW assoc AS
(
SELECT players.username, tmp.cluster_id, tmp.association
FROM (SELECT cluster_id, player_id, SUM(association) AS association
      FROM associations
      GROUP BY player_id, cluster_id) tmp
         INNER JOIN players ON players.id = tmp.player_id);

CREATE TABLE track_associator_progress
(
    max_updated_at_processed BIGINT NOT NULL
);

CREATE TABLE blocks
(
    x           INTEGER  NOT NULL,
    y           SMALLINT NOT NULL,
    z           INTEGER  NOT NULL,
    block_state INTEGER  NOT NULL,
    created_at  BIGINT   NOT NULL,
    dimension   SMALLINT NOT NULL,
    server_id   SMALLINT NOT NULL,

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX blocks_by_loc
    ON blocks (x, z);

CREATE INDEX blocks_by_time
    ON blocks (created_at);

CREATE INDEX blocks_by_chunk
    ON blocks ((x >> 4), (z >> 4));

CREATE TYPE statuses_enum AS ENUM ('OFFLINE', 'QUEUE', 'ONLINE');

CREATE UNLOGGED TABLE statuses
(
    player_id   INTEGER       NOT NULL,
    curr_status statuses_enum NOT NULL,
    updated_at  BIGINT        NOT NULL,
    data        TEXT, -- any additional data, such as queue position
    dimension   SMALLINT      NOT NULL,
    server_id   SMALLINT      NOT NULL,

    UNIQUE (player_id, server_id),

    FOREIGN KEY (player_id) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE notes
(
    x          INTEGER  NOT NULL,
    z          INTEGER  NOT NULL,
    data       TEXT     NOT NULL,
    created_at BIGINT   NOT NULL,
    updated_at BIGINT   NOT NULL,
    dimension  SMALLINT NOT NULL,
    server_id  SMALLINT NOT NULL,

    UNIQUE (server_id, dimension, x, z),

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE signs
(
    x          INTEGER  NOT NULL,
    y          SMALLINT NOT NULL,
    z          INTEGER  NOT NULL,
    nbt        BYTEA    NOT NULL,
    created_at BIGINT   NOT NULL,
    dimension  SMALLINT NOT NULL,
    server_id  SMALLINT NOT NULL,

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX signs_by_loc
    ON signs (x, z);

CREATE TABLE chat
(
    data        JSON     NOT NULL,
    chat_type   SMALLINT NOT NULL, -- SPacketChat: enum ChatType CHAT((byte)0), SYSTEM((byte)1), GAME_INFO((byte)2);
    reported_by INTEGER  NOT NULL,
    created_at  BIGINT   NOT NULL,
    server_id   SMALLINT NOT NULL,

    FOREIGN KEY (reported_by) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX chat_by_time
    ON chat (server_id, created_at);
CREATE INDEX chat_by_time2
    ON chat (created_at);

CREATE TABLE generator_cache
(
    data      BYTEA    NOT NULL,
    x         INTEGER  NOT NULL,
    z         INTEGER  NOT NULL,
    dimension SMALLINT NOT NULL,
    server_id SMALLINT NOT NULL,

    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE UNIQUE INDEX generator_cache_by_loc ON generator_cache (server_id, dimension, x, z);

CREATE TABLE chat_progress
(
    max_created_at_processed BIGINT NOT NULL
);
INSERT INTO chat_progress(max_created_at_processed)
VALUES (0);

-- chat that we understand, but isn't worth making a whole table for. e.g. "player left the game" isn't useful because we have player_sessions e.g. "bad command type /help" is useless data
CREATE TABLE chat_miscellaneous
(
    data        JSONB    NOT NULL,
    kind        TEXT     NOT NULL,
    extracted   TEXT     NOT NULL,
    chat_type   SMALLINT NOT NULL,
    reported_by INTEGER  NOT NULL,
    created_at  BIGINT   NOT NULL,
    server_id   SMALLINT NOT NULL,

    FOREIGN KEY (reported_by) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX chat_miscellaneous_by_time
    ON chat_miscellaneous (created_at);

CREATE TABLE chat_player_message
(
    message     TEXT     NOT NULL,
    author      TEXT     NOT NULL,
    created_at  BIGINT   NOT NULL,
    server_id   SMALLINT NOT NULL,
    message_vec TSVECTOR NOT NULL GENERATED ALWAYS AS (TO_TSVECTOR('english', message)) STORED,

    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX chat_player_message_by_time
    ON chat_player_message (created_at);
CREATE INDEX chat_player_message_by_message
    ON chat_player_message USING GIN (message_vec);
CREATE INDEX chat_player_message_by_author
    ON chat_player_message (author, created_at);

CREATE TABLE chat_server_message
(
    message    TEXT     NOT NULL,
    created_at BIGINT   NOT NULL,
    server_id  SMALLINT NOT NULL,

    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX chat_server_message_by_time
    ON chat_server_message (created_at);

CREATE TABLE chat_whisper
(
    message      TEXT     NOT NULL,
    other_player TEXT     NOT NULL,
    outgoing     BOOLEAN  NOT NULL,
    reported_by  INTEGER  NOT NULL,
    created_at   BIGINT   NOT NULL,
    server_id    SMALLINT NOT NULL,

    FOREIGN KEY (reported_by) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX chat_whisper_by_time
    ON chat_whisper (created_at);

CREATE TABLE chat_death
(
    template   TEXT     NOT NULL,
    player_1   TEXT     NOT NULL,
    player_2   TEXT,
    created_at BIGINT   NOT NULL,
    server_id  SMALLINT NOT NULL,

    CHECK (player_2 IS NULL OR LENGTH(player_2) > 0), -- disallow non-null empty string

    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX chat_death_by_time
    ON chat_death (created_at);
CREATE INDEX chat_death_by_template
    ON chat_death (template, created_at);

CREATE TABLE death_templates
(
    template        TEXT    NOT NULL,
    killer_is_first BOOLEAN NOT NULL DEFAULT TRUE
);

-- insert them all into death_templates

CREATE VIEW deaths AS
SELECT template,
       CASE WHEN killer_is_first THEN player_2 ELSE player_1 END                                        AS died,
       CASE WHEN killer_is_first IS NULL THEN NULL WHEN killer_is_first THEN player_1 ELSE player_2 END AS killed_by,
       created_at
FROM chat_death
         LEFT OUTER JOIN death_templates USING (template);
