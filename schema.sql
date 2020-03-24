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
VALUES (-1, 'Nether')
     , (0, 'Overworld')
     , (1, 'End');

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
    EXCLUDE USING GiST (server_id WITH =, player_id WITH =, range WITH &&),
    FOREIGN KEY (player_id) REFERENCES players (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX player_sessions_range ON player_sessions USING GiST (server_id, range);

CREATE TABLE hits
(
    id         BIGSERIAL PRIMARY KEY,
    created_at BIGINT  NOT NULL,
    x          INTEGER NOT NULL,
    z          INTEGER NOT NULL,
    dimension  INTEGER NOT NULL,
    server_id  INTEGER NOT NULL,

    FOREIGN KEY (server_id) REFERENCES servers (id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (dimension) REFERENCES dimensions (ordinal)
        ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX hits_worlds ON hits (server_id, dimension, created_at);

