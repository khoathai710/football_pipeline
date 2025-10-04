CREATE SCHEMA gold;

CREATE TABLE gold.goals_assists(
    player_id      TEXT,
    first_name     VARCHAR(100),
    player_last_name VARCHAR(100),
    club           VARCHAR(100),
    position       VARCHAR(50),
    nation         VARCHAR(100),
    img_url        TEXT,
    match_id       TEXT,
    goal_minute    VARCHAR(100),
    category            INT
);

CREATE TABLE gold.cards (
    player_id      TEXT,
    first_name     VARCHAR(100),
    player_last_name VARCHAR(100),
    club           VARCHAR(100),
    position       VARCHAR(50),
    nation         VARCHAR(100),
    img_url        TEXT,
    match_id       TEXT,
    minute    VARCHAR(10),
    category           INT
);
