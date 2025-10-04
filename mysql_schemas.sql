DROP TABLE IF EXISTS football_stadiums;
CREATE TABLE football_stadiums (
    team_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    stadium VARCHAR(100) NOT NULL,
    capacity INT,
    image_link TEXT,
    location TEXT
);

