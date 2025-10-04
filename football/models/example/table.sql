{{ config(materialized='table') }}
with table_score as (
    WITH split_score AS (
    SELECT
        match_id,
        home_name,
        away_name,
        home_score,
        away_score
    FROM gold.match_score
    ),
    home_stats AS (
        SELECT
            home_name AS team,
            COUNT(*) AS played,
            SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS won,
            SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) AS drawn,
            SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) AS lost,
            SUM(home_score) AS gs,
            SUM(away_score) AS ga
        FROM split_score
        GROUP BY home_name
    ),
    away_stats AS (
        SELECT
            away_name AS team,
            COUNT(*) AS played,
            SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) AS won,
            SUM(CASE WHEN away_score = home_score THEN 1 ELSE 0 END) AS drawn,
            SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) AS lost,
            SUM(away_score) AS gs,
            SUM(home_score) AS ga
        FROM split_score
        GROUP BY away_name
    ),
    all_stats AS (
        SELECT * FROM home_stats
        UNION ALL
        SELECT * FROM away_stats
    )
    SELECT
        team,
        SUM(played) AS played,
        SUM(won) AS won,
        SUM(drawn) AS drawn,
        SUM(lost) AS lost,
        SUM(gs) AS gs,
        SUM(ga) AS ga,
        SUM(gs) - SUM(ga) AS gd,
        SUM(won) * 3 + SUM(drawn) AS points
    FROM all_stats
    GROUP BY team
    ORDER BY points DESC, gd DESC, gs desc
    
),
table_full_info AS(
	SELECT 
		tc.team,
		tc.played,
		tc.won,
		tc.drawn,
		tc.lost,
		tc.gs,
		tc.ga,
		tc.gd,
		tc.points,
		t.stadium,
		t.capacity,
		t.location,
		t.link,
		t.est
	FROM table_score tc
	left join gold.team_info t on t.name = tc.team
)
select * from table_full_info