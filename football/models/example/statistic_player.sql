
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH goals_assists_player AS (
    SELECT 
        ga.player_id,
        ga.first_name,
        ga.player_last_name,
        ga.club,
        ga.position,
        ga.nation,
        ga.img_url AS link_img_player,
        COUNT(CASE WHEN ga.category = 1 THEN 1 END) AS goals,
        COUNT(CASE WHEN ga.category = 0 THEN 1 END) AS assists
    FROM gold.goals_assists ga
    WHERE ga.first_name IS NOT NULL 
       OR ga.player_last_name IS NOT NULL
    GROUP BY ga.player_id, ga.first_name, ga.player_last_name, 
             ga.club, ga.position, ga.nation, ga.img_url
),
cards_player AS (
    SELECT 
        ga.player_id,
        ga.first_name,
        ga.player_last_name,
        ga.club,
        ga.position,
        ga.nation,
        ga.img_url AS link_img_player,
        COUNT(CASE WHEN ga.category = 1 THEN 1 END) AS yellow,
        COUNT(CASE WHEN ga.category = 0 THEN 1 END) AS red
    FROM gold.cards ga
    WHERE ga.first_name IS NOT NULL 
       OR ga.player_last_name IS NOT NULL
    GROUP BY ga.player_id, ga.first_name, ga.player_last_name, 
             ga.club, ga.position, ga.nation, ga.img_url
)
SELECT 
    COALESCE(g.player_id, c.player_id) AS player_id,
    COALESCE(g.first_name, c.first_name) AS first_name,
    COALESCE(g.player_last_name, c.player_last_name) AS player_last_name,
    COALESCE(g.club, c.club) AS club,
    COALESCE(g.position, c.position) AS position,
    COALESCE(g.nation, c.nation) AS nation,
    COALESCE(g.link_img_player, c.link_img_player) AS link_img_player,

    COALESCE(g.goals, 0)   AS goals,
    COALESCE(g.assists, 0) AS assists,
    COALESCE(c.yellow, 0)  AS yellow_cards,
    COALESCE(c.red, 0)     AS red_cards
FROM goals_assists_player g
FULL OUTER JOIN cards_player c
    ON g.player_id = c.player_id
   AND (g.first_name = c.first_name OR g.player_last_name = c.player_last_name)

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
