--A query that does state change tracking for players:
--    A player entering the league should be 'New'
--    A player leaving the league should be 'Retired'
--    A player staying in the league should be 'Continued Playing'
--    A player that comes out of retirement should be 'Returned from Retirement'
--    A player that stays out of the league should be 'Stayed Retired'


WITH SEASONS AS (
-- GET ALL SEASONS
SELECT DISTINCT
    SEASON
FROM
    PLAYER_SEASONS
), PLAYERS AS (
-- GET ALL PLAYERS WITH THE FIRST AND LAST ACTIVE SEASON
SELECT
    PLAYER_NAME
    , MIN(SEASON) AS MIN_SEASON
FROM
    PLAYER_SEASONS
GROUP BY
	PLAYER_NAME
), ALL_PLAYER_SEASON_PAIRS AS (
-- GET ALL PLAYER SEASON POSSIBLE COMBINATIONS
SELECT
    P.PLAYER_NAME
    , S.SEASON
FROM
    PLAYERS AS P
    INNER JOIN
    SEASONS S
    ON S.SEASON >= P.MIN_SEASON
), STATUS_FLAGS AS (
-- MARK WHETHER THEY ACTUALLY PLAYED IN THAT SEASON
SELECT
    APSP.PLAYER_NAME
    , APSP.SEASON
    , CASE
        WHEN PS.PLAYER_NAME IS NOT NULL THEN 1
        ELSE 0
    END AS PLAYED
FROM
    ALL_PLAYER_SEASON_PAIRS AS APSP
    LEFT JOIN
    PLAYER_SEASONS AS PS
    ON (
        APSP.PLAYER_NAME = PS.PLAYER_NAME
        AND APSP.SEASON = PS.SEASON
    )
), WITH_PREV AS (
SELECT
    PLAYER_NAME
    , SEASON
    , PLAYED
    , LAG(PLAYED) OVER (
        PARTITION BY
            PLAYER_NAME
        ORDER BY
            SEASON -- INTEGER
    ) AS PREV_PLAYED
FROM
    STATUS_FLAGS
)
SELECT
    WP.PLAYER_NAME
    , WP.SEASON
    , WP.PLAYED
    , WP.PREV_PLAYED
    , CASE
        WHEN WP.PLAYED = 1 AND WP.PREV_PLAYED IS NULL THEN 'New' -- IF THE PLAYER PLAYED THIS SEASON AND THE SEASON EQUALS IS FIRST SEASON THEN NEW
        WHEN WP.PLAYED = 0 AND WP.PREV_PLAYED = 1 THEN 'Retired'
        WHEN WP.PLAYED = 1 AND WP.PREV_PLAYED = 1 THEN 'Continued Playing'
        WHEN WP.PLAYED = 1 AND WP.PREV_PLAYED = 0 THEN 'Returned from Retirement'
        WHEN WP.PLAYED = 0 AND WP.PREV_PLAYED = 0 THEN 'Stayed Retired'
    END AS STATUS_CHANGE
FROM
    WITH_PREV AS WP
;