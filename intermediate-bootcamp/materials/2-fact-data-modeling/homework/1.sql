-- SELECT
--     GAME_ID
--     , TEAM_ID
--     , PLAYER_ID
--     , COUNT(1)
-- FROM (
--     SELECT DISTINCT -- EVEN WITH DISTINCT RETURNS RESULTS; APPLYING ROW_NUMBER WITHOUT ORDER BY MEANS THE ROW_NUMBER RESULTS MAY VARY
--     PLUS_MINUS
--     , TEAM_ID
--     , AST
--     , STL
--     , BLK
--     , "TO"
--     , PF
--     , PTS
--     , GAME_ID
--     , PLAYER_ID
--     , FGM
--     , FGA
--     , FG_PCT
--     , FG3M
--     , FG3A
--     , FG3_PCT
--     , FTM
--     , FTA
--     , FT_PCT
--     , OREB
--     , DREB
--     , REB
--     , TEAM_ABBREVIATION
--     , TEAM_CITY
--     , MIN
--     , PLAYER_NAME
--     , NICKNAME
--     , START_POSITION
--     , COMMENT
--     FROM
--         GAME_DETAILS
-- ) AS TMP
-- GROUP BY 1,2,3
-- HAVING COUNT(1) > 1;
WITH DEDUPLICATED_GAME_DETAILS AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                GAME_ID
                , TEAM_ID
                , PLAYER_ID
        ) AS RN
        , *
    FROM
        GAME_DETAILS
)
SELECT
    *
FROM
    DEDUPLICATED_GAME_DETAILS
WHERE
    1 = 1
    AND RN = 1;