from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("Jupyter").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') 
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')
# spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') 
# spark.conf.set('spark.sql.sources.v2.bucketing.pushPartValues.enabled','true')
# spark.conf.set('spark.sql.requireAllClusterKeysForCoPartition','false')
# spark.conf.set('spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled','true')
#

# read data
# match_id pk
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv").select("match_id", "playlist_id", "mapid")
# match_id	player_gamertag pk
match_details = (
    spark.read.option("header", "true")
    .csv("/home/iceberg/data/match_details.csv")
    .select("match_id", "player_gamertag", "player_total_kills")
    .withColumn("player_total_kills", F.col("player_total_kills").cast("int"))
)
# match_id	medal_id	player_gamertag pk
medals_matches_players = (
    spark.read.option("header", "true")
    .csv("/home/iceberg/data/medals_matches_players.csv")
    .select("match_id", "medal_id", "count")
    .withColumn("count", F.col("count").cast("int"))
)
# medal_id pk
medals = (
    spark.read.option("header", "true")
    .csv("/home/iceberg/data/medals.csv")
    .select("medal_id", "description", "name")
    .filter(F.col("name") == "Killing Spree")
)
# mapid pk
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

# write bucketed data
# sortBy("match_id") gives an error "IllegalArgumentException: Cannot convert transform with more than one column reference: sorted_bucket(match_id, 16, match_id)"
matches.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("default.matches_hive")
match_details.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("default.match_details_hive")
medals_matches_players.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("default.medals_matches_players_hive")

# read bucketed data
matches_bucketed = spark.table("default.matches_hive")
match_details_bucketed = spark.table("default.match_details_hive")
medals_matches_players_bucketed = spark.table("default.medals_matches_players_hive")

# joined data
join_dataset = (
    matches_bucketed.alias("m")
    .join(match_details_bucketed.alias("md"), on="match_id", how="inner")
    .join(medals_matches_players_bucketed.alias("mmp"), on="match_id", how="inner")
    .select (
        "m.match_id"
        , "m.playlist_id"
        , "m.mapid"
        , "md.player_gamertag"
        , "md.player_total_kills"
        , "mmp.medal_id"
        , "mmp.count"
    )
)

join_dataset.explain("FORMATTED")

#Which player averages the most kills per game?
(
match_details_bucketed.groupBy("player_gamertag")
    .agg(
        F.avg("player_total_kills").alias("avg_kills")
    )
    .orderBy(F.desc('avg_kills'))
    .limit(1)
).show()

#Which playlist gets played the most?
(
matches_bucketed.groupBy("playlist_id")
    .agg(
        F.count("*").alias("num_played")
    )
    .orderBy(F.desc('num_played'))
    .limit(1)
).show(truncate=False)

#Which map gets played the most?
(
matches_bucketed.groupBy("mapid")
    .agg(
        F.count("*").alias("num_played")
    )
    .orderBy(F.desc('num_played'))
    .limit(1)
).show(truncate=False)

#Which map do players get the most Killing Spree medals on?
(
medals.filter("name = 'Killing Spree'")
    .join(medals_matches_players_bucketed, "medal_id")
    .join(matches_bucketed, "match_id")
    .groupBy("mapid")
    .agg(
        F.sum("count").alias("num_medals_per_map")
    )
    .orderBy(F.desc("num_medals_per_map"))
    .join(maps, 'mapid')
    .limit(1)
).show(truncate=False)


spark.sql("drop table if exists default.join_dataset_playlist_id")

spark.sql("""
CREATE TABLE default.join_dataset_playlist_id (
   match_id string
 ,player_gamertag string
 ,player_total_kills string
 ,playlist_id string
 ,mapid string
 ,medal_id string
  ) USING iceberg PARTITIONED BY (playlist_id)
""")

spark.sql("drop table if exists default.join_dataset_mapid")

spark.sql("""
CREATE TABLE default.join_dataset_mapid (
  match_id string
 ,player_gamertag string
 ,player_total_kills string
 ,playlist_id string
 ,mapid string
 ,medal_id string
  ) USING iceberg PARTITIONED BY (mapid)
""")

spark.sql("drop table if exists default.join_dataset_playlist_id_mapid")

spark.sql("""
CREATE TABLE default.join_dataset_playlist_id_mapid (
  match_id string
 ,player_gamertag string
 ,player_total_kills string
 ,playlist_id string
 ,mapid string
 ,medal_id string
  ) USING iceberg PARTITIONED BY (playlist_id, mapid)
""")


join_dataset_cached = join_dataset.select(
"match_id"
 ,"player_gamertag"
 ,"player_total_kills"
 ,"playlist_id"
 ,"mapid"
 ,"medal_id").persist()

join_dataset_cached.sortWithinPartitions("playlist_id").write.mode("append").saveAsTable("default.join_dataset_playlist_id")

join_dataset_cached.sortWithinPartitions("mapid").write.mode("append").saveAsTable("default.join_dataset_mapid")

join_dataset_cached.sortWithinPartitions("playlist_id", "mapid").write.mode("append").saveAsTable("default.join_dataset_playlist_id_mapid")

spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'playlist_id' 
FROM default.join_dataset_playlist_id.files
union all
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'mapid' 
FROM default.join_dataset_mapid.files
union all
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'both' 
FROM default.join_dataset_playlist_id_mapid.files
""").show()