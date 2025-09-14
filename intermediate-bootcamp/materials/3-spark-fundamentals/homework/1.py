from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName("Jupyter").getOrCreate()
# 

# Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Explicitly broadcast JOINs
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")

print("medals", medals.count())
print("medals_matches_players", medals_matches_players.count())
print("maps", maps.count())
print("matches", matches.count())

medals_matches_players_join_medals = medals_matches_players.join(broadcast(medals), on="medal_id", how="inner")

medals_matches_players_join_medals.explain("FORMATTED")

matches_join_maps = matches.join(broadcast(maps), on="mapid", how="left")

matches_join_maps.explain("FORMATTED")