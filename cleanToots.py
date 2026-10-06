from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, regexp_replace, length, window, avg, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

# Connexion PostgreSQL (conteneur Docker)
JDBC_URL = "jdbc:postgresql://localhost:5432/mastodon_db"
JDBC_PROPS = {
    "user": "mastodon",
    "password": "mastodon",
    "driver": "org.postgresql.Driver",
}
spark = (
    SparkSession.builder
    .appName("Mastodon_Streaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "org.postgresql:postgresql:42.7.7"
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 0. Lecture du topic Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "mastodon_stream")
    .option("startingOffsets", "earliest")
    .load()
)

# 1. Schéma des messages JSON
schema = StructType([
    StructField("id", StringType()),
    StructField("created_at", StringType()),
    StructField("username", StringType()),
    StructField("content", StringType()),
    StructField("language", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("favourites_count", IntegerType()),
    StructField("reblogs_count", IntegerType()),
])
# 2. Parsing : binaire -> texte -> colonnes
toots = (
    df.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# 3. Nettoyage : date en timestamp, suppression du HTML
toots = (
    toots
    .withColumn("created_at", to_timestamp(col("created_at")))
    .withColumn("content", regexp_replace(col("content"), "<[^>]+>", ""))
)

# Transformation 1 : filtre sur la langue + longueur du texte
toots_filtered = (
    toots
    .filter(col("language").isin("en", "fr"))
    .withColumn("content_length", length(col("content")))
)

# Transformation 2 + Action 1 : nombre de toots par fenêtre d'1 heure
toots_per_hour = (
    toots_filtered
    .groupBy(window(col("created_at"), "1 hour"))
    .agg(count("*").alias("nb_toots"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "nb_toots",
    )
)

# Action 2 : longueur moyenne des toots par utilisateur
avg_length_per_user = (
    toots_filtered
    .groupBy("username")
    .agg(
        avg("content_length").alias("avg_length"),
        count("*").alias("nb_toots"),
    )
)

# Écriture dans PostgreSQL : chaque micro-batch est écrit en mode batch
def write_append(table):
    def _write(batch_df, batch_id):
        batch_df.write.jdbc(JDBC_URL, table, mode="append", properties=JDBC_PROPS)
    return _write


def write_overwrite(table):
    def _write(batch_df, batch_id):
        (
            batch_df.write
            .option("truncate", "true")
            .jdbc(JDBC_URL, table, mode="overwrite", properties=JDBC_PROPS)
        )
    return _write


# Toots nettoyés : ajoutés au fur et à mesure (données historiques pour la partie 3)
q_toots = (
    toots_filtered.writeStream
    .foreachBatch(write_append("toots"))
    .option("checkpointLocation", "checkpoints/toots")
    .start()
)

# Agrégations : mode complete = résultat entier recalculé, la table est remplacée
q_per_hour = (
    toots_per_hour.writeStream
    .outputMode("complete")
    .foreachBatch(write_overwrite("toots_per_hour"))
    .option("checkpointLocation", "checkpoints/toots_per_hour")
    .start()
)

q_avg = (
    avg_length_per_user.writeStream
    .outputMode("complete")
    .foreachBatch(write_overwrite("avg_length_per_user"))
    .option("checkpointLocation", "checkpoints/avg_length_per_user")
    .start()
)

spark.streams.awaitAnyTermination()