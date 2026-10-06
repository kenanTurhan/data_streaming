import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, explode, lower, count, avg, row_number, lit
)
from pyspark.sql.window import Window

# Seuil "utilisateur actif" : un utilisateur est actif s'il a plus de X toots
X_MIN_TOOTS = 2

# Connexion PostgreSQL (conteneur Docker)
JDBC_URL = "jdbc:postgresql://localhost:5432/mastodon_db"
JDBC_PROPS = {
    "user": "mastodon",
    "password": "mastodon",
    "driver": "org.postgresql.Driver",
}

# Nombre de partitions utilisé par repartition() avant les groupBy
NB_PARTITIONS = 4


# ---------------------------------------------------------------------------
# Session et lecture
# ---------------------------------------------------------------------------
def create_spark():
    # Même configuration que le streaming : master local, driver JDBC chargé
    # via spark.jars.packages, 4 partitions de shuffle (inutile d'en avoir 200
    # sur un petit jeu de données en local).
    spark = (
        SparkSession.builder
        .appName("Mastodon_Batch")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.7")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_toots(spark):
    # Lecture JDBC : Spark ne lit RIEN ici (évaluation paresseuse), la requête
    # SQL n'est envoyée à PostgreSQL qu'au moment d'une action (count, write...).
    # dropDuplicates("id") : le streaming peut réécrire un toot deux fois
    # (reprise après un crash), l'id est la clé naturelle pour dédoublonner.
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "toots")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .load()
        .dropDuplicates(["id"])
    )


# ---------------------------------------------------------------------------
# Helpers d'optimisation (appliqués seulement en mode "optimisé")
# ---------------------------------------------------------------------------
def before_group(df, optimized, *cols):
    # repartition(n, cols) fait un shuffle COMPLET pour répartir les lignes
    # en n partitions de taille équilibrée, avec les mêmes clés au même endroit.
    # Avant un groupBy, cela évite un déséquilibre (une partition énorme,
    # les autres vides) et laisse tous les cœurs travailler en parallèle.
    if optimized:
        return df.repartition(NB_PARTITIONS, *cols)
    return df


def write_table(df, table, optimized):
    # coalesce(1) AVANT l'écriture JDBC : chaque partition ouvre sa propre
    # connexion PostgreSQL. Ces résultats sont minuscules (quelques lignes),
    # donc 1 seule partition = 1 seule connexion et 1 seule transaction au
    # lieu de plusieurs connexions presque vides.
    # coalesce (et non repartition) car il fusionne les partitions SANS shuffle.
    if optimized:
        df = df.coalesce(1)
    (
        df.write
        .option("truncate", "true")  # garde la table, vide juste son contenu
        .jdbc(JDBC_URL, table, mode="overwrite", properties=JDBC_PROPS)
    )


# ---------------------------------------------------------------------------
# Transformations / agrégations (chacune retourne un DataFrame)
# ---------------------------------------------------------------------------
def active_users(df, optimized):
    # Transformation : on garde les utilisateurs avec plus de X toots.
    # groupBy + count = nombre de toots par utilisateur, puis filter (HAVING).
    return (
        before_group(df, optimized, "username")
        .groupBy("username")
        .agg(count("*").alias("nb_toots"))
        .filter(col("nb_toots") > X_MIN_TOOTS)
    )


def hashtags_per_day(df, optimized):
    # explode transforme un tableau en plusieurs lignes : un toot avec 3
    # hashtags devient 3 lignes (une par hashtag). C'est indispensable pour
    # pouvoir faire un groupBy sur le hashtag lui-même.
    # lower() évite de compter "#Spark" et "#spark" comme deux hashtags.
    exploded = (
        df.select(
            to_date("created_at").alias("day"),
            explode("hashtags").alias("hashtag"),
        )
        .withColumn("hashtag", lower(col("hashtag")))
    )
    return (
        before_group(exploded, optimized, "day", "hashtag")
        .groupBy("day", "hashtag")
        .agg(count("*").alias("nb_toots"))
    )


def top_hashtags(per_day):
    # Hashtag le plus fréquent de chaque jour : row_number() numérote les
    # hashtags de chaque jour du plus au moins fréquent, on garde le rang 1.
    # (hashtag en second critère pour un résultat déterministe en cas d'égalité)
    w = Window.partitionBy("day").orderBy(col("nb_toots").desc(), "hashtag")
    return (
        per_day.withColumn("rang", row_number().over(w))
        .filter(col("rang") == 1)
        .drop("rang")
    )


def toots_per_day(df, optimized):
    return (
        before_group(df.withColumn("day", to_date("created_at")), optimized, "day")
        .groupBy("day")
        .agg(count("*").alias("nb_toots"))
    )


def avg_length(df, optimized):
    # Longueur moyenne par langue + une ligne globale ("ALL") empilée avec
    # union : une seule table contient les deux niveaux d'agrégation.
    by_lang = (
        before_group(df, optimized, "language")
        .groupBy("language")
        .agg(avg("content_length").alias("avg_length"))
    )
    overall = df.agg(avg("content_length").alias("avg_length")).select(
        lit("ALL").alias("language"), "avg_length"
    )
    return by_lang.unionByName(overall)


def engagement_per_user(df, optimized):
    # Métrique supplémentaire : engagement moyen = favoris + reblogs par toot,
    # moyenné par utilisateur (mesure la "popularité" de chaque auteur).
    return (
        before_group(df, optimized, "username")
        .groupBy("username")
        .agg(avg(col("favourites_count") + col("reblogs_count")).alias("avg_engagement"))
    )


# ---------------------------------------------------------------------------
# Exécution complète des agrégations
# ---------------------------------------------------------------------------
def build_results(df, optimized):
    # Construit les 6 DataFrames résultats (rien n'est exécuté ici : lazy).
    per_day = hashtags_per_day(df, optimized)
    return {
        "batch_active_users": active_users(df, optimized),
        "batch_hashtags_per_day": per_day,
        "batch_top_hashtags": top_hashtags(per_day),
        "batch_toots_per_day": toots_per_day(df, optimized),
        "batch_avg_length": avg_length(df, optimized),
        "batch_engagement_per_user": engagement_per_user(df, optimized),
    }


def run_aggregations(df, optimized):
    # Chaque write_table est une ACTION : elle déclenche un job Spark complet.
    # Sans cache, chacun des 6 jobs relit toute la table depuis PostgreSQL.
    # Avec cache, la source est lue une fois puis servie depuis la mémoire.
    results = build_results(df, optimized)
    for table, result in results.items():
        write_table(result, table, optimized)
    return results


def timed(label, fn):
    start = time.perf_counter()
    out = fn()
    elapsed = time.perf_counter() - start
    print(f"[{label}] {elapsed:.2f} s")
    return out, elapsed


def show_results(results):
    for table, result in results.items():
        print(f"\n=== {table} ===")
        result.show(20, truncate=False)


def print_comparison(t_before, t_cache, t_after):
    print("\n" + "=" * 52)
    print(f"{'Scénario':<34}{'Temps (s)':>12}")
    print("-" * 52)
    print(f"{'Sans optimisation':<34}{t_before:>12.2f}")
    print(f"{'Avec optimisation (agrégations)':<34}{t_after:>12.2f}")
    print(f"{'  + matérialisation du cache':<34}{t_cache:>12.2f}")
    print(f"{'Avec optimisation (total)':<34}{t_after + t_cache:>12.2f}")
    print("=" * 52)
    print("Le cache coûte une lecture initiale, rentabilisée dès que plusieurs "
          "agrégations réutilisent la source.")


def print_partitions(df):
    # getNumPartitions() n'exécute rien : il lit juste le plan.
    print("\n--- Nombre de partitions ---")
    print(f"Source (lecture JDBC)      : {df.rdd.getNumPartitions()}")
    repart = df.repartition(NB_PARTITIONS, "username")
    print(f"Après repartition({NB_PARTITIONS})       : {repart.rdd.getNumPartitions()}")
    print(f"Après coalesce(1)          : {repart.coalesce(1).rdd.getNumPartitions()}")


# ---------------------------------------------------------------------------
def main():
    spark = create_spark()

    # --- 1. SANS optimisation : la source n'est pas en cache ---
    df_plain = load_toots(spark)
    print("\n##### Agrégations SANS optimisation #####")
    _, t_before = timed("sans optimisation", lambda: run_aggregations(df_plain, False))

    # --- 2. AVEC optimisation ---
    # Nouvelle lecture pour repartir d'un DataFrame vierge (non mis en cache).
    df_opt = load_toots(spark)
    print("\n##### Agrégations AVEC optimisation #####")

    def materialize_cache():
        # cache() est paresseux : il marque seulement le DataFrame. Le count()
        # est l'action qui force la lecture et remplit le cache en mémoire.
        df_opt.cache()
        return df_opt.count()

    nb, t_cache = timed("matérialisation du cache", materialize_cache)
    print(f"Toots dédoublonnés en cache : {nb}")

    results, t_after = timed("avec optimisation", lambda: run_aggregations(df_opt, True))

    # --- 3. Affichage ---
    show_results(results)
    print_comparison(t_before, t_cache, t_after)
    print_partitions(df_opt)

    input("Spark UI sur http://localhost:4040 - Entrée pour quitter")
    spark.stop()


if __name__ == "__main__":
    main()
