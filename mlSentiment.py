from dotenv import load_dotenv
load_dotenv()


import os

# --- CORRECTION DU BUG WINDOWS / JAVA ---
if 'JAVA_HOME' in os.environ:
    del os.environ['JAVA_HOME']

# 1. Définir le chemin vers le dossier hadoop
hadoop_path = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = hadoop_path

# 2. Ajouter le dossier bin au PATH de Windows pour qu'il trouve hadoop.dll
os.environ['PATH'] = os.path.join(hadoop_path, 'bin') + os.pathsep + os.environ.get('PATH', '')


from pyspark.sql.functions import col, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml import Pipeline


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1. Création de la session Spark
spark = SparkSession.builder \
    .appName("Mastodon_Sentiment_Analysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Définition des colonnes du dataset Sentiment140
schema = StructType([
    StructField("target", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("user", StringType(), True),
    StructField("text", StringType(), True)
])

# 3. Chargement des données d'entraînement
chemin_csv = "data/training.csv"
df_kaggle = spark.read.csv(chemin_csv, schema=schema)

# 4. Affichage des 5 premières lignes pour vérifier que ça marche
df_kaggle.show(5)

# 5. Nettoyage basique du texte (suppression des URL et caractères spéciaux)
df_clean = df_kaggle.withColumn("text_clean", regexp_replace(col("text"), r"http\S+", ""))
df_clean = df_clean.withColumn("text_clean", regexp_replace(col("text_clean"), r"[^a-zA-Z\s]", ""))

# 6. Configuration des étapes de transformation (Pipeline ML)
tokenizer = Tokenizer(inputCol="text_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="features", vocabSize=10000, minDF=5)

# 7. Création et exécution du pipeline de préparation
print("Préparation des données en cours (cela peut prendre un peu de temps)...")
pipeline = Pipeline(stages=[tokenizer, remover, vectorizer])
model_prep = pipeline.fit(df_clean)
df_prepared = model_prep.transform(df_clean)

# 8. Affichage du résultat avec les vecteurs
df_prepared.select("target", "filtered_words", "features").show(5)

# 9. Ajustement de la cible : Sentiment140 utilise 0 (négatif) et 4 (positif). On passe le 4 en 1.
df_prepared = df_prepared.withColumn("label", (col("target") / 4).cast("double"))

# 10. Séparation des données (80% pour l'entraînement, 20% pour vérifier la précision)
train_data, test_data = df_prepared.randomSplit([0.8, 0.2], seed=42)

# 11. Entraînement du modèle de Machine Learning
print("Entraînement du modèle en cours (cela peut prendre quelques minutes, patience !)...")
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
model = lr.fit(train_data)

# 12. Évaluation du modèle sur les 20% de données de test
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Précision du modèle : {accuracy * 100:.2f}%")

# 13. Sauvegarde du modèle pour l'utiliser plus tard en temps réel sur Kafka
model.write().overwrite().save("modele_sentiment")
print("Modèle sauvegardé avec succès dans le dossier 'modele_sentiment' !")


# --- APPLICATION DU MODÈLE SUR LES DONNÉES MASTODON ---

# 14. Configuration de la base de données (en dur pour garantir la connexion)
db_user = "mastodon"
db_password = "mastodon"
db_name = "mastodon_db"

# Utilisation de 127.0.0.1 au lieu de localhost pour corriger le routage Windows
db_url = f"jdbc:postgresql://127.0.0.1:5433/{db_name}"

db_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# 15. Charger la table 'toots' depuis PostgreSQL
print("Chargement des toots depuis la base de données...")
df_toots = spark.read.jdbc(url=db_url, table="toots", properties=db_properties)

# 16. Nettoyage du texte (on suppose que la colonne contenant le texte s'appelle 'content')
df_toots_clean = df_toots.withColumn("text_clean", regexp_replace(col("content"), r"http\S+", ""))
df_toots_clean = df_toots_clean.withColumn("text_clean", regexp_replace(col("text_clean"), r"[^a-zA-Z\s]", ""))

# 17. Préparation et Prédiction avec le modèle entraîné juste au-dessus
print("Application du modèle sur vos toots...")
df_toots_prepared = model_prep.transform(df_toots_clean)
predictions_toots = model.transform(df_toots_prepared)

# 18. Sauvegarde dans la nouvelle table 'toots_sentiments'
print("Sauvegarde des prédictions dans PostgreSQL...")
result_df = predictions_toots.select("id", "content", "prediction")
result_df.write.jdbc(url=db_url, table="toots_sentiments", mode="overwrite", properties=db_properties)

print("Partie 4 totalement terminée ! La table toots_sentiments est prête.")