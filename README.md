# Pipeline de streaming Mastodon

Ce dossier contient le pipeline de collecte, de traitement et d'analyse de
toots Mastodon. Les données sont collectées depuis `mastodon.social`, publiées
dans Kafka, transformées par Apache Spark et persistées dans PostgreSQL.
Un traitement batch produit ensuite plusieurs indicateurs et un script séparé
entraîne un modèle de sentiment.

## Architecture

```text
Mastodon (hashtag #AI)
        |
        v
getToots.py  -- Kafka topic: mastodon_stream -->  batchToots.py
        |                                             |
        v                                             v
   Kafka (9092)                              PostgreSQL (mastodon_db)
        |                                             ^
        v                                             |
cleanToots.py  -- streaming Spark/JDBC --------------+
        |
        +--> toots
        +--> toots_per_hour
        +--> avg_length_per_user

mlSentiment.py
        |
        +--> training.csv (Sentiment140)
        +--> modele_sentiment/
        +--> toots_sentiments (PostgreSQL)
```

Le projet est prévu pour une exécution locale sous Windows. Spark utilise
`local[*]`, Kafka est lancé en mode KRaft avec un seul broker et PostgreSQL est
fourni par Docker Compose.

## Contenu du dossier

| Fichier ou dossier | Rôle |
| --- | --- |
| `docker-compose.yaml` | Démarre Kafka et PostgreSQL |
| `getToots.py` | Lit le hashtag `AI` sur Mastodon et publie les toots dans Kafka |
| `cleanToots.py` | Consomme Kafka, nettoie les messages et écrit les résultats Spark dans PostgreSQL |
| `batchToots.py` | Calcule les agrégations batch et compare des variantes d'optimisation |
| `mlSentiment.py` | Entraîne un modèle de sentiment puis le lance sur la table `toots` |
| `hadoop/bin/` | Contient `winutils.exe` et `hadoop.dll`, nécessaires à Spark sous Windows |
| `checkpoints/` | Checkpoints des requêtes Spark Structured Streaming |
| `modele_sentiment/` | Modèle Spark ML généré par `mlSentiment.py` |

Les répertoires `.venv/`, `checkpoints/` et `modele_sentiment/` sont des
artefacts d'exécution. Ils peuvent être régénérés et ne doivent pas être
confondus avec du code source.

## Prérequis

- Windows 10 ou 11 ;
- Python 3.12 recommandé ;
- Java installé et disponible dans le `PATH` (Spark 4.x nécessite une JVM
  compatible) ;
- Docker Desktop démarré ;
- un compte Mastodon et un jeton d'accès autorisant la lecture de la timeline ;
- suffisamment de mémoire pour Spark, Kafka et PostgreSQL.

Les dépendances Python utilisées par les scripts sont :

- `pyspark` ;
- `python-dotenv` ;
- `Mastodon.py` ;
- `kafka-python`.

Les drivers Java JDBC sont récupérés par Spark au démarrage :

- PostgreSQL `org.postgresql:postgresql:42.7.7` pour le streaming et le batch ;
- PostgreSQL `org.postgresql:postgresql:42.6.0` pour le script de sentiment ;
- connecteur Kafka Spark SQL `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`.

## Installation

Depuis PowerShell, se placer dans ce dossier :

```powershell
cd D:\5SPAR\data_streaming
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install pyspark python-dotenv Mastodon.py kafka-python
```

Si PowerShell bloque l'activation de l'environnement virtuel, exécuter une
fois PowerShell en tant qu'utilisateur courant :

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Le dossier `hadoop` est déjà prévu dans le projet. Les scripts le recherchent
relativement au répertoire courant ; il faut donc les lancer depuis
`data_streaming`, et non depuis un autre dossier.

## Configuration

Créer ou compléter `.env` avec un jeton local :

```dotenv
access_token=<JETON_MASTODON>
POSTGRES_USER=mastodon
POSTGRES_PASSWORD=mastodon
POSTGRES_DB=mastodon_db
```

Ne jamais publier un vrai jeton dans Git ou dans cette documentation. Le
fichier `.env` existant contient un secret : s'il a été partagé ou commité,
révoquer ce jeton dans Mastodon et en générer un nouveau.

### Ports utilisés

| Service | Adresse depuis Windows | Adresse entre conteneurs |
| --- | --- | --- |
| Kafka | `localhost:9092` | `kafka:29092` |
| PostgreSQL | `127.0.0.1:5433` | `db:5432` |
| Spark UI | `http://localhost:4040` | — |

Les scripts `cleanToots.py` et `batchToots.py` utilisent bien le port hôte
`5433`. `mlSentiment.py` utilise actuellement `127.0.0.1:5432` : si ce script
est lancé avec le `docker-compose.yaml` fourni, modifier cette valeur en
`5433` avant l'exécution, ou adapter le mapping de ports Docker.

Les identifiants PostgreSQL sont actuellement définis directement dans
`cleanToots.py`, `batchToots.py` et `mlSentiment.py`. Les variables `.env`
servent surtout au jeton Mastodon dans le code actuel ; elles ne remplacent
pas encore ces constantes.

## Démarrage des services

Lancer Kafka et PostgreSQL :

```powershell
docker compose up -d
docker compose ps
```

Vérifier que les conteneurs `kafka` et `postgres-dev` sont en état `running`.
Pour arrêter les services :

```powershell
docker compose down
```

Pour supprimer également les données persistées de PostgreSQL, uniquement si
cela est souhaité :

```powershell
docker compose down -v
```

## Exécution du pipeline temps réel

Ouvrir trois terminaux PowerShell, activer `.venv` dans chacun et exécuter les
commandes depuis `D:\5SPAR\data_streaming`.

### 1. Producteur Mastodon

```powershell
python .\getToots.py
```

Le script vérifie le compte Mastodon, interroge périodiquement le hashtag
`AI`, puis publie des messages JSON dans le topic `mastodon_stream`. Il
fonctionne en continu et attend cinq secondes entre deux lectures.

### 2. Consommateur Spark Structured Streaming

```powershell
python .\cleanToots.py
```

Le script :

1. consomme le topic Kafka depuis le début (`startingOffsets=earliest`) ;
2. désérialise les messages JSON ;
3. convertit `created_at` en timestamp et retire le HTML du contenu ;
4. conserve les langues `fr` et `en` ;
5. ajoute la longueur du contenu ;
6. écrit les toots dans `toots` en mode append ;
7. met à jour `toots_per_hour` et `avg_length_per_user` en mode complete ;
8. conserve l'état des requêtes dans `checkpoints/`.

Le script reste actif jusqu'à son interruption. Pour un redémarrage cohérent,
conserver les checkpoints. Pour repartir de zéro, arrêter le consommateur puis
supprimer uniquement les répertoires de checkpoints concernés :

```powershell
Remove-Item -Recurse -Force .\checkpoints\toots
Remove-Item -Recurse -Force .\checkpoints\toots_per_hour
Remove-Item -Recurse -Force .\checkpoints\avg_length_per_user
```

### 3. Traitement batch

```powershell
python .\batchToots.py
```

Le batch dédoublonne les toots par `id` et génère les tables suivantes :

| Table | Contenu |
| --- | --- |
| `batch_active_users` | utilisateurs ayant plus de deux toots |
| `batch_hashtags_per_day` | nombre de toots par hashtag et par jour |
| `batch_top_hashtags` | hashtag le plus fréquent de chaque jour |
| `batch_toots_per_day` | nombre de toots par jour |
| `batch_avg_length` | longueur moyenne par langue et moyenne globale |
| `batch_engagement_per_user` | engagement moyen par utilisateur (`favourites_count + reblogs_count`) |

Le script compare une exécution sans optimisation à une exécution avec
répartition des données et cache Spark. Il affiche également la Spark UI à
l'adresse `http://localhost:4040` et attend une validation clavier avant de
s'arrêter.

## Analyse de sentiment

`mlSentiment.py` est un flux de traitement distinct du streaming Kafka.
Il attend le fichier `data/training.csv` au format Sentiment140 avec les
colonnes suivantes :

```text
target,id,date,flag,user,text
```

Le script :

1. charge le jeu d'entraînement ;
2. supprime les URL et les caractères non alphabétiques ;
3. tokenize le texte et retire les stop words ;
4. construit des vecteurs avec `CountVectorizer` ;
5. transforme la cible Sentiment140 (`0` négatif, `4` positif) en `0/1` ;
6. entraîne une régression logistique sur 80 % des données ;
7. évalue l'accuracy sur les 20 % restants ;
8. sauvegarde le modèle dans `modele_sentiment/` ;
9. lit la table PostgreSQL `toots` ;
10. écrit les prédictions dans `toots_sentiments`.

Après avoir vérifié le port PostgreSQL indiqué plus haut et placé le fichier
d'entraînement au bon emplacement :

```powershell
python .\mlSentiment.py
```

La table `toots` doit contenir au minimum `id` et `content`. Le modèle et le
pipeline de préparation sont entraînés à chaque lancement ; le dossier
`modele_sentiment/` est donc écrasé.

## Structure des messages Kafka

Chaque message publié dans `mastodon_stream` ressemble à ceci :

```json
{
  "id": "123456789",
  "created_at": "2026-01-01T12:00:00+00:00",
  "username": "utilisateur",
  "content": "<p>Contenu du toot</p>",
  "language": "fr",
  "hashtags": ["AI", "Spark"],
  "favourites_count": 3,
  "reblogs_count": 1
}
```

## Dépannage

### Spark échoue sur `winutils.exe` ou `hadoop.dll`

Vérifier que le script est lancé depuis `data_streaming` et que
`hadoop\bin\winutils.exe` et `hadoop\bin\hadoop.dll` existent. Les scripts
définissent automatiquement `HADOOP_HOME` à partir du répertoire courant.

### PostgreSQL est inaccessible

Vérifier `docker compose ps`, puis utiliser `127.0.0.1:5433` depuis Windows.
Le port interne du conteneur est `5432`, mais le port exposé par Compose est
`5433`.

### Aucun toot n'est consommé

Vérifier que le producteur est actif, que Kafka écoute sur `localhost:9092` et
que le topic est bien `mastodon_stream`. Vérifier aussi que le jeton Mastodon
est valide et que le compte peut lire les données.

### Erreur de dépendance Java Spark

La première exécution peut télécharger les packages Maven déclarés dans
`spark.jars.packages`. Une connexion Internet est donc nécessaire au premier
démarrage, et les versions Scala/Spark du connecteur Kafka doivent rester
compatibles avec la version de PySpark installée.

## Limites connues

- Les paramètres PostgreSQL sont encore codés en dur dans les scripts.
- Le nettoyage textuel et la détection de langue sont volontairement simples.
- Le producteur ne fait pas d'attente explicite sur `producer.flush()` avant
  de poursuivre.
- Le pipeline est dimensionné pour une exécution locale de démonstration, pas
  pour un déploiement multi-brokers ou une base de production.
- Le traitement de sentiment dépend d'un dataset d'entraînement externe qui
  n'est pas fourni dans ce dossier.

## Arrêt propre

Interrompre les scripts Python avec `Ctrl+C`, puis arrêter les conteneurs :

```powershell
docker compose down
```

Conserver les répertoires `checkpoints/` si le streaming doit reprendre son
état. Ne supprimer le volume PostgreSQL qu'en cas de réinitialisation
volontaire des données.
