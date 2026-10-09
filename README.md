# Pipeline de streaming Mastodon

Ce dossier contient un pipeline local d'ingestion, de transformation et
d'analyse de publications Mastodon contenant le hashtag `AI`.

Le projet combine :

- Mastodon.social comme source de données ;
- Kafka pour transporter les messages en temps réel ;
- Apache Spark Structured Streaming pour nettoyer et agréger les messages ;
- PostgreSQL pour stocker les données ;
- Spark MLlib pour entraîner un modèle de sentiment ;
- un notebook Jupyter pour explorer les résultats.

Le projet est configuré pour une exécution locale sous Windows. Il s'agit
d'un projet de démonstration : les scripts utilisent Spark en mode
`local[*]`, Kafka avec un seul broker KRaft et PostgreSQL dans Docker.

## Architecture

```text
Mastodon.social (#AI)
        |
        v
getToots.py
        |
        v
Kafka : mastodon_stream
        |
        v
cleanToots.py
        |
        +--> PostgreSQL : toots
        +--> PostgreSQL : toots_per_hour
        +--> PostgreSQL : avg_length_per_user
                         ^
                         |
batchToots.py -----------+
        |
        +--> batch_active_users
        +--> batch_hashtags_per_day
        +--> batch_top_hashtags
        +--> batch_toots_per_day
        +--> batch_avg_length
        +--> batch_engagement_per_user

data/training.csv --> mlSentiment.py --> modele_sentiment/
                                  |
                                  +--> PostgreSQL : toots_sentiments

dashboard.ipynb --> lecture PostgreSQL et graphiques
```

## Contenu du dossier

| Fichier ou dossier | Rôle |
| --- | --- |
| `docker-compose.yaml` | Lance Kafka 3.9.0 et PostgreSQL 18. |
| `getToots.py` | Vérifie le compte Mastodon et publie les toots du hashtag `AI` dans Kafka. |
| `cleanToots.py` | Consomme Kafka, nettoie les toots et écrit les résultats dans PostgreSQL. |
| `batchToots.py` | Lit `toots`, dédoublonne les lignes et calcule six agrégations batch. |
| `mlSentiment.py` | Entraîne une régression logistique sur Sentiment140 et prédit le sentiment des toots. |
| `dashboard.ipynb` | Notebook Jupyter d'exploration des sentiments, toots, heures et hashtags. |
| `hadoop/bin/` | Contient `winutils.exe` et `hadoop.dll`, utilisés par Spark sous Windows. |
| `checkpoints/` | État des requêtes Spark Structured Streaming. |
| `data/training.csv` | Jeu de données Sentiment140 attendu par `mlSentiment.py`. |
| `modele_sentiment/` | Modèle Spark ML généré par `mlSentiment.py`. |

`.venv/`, `data/`, `hadoop/`, `checkpoints/` et `modele_sentiment/` sont
ignorés par Git dans le projet. Les dossiers `checkpoints/` et
`modele_sentiment/` sont des sorties d'exécution, tandis que `data/` doit
contenir les données d'entraînement locales.

## Prérequis

- Windows 10 ou 11 ;
- Python 3.12 ;
- Java installé et disponible dans le `PATH` ;
- Docker Desktop démarré avec Docker Compose ;
- un compte Mastodon.social et un jeton d'accès autorisant la lecture ;
- une connexion Internet lors du premier lancement de Spark, afin de
  télécharger les dépendances Maven déclarées dans les scripts ;
- suffisamment de mémoire pour exécuter simultanément Spark, Kafka et
  PostgreSQL.

Les dépendances Python utilisées par les scripts sont :

- `pyspark` ;
- `python-dotenv` ;
- `Mastodon.py` ;
- `kafka-python`.

Le notebook utilise également :

- `pandas` ;
- `matplotlib` ;
- `seaborn` ;
- `sqlalchemy` ;
- un pilote PostgreSQL SQLAlchemy, par exemple `psycopg2-binary`.

Les scripts téléchargent eux-mêmes les dépendances Java suivantes :

| Script | Dépendances Spark téléchargées |
| --- | --- |
| `cleanToots.py` | `spark-sql-kafka-0-10_2.13:4.2.0` et `postgresql:42.7.7` |
| `batchToots.py` | `postgresql:42.7.7` |
| `mlSentiment.py` | `postgresql:42.6.0` |

La version de PySpark installée doit rester compatible avec le connecteur
Kafka Spark SQL `4.2.0` utilisé par `cleanToots.py`.

## Installation

Depuis PowerShell :

```powershell
cd D:\5SPAR\data_streaming
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install pyspark python-dotenv Mastodon.py kafka-python
python -m pip install pandas matplotlib seaborn sqlalchemy psycopg2-binary jupyter
```

Si PowerShell refuse l'activation de l'environnement virtuel :

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Les scripts doivent être lancés depuis `D:\5SPAR\data_streaming`. Ils
construisent le chemin de `hadoop` avec le répertoire courant ; les lancer
depuis un autre dossier peut donc provoquer une erreur liée à
`winutils.exe` ou `hadoop.dll`.

## Configuration

Créer `.env` à la racine du dossier :

```dotenv
access_token=JETON_MASTODON
POSTGRES_USER=mastodon
POSTGRES_PASSWORD=mastodon
POSTGRES_DB=mastodon_db
```

Dans l'état actuel du code, seul `access_token` est lu par les scripts
Python. Les identifiants PostgreSQL sont encore écrits en dur dans
`cleanToots.py`, `batchToots.py` et `mlSentiment.py` et correspondent aux
valeurs du fichier Compose :

```text
utilisateur : mastodon
mot de passe : mastodon
base         : mastodon_db
```

Ne jamais versionner un vrai jeton Mastodon. Si un jeton a été publié,
révoquer celui-ci depuis Mastodon et en générer un nouveau.

### Ports et connexions

| Service | Depuis Windows | Entre conteneurs |
| --- | --- | --- |
| Kafka | `localhost:9092` | `kafka:29092` |
| PostgreSQL | `127.0.0.1:5433` | `db:5432` |
| Spark UI | `http://localhost:4040` | — |

Les trois scripts Python utilisent actuellement PostgreSQL sur
`127.0.0.1:5433`. Le port `5432` est uniquement le port PostgreSQL à
l'intérieur du conteneur.

Le notebook contient une configuration de connexion SQLAlchemy à compléter
ou à vérifier avant exécution. Elle doit utiliser une URL valide correspondant
au port hôte `5433`, par exemple :

```python
create_engine(
    "postgresql+psycopg2://mastodon:mastodon@127.0.0.1:5433/mastodon_db"
)
```

## Démarrer Kafka et PostgreSQL

```powershell
docker compose up -d
docker compose ps
```

Les conteneurs `kafka` et `postgres-dev` doivent être en état `running`.

Pour arrêter les services sans supprimer les données PostgreSQL :

```powershell
docker compose down
```

Pour supprimer également le volume PostgreSQL :

```powershell
docker compose down -v
```

Cette dernière commande réinitialise toutes les tables et données stockées
dans PostgreSQL.

## Exécuter le pipeline temps réel

Le producteur et le consommateur sont des processus continus. Ouvrir deux
terminaux PowerShell, activer `.venv` dans chacun et se placer dans
`D:\5SPAR\data_streaming`.

### 1. Producteur Mastodon

```powershell
python .\getToots.py
```

Le script :

1. charge `access_token` depuis `.env` ;
2. vérifie les identifiants Mastodon ;
3. interroge toutes les cinq secondes le hashtag `AI` sur
   `https://mastodon.social` ;
4. publie les messages JSON dans le topic Kafka `mastodon_stream`.

Le producteur conserve l'identifiant du dernier lot reçu avec `since_id`.
Il intercepte les erreurs réseau Mastodon et continue après cinq secondes.
Arrêter le processus avec `Ctrl+C`.

### 2. Consommateur Spark Structured Streaming

```powershell
python .\cleanToots.py
```

Le consommateur :

1. lit `mastodon_stream` depuis `startingOffsets=earliest` ;
2. désérialise la valeur JSON ;
3. convertit `created_at` en timestamp ;
4. retire les balises HTML de `content` ;
5. conserve uniquement les langues `en` et `fr` ;
6. ajoute `content_length` ;
7. écrit les toots nettoyés dans `toots` en mode `append` ;
8. recalcule `toots_per_hour` et `avg_length_per_user` en mode `complete` ;
9. sauvegarde l'état dans les trois répertoires de checkpoints.

Le processus reste actif jusqu'à `Ctrl+C`. Conserver les checkpoints permet
à Spark de reprendre les offsets Kafka. Pour repartir d'un état de streaming
vierge, arrêter le consommateur puis supprimer uniquement les checkpoints :

```powershell
Remove-Item -Recurse -Force .\checkpoints\toots
Remove-Item -Recurse -Force .\checkpoints\toots_per_hour
Remove-Item -Recurse -Force .\checkpoints\avg_length_per_user
```

### Format des messages Kafka

Chaque message publié dans `mastodon_stream` contient les champs suivants :

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

## Traitement batch

Le batch lit la table `toots` depuis PostgreSQL. Il dédoublonne les lignes
sur `id`, puis exécute deux scénarios :

1. sans optimisation ;
2. avec `repartition`, cache Spark et `coalesce(1)` avant les écritures.

```powershell
python .\batchToots.py
```

Les deux scénarios écrivent les tables suivantes :

| Table | Calcul |
| --- | --- |
| `batch_active_users` | utilisateurs ayant strictement plus de deux toots (`X_MIN_TOOTS = 2`) |
| `batch_hashtags_per_day` | nombre de toots par jour et par hashtag, avec hashtags passés en minuscules |
| `batch_top_hashtags` | hashtag le plus fréquent de chaque jour |
| `batch_toots_per_day` | nombre de toots par jour |
| `batch_avg_length` | longueur moyenne par langue et moyenne globale (`language = ALL`) |
| `batch_engagement_per_user` | engagement moyen par utilisateur : `favourites_count + reblogs_count` |

Le script affiche les résultats, compare les temps d'exécution et affiche le
nombre de partitions. Il attend ensuite une pression sur Entrée en affichant
l'URL de la Spark UI.

## Analyse de sentiment

`mlSentiment.py` est indépendant du flux Kafka. Il attend le fichier local
`data/training.csv`, qui n'est pas fourni par le code source du projet. Le
fichier doit être au format Sentiment140, sans en-tête attendu par le script,
avec les colonnes :

```text
target,id,date,flag,user,text
```

Le script se lance depuis la racine du projet :

```powershell
python .\mlSentiment.py
```

Il :

1. charge le CSV avec le schéma Sentiment140 ;
2. supprime les URL et les caractères non alphabétiques ;
3. tokenize le texte et retire les stop words ;
4. construit les vecteurs avec `CountVectorizer` ;
5. transforme la cible `0/4` en label `0.0/1.0` ;
6. sépare les données en 80 % d'entraînement et 20 % de test (`seed=42`) ;
7. entraîne une régression logistique Spark ML (`maxIter=10`) ;
8. affiche l'accuracy sur le jeu de test ;
9. sauvegarde le modèle dans `modele_sentiment/` ;
10. lit la table `toots` ;
11. applique le pipeline et le modèle aux contenus des toots ;
12. remplace la table `toots_sentiments` avec les colonnes `id`, `content` et
    `prediction`.

Le dossier `modele_sentiment/` est écrasé à chaque lancement. La table
`toots` doit donc déjà exister et contenir au minimum `id` et `content`.

## Notebook d'analyse

Une fois `toots_sentiments` remplie, lancer Jupyter depuis la racine :

```powershell
python -m jupyter notebook .\dashboard.ipynb
```

Le notebook utilise PostgreSQL et présente notamment :

- un aperçu des prédictions de sentiment ;
- la distribution des sentiments positifs et négatifs ;
- un aperçu de la table `toots` ;
- le nombre de toots par heure ;
- les hashtags présents et les dix hashtags les plus fréquents ;
- un tableau croisé du nombre de messages par jour et par heure.

Avant d'exécuter les cellules, vérifier la variable `engine` et remplacer la
chaîne de connexion masquée ou incorrecte par une URL SQLAlchemy valide (voir
la section [Ports et connexions](#ports-et-connexions)).

## Schéma produit dans PostgreSQL

La table `toots` issue du streaming contient les champs du message Kafka et
le champ calculé `content_length` :

```text
id, created_at, username, content, language, hashtags,
favourites_count, reblogs_count, content_length
```

Les tables d'agrégation contiennent les colonnes correspondant à leurs
calculs. Les tables `toots`, `toots_per_hour` et `avg_length_per_user` sont
alimentées par `cleanToots.py`; les tables commençant par `batch_` sont
réécrites par `batchToots.py`; `toots_sentiments` est réécrite par
`mlSentiment.py`.

## Dépannage

### Spark échoue sur `winutils.exe` ou `hadoop.dll`

Vérifier que le script est lancé depuis `data_streaming` et que
`hadoop\bin\winutils.exe` et `hadoop\bin\hadoop.dll` existent.

### PostgreSQL est inaccessible

Vérifier :

```powershell
docker compose ps
Test-NetConnection 127.0.0.1 -Port 5433
```

Depuis Windows, utiliser `127.0.0.1:5433`. Le port interne `5432` n'est pas
celui à utiliser depuis les scripts lancés sur l'hôte.

### Aucun toot n'est consommé

Vérifier que le producteur est actif, que Kafka écoute sur `localhost:9092`,
que le topic est `mastodon_stream` et que le jeton Mastodon est valide.

### Les dépendances Spark ne sont pas téléchargées

Les premières exécutions peuvent nécessiter Internet pour télécharger les
packages Maven déclarés dans `spark.jars.packages`. Vérifier également la
compatibilité entre la version de PySpark installée et les versions Scala,
Spark et Kafka demandées par le script.

### Le modèle de sentiment échoue au chargement des données

Vérifier que `data/training.csv` existe, qu'il suit le format Sentiment140 et
que le script est lancé depuis `data_streaming`, car le chemin est relatif.

## Limites connues

- Les paramètres PostgreSQL ne sont pas encore lus depuis `.env`.
- Le producteur ne vérifie pas explicitement le résultat de
  `producer.send()` et n'appelle pas `producer.flush()` avant de poursuivre.
- Le nettoyage HTML et le nettoyage textuel du sentiment sont volontairement
  simples.
- Le modèle de sentiment est réentraîné à chaque exécution.
- Le dataset d'entraînement n'est pas versionné dans le projet.
- Les checkpoints et les écritures JDBC sont adaptés à une démonstration
  locale, pas à un déploiement multi-brokers ou à une base de production.
- Le notebook contient des sorties Jupyter enregistrées et nécessite une
  configuration manuelle de sa connexion SQLAlchemy.

## Arrêt propre

Arrêter les scripts Python avec `Ctrl+C`, puis arrêter les conteneurs :

```powershell
docker compose down
```

Conserver `checkpoints/` si le streaming doit reprendre ses offsets. Ne
supprimer le volume PostgreSQL avec `docker compose down -v` qu'en cas de
réinitialisation volontaire des données.
