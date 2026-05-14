# 🚀 Commandes ALS et Streaming

Guide des commandes pour lancer l'entraînement du modèle ALS et le streaming Kafka.

---

## 📋 Table des Matières

- [🤖 Entraînement ALS](#-entraînement-als)
- [📡 Streaming Kafka](#-streaming-kafka)
- [🔧 Configuration](#-configuration)
- [📊 Monitoring](#-monitoring)
- [🐛 Dépannage](#-dépannage)

---

## 🤖 Entraînement ALS

### Commande Standard

```bash
# Entraînement ALS avec configuration par défaut
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/train_als.py
```

### Commande Optimisée

```bash
# Entraînement avec plus de ressources
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=200 \
  /opt/spark-apps/train_als.py
```

### Entraînement avec Logs Détaillés

```bash
# Mode debug pour voir les étapes d'entraînement
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 3g \
  --executor-memory 3g \
  --conf spark.driver.logLevel=DEBUG \
  --conf spark.executor.logLevel=DEBUG \
  /opt/spark-apps/train_als.py
```

### Via Airflow (Recommandé)

```bash
# 1. Débloquer le DAG Airflow
docker-compose exec airflow-scheduler airflow dags unpause recommendation_pipeline

# 2. Déclencher manuellement
docker-compose exec airflow-scheduler airflow dags trigger recommendation_pipeline \
  --conf '{"run_id": "manual_'$(date +%s)"}'

# 3. Suivre l'exécution
docker-compose exec airflow-scheduler airflow dags log recommendation_pipeline --follow
```

---

## 📡 Streaming Kafka

### Commande Standard

```bash
# Streaming avec configuration par défaut
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/streaming.py
```

### Commande Optimisée

```bash
# Streaming avec plus de mémoire et 1 core
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --executor-memory 1536m \
  --executor-cores 1 \
  --conf spark.sql.streaming.checkpointLocation=/opt/spark-apps/checkpoints \
  /opt/spark-apps/streaming.py
```

### Streaming avec Logs

```bash
# Streaming avec logs détaillés
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --executor-memory 1536m \
  --executor-cores 1 \
  --conf spark.driver.logLevel=INFO \
  --conf spark.executor.logLevel=INFO \
  /opt/spark-apps/streaming.py
```

### Streaming en Arrière-plan

```bash
# Lancer en background et récupérer l'ID
STREAMING_JOB=$(docker-compose exec -d spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --executor-memory 1536m \
  --executor-cores 1 \
  /opt/spark-apps/streaming.py | grep -o 'application_[0-9]*')

echo "Streaming job ID: $STREAMING_JOB"

# Arrêter le streaming
docker-compose exec spark-master spark-submit --kill $STREAMING_JOB
```

---

## 🔧 Configuration

### Variables d'Environnement

```bash
# Vérifier les variables
docker-compose exec spark-master env | grep -E "(SPARK|KAFKA|TOP_N)"

# Modifier la configuration
vim .env
# Puis redémarrer
docker-compose restart spark-master
```

### Configuration Spark

```bash
# Voir la configuration Spark active
docker-compose exec spark-master spark-submit --help

# Configuration personnalisée
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=200 \
  /opt/spark-apps/train_als.py
```

---

## 📊 Monitoring

### Monitoring en Temps Réel

```bash
# Terminal 1 : Logs Spark
docker-compose logs -f spark-master

# Terminal 2 : Logs Worker
docker-compose logs -f spark-worker-1

# Terminal 3 : Logs Streaming
docker-compose logs -f spark-master | grep -E "(Streaming|Batch|ERROR)"

# Terminal 4 : UI Spark
curl http://localhost:8080
```

### Monitoring avec Filtres

```bash
# Voir seulement les erreurs
docker-compose logs -f spark-master | grep ERROR

# Voir les métriques de performance
docker-compose logs -f spark-master | grep -E "(Job.*finished|took|memory)"

# Voir l'état du streaming
docker-compose logs -f spark-master | grep -E "(MicroBatch|Batch|streaming)"
```

### Monitoring des Ressources

```bash
# Utilisation mémoire et CPU
docker stats spark-master spark-worker-1

# Utilisation disque
docker-compose exec spark-master df -h /opt/spark-apps/

# Réseau et connexions
docker-compose exec spark-master netstat -tulpn | grep -E "(7077|8080)"
```

---

## 🐛 Dépannage

### Problèmes Communs

#### 1. Mémoire Insuffisante

```bash
# Augmenter la mémoire
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 6g \
  --executor-memory 6g \
  /opt/spark-apps/train_als.py

# Ou modifier docker-compose.yml
# SPARK_WORKER_MEMORY=4g
```

#### 2. Connexion Kafka Échouée

```bash
# Vérifier Kafka
docker-compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# Tester la connectivité
docker-compose exec spark-master bash -c "echo 'test' | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic reviews"
```

#### 3. Modèle Non Trouvé

```bash
# Vérifier les modèles
docker-compose exec spark-master ls -la /opt/spark-apps/als_model
docker-compose exec spark-master ls -la /opt/spark-apps/indexer_model

# Recréer les modèles
docker-compose exec spark-master rm -rf /opt/spark-apps/als_model
docker-compose exec spark-master rm -rf /opt/spark-apps/indexer_model
make train
```

#### 4. Streaming Bloqué

```bash
# Vérifier les offsets Kafka
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic reviews \
  --time -1

# Réinitialiser les checkpoints
docker-compose exec spark-master rm -rf /opt/spark-apps/checkpoints
```

---

## 🎯 Commandes Rapides (Makefile)

Si vous avez créé le Makefile :

```bash
# Entraînement ALS
make train

# Entraînement avec debug
make train-dev

# Streaming standard
make stream

# Streaming limité (10 minutes)
make stream-dev

# Vérifier la santé
make health

# Voir les logs
make logs-spark

# Tester l'API
make api-test

# Nettoyer tout
make clean
```

---

## 📈 Performance Tips

### Entraînement ALS

```bash
# Optimisé pour gros dataset
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 8g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 4 \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.default.parallelism=500 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  /opt/spark-apps/train_als.py
```

### Streaming Haute Performance

```bash
# Streaming optimisé pour haut débit
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --executor-memory 3g \
  --executor-cores 2 \
  --conf spark.sql.streaming.checkpointLocation=/opt/spark-apps/checkpoints \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /opt/spark-apps/streaming.py
```

---

## 📞 Aide

### Documentation

```bash
# Documentation ALS
docker-compose exec spark-master spark-submit --help

# Documentation Spark
curl http://localhost:8080/docs
```

### Support

```bash
# Logs complets pour debug
docker-compose logs spark-master > debug.log 2>&1

# Configuration système
docker-compose config
```

---

## 🔄 Workflows Recommandés

### Workflow 1: Développement

```bash
# 1. Démarrer les services
make up

# 2. Entraîner le modèle
make train

# 3. Démarrer le streaming
make stream

# 4. Tester l'API
make api-test
```

### Workflow 2: Production

```bash
# 1. Nettoyer
make clean

# 2. Déployer
make prod-deploy

# 3. Monitorer
make monitor
```

### Workflow 3: Debug

```bash
# 1. Logs détaillés
make logs

# 2. Vérifier la santé
make health

# 3. Tester les composants
make test
```

---

*Pour plus d'options, voir le [README.md](README.md) complet*
