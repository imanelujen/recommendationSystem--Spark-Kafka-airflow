#!/usr/bin/env python3
"""
Crée l'indexer pour les UserIds à partir des données existantes
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CreateIndexer")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Lire les données existantes
    df = spark.read.csv('/opt/spark-data/Reviews.csv', header=True, inferSchema=True)
    df = df.select('UserId').dropna().distinct()
    
    # Créer l'indexer
    indexer = StringIndexer(inputCol='UserId', outputCol='userIndex', handleInvalid='skip')
    indexer_model = indexer.fit(df)
    
    # Sauvegarder l'indexer
    indexer_model.write().overwrite().save('/opt/spark-apps/indexer_model')
    
    print("✅ Indexer créé avec succès")
    print(f"✅ {df.count()} utilisateurs uniques indexés")
    
    spark.stop()
