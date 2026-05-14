"""
Kafka Producer — simule un flux temps réel depuis le CSV Amazon Reviews.
Envoie un message (UserId, ProductId, Score, Time) toutes les DELAY_SECONDS secondes.
"""

import os
import csv
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config depuis variables d'environnement ──────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC             = os.getenv("KAFKA_TOPIC", "reviews")
DATA_PATH         = os.getenv("DATA_PATH", "/data/Reviews.csv")
DELAY             = float(os.getenv("DELAY_SECONDS", "0.1"))


def wait_for_kafka(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Attend que Kafka soit disponible avant de démarrer."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",          # garantit la livraison
                retries=3,
            )
            logger.info("Connecté à Kafka (%s)", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka non disponible, tentative %d/%d", attempt + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka après %d tentatives" % retries)


def stream_reviews(producer: KafkaProducer) -> None:
    """Lit le CSV ligne par ligne et publie chaque ligne comme un événement."""
    sent = 0
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Construire le message — uniquement les champs nécessaires
            message = {
                "UserId":    row.get("UserId", ""),
                "ProductId": row.get("ProductId", ""),
                "Score":     float(row.get("Score", 0)),
                "Time":      int(row.get("Time", 0)),
            }

            # Sauter les lignes incomplètes
            if not message["UserId"] or not message["ProductId"]:
                continue

            producer.send(TOPIC, value=message)
            sent += 1

            if sent % 1000 == 0:
                logger.info("Envoyé %d messages", sent)
                producer.flush()   # vider le buffer périodiquement

            time.sleep(DELAY)

    producer.flush()
    logger.info("Streaming terminé — %d messages envoyés", sent)


if __name__ == "__main__":
    producer = wait_for_kafka()
    stream_reviews(producer)