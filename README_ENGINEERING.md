# Data Factory — Pipeline Data Engineering (Airbnb NYC 2019)

> Architecture Medallion · Bronze → Silver → Gold · PySpark · Onyxia  
> Master 2 IMSD · Université Paris-Saclay / Évry · 2025-2026

**Auteurs :** Emmanuel KOURAOGO & Mylane PECH — rôle **Data Engineer**

---

## Contexte

Pipeline de traitement des données **Airbnb New York City 2019** (48 895 listings) selon l'architecture **Medallion** sur la plateforme **Onyxia** (SSP Cloud), entièrement en **PySpark distribué**.

| Zone | Contenu | Responsabilité |
|------|---------|----------------|
| 🥉 Bronze | CSV brut Airbnb (non modifié) | Architecte |
| 🥈 Silver | Données nettoyées en Parquet | **Data Engineer** |
| 🥇 Gold | Features ML + KPI dashboard | **Data Engineer** |

---

## Architecture du pipeline

```
Bronze (CSV)                       Silver (Parquet)               Gold (Parquet)
s3a://USER/bronze/airbnb/   ──►   s3a://USER/silver/airbnb/  ──►  s3a://USER/gold/ml/
AB_NYC_2019.csv                    cleaned/                         s3a://USER/gold/dashboard/

                  [bronze_to_silver.py]             [silver_to_gold.py]
```

---

## Structure du projet

```
Data-factory/
├── src/
│   └── engineering/
│       ├── bronze_to_silver.py    # Nettoyage CSV → Parquet partitionné
│       └── silver_to_gold.py      # Features ML + KPI dashboard
├── reports/
│   └── quality_report.json        # Rapport qualité (nulls avant/après)
└── README_ENGINEERING.md
```

---

## Scripts

### `bronze_to_silver.py` — Nettoyage & qualité

**Nettoyages appliqués :**

| Opération | Détail | Impact |
|-----------|--------|--------|
| Correction des types | `price`, `minimum_nights`, `latitude` → `int`/`double` | Toutes colonnes |
| Valeurs manquantes | `reviews_per_month` null → `0.0` (aucune review, pas une donnée manquante) | 20.56% |
| Valeurs manquantes | `last_review` null → `"No review"` | 20.56% |
| Valeurs manquantes | `name`/`host_name` null → `"Unknown"` | ~0.04% |
| Suppressions aberrantes | `price ≤ 0` (11 lignes) + `minimum_nights > 365` (14 lignes) | 25 lignes |
| Déduplication | Sur clé `id` → idempotence garantie | — |

**Sortie :** Parquet partitionné par `neighbourhood_group` (5 partitions → partition pruning efficace)

---

### `silver_to_gold.py` — Feature Engineering & KPIs

**Gold ML** — 6 features pour le Data Scientist :

| Feature | Description |
|---------|-------------|
| `price_log` | Log du prix (normalisation distribution) |
| `has_reviews` | Indicateur binaire d'activité |
| `occupancy_rate` | Taux d'occupation estimé |
| `zone_avg_price` | Prix moyen de la zone |
| `price_vs_zone_pct` | Écart relatif au prix moyen de zone |
| `zone_type_avg_price` | Prix moyen zone × type de logement |

**Gold Dashboard** — 15 KPI agrégés (5 zones × 3 types) pour le Data Analyst : prix moyen/médian/min/max, disponibilité, reviews.

---

## Résultats

| Métrique | Valeur |
|----------|--------|
| Lignes Bronze | 48 895 |
| Lignes Silver | 48 870 |
| Lignes retirées | 25 (0.05%) |
| Taux de rétention | **99.95%** |
| Partitions Silver | 5 (Bronx, Brooklyn, Manhattan, Queens, Staten Island) |
| Table ML Gold | 48 870 lignes × 22 colonnes |
| KPI Dashboard Gold | 15 lignes (5 zones × 3 types) |

---

## Propriétés garanties

- **100% PySpark** — aucune opération pandas, tout le traitement est distribué
- **Idempotent** — `mode("overwrite")` sur chaque script, relançable sans duplication
- **0 credential en dur** — `S3_USERNAME` via variable d'environnement, accès S3 géré par Onyxia
- **Gouvernance Medallion** — Silver ← Bronze uniquement, Gold ← Silver uniquement

---

## Lancement du pipeline

### Pré-requis
- Service **Jupyter PySpark** lancé sur Onyxia (SSP Cloud)
- Dossiers S3 créés : `bronze/`, `silver/`, `gold/ml/`, `gold/dashboard/`
- CSV brut présent dans Bronze

### Exécution

```bash
cd ~/work/data-factory

# 1. Bronze → Silver
python src/engineering/bronze_to_silver.py

# 2. Silver → Gold
python src/engineering/silver_to_gold.py

# Avec un bucket S3 différent
S3_USERNAME=votre_user python src/engineering/bronze_to_silver.py
```

### Vérification

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("verif").getOrCreate()
df = spark.read.parquet("s3a://ekouraogo/silver/airbnb/cleaned/")
print("Lignes Silver:", df.count())
df.groupBy("neighbourhood_group").count().show()
spark.stop()
```

---

## Stack technique

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Onyxia](https://img.shields.io/badge/Onyxia-SSP_Cloud-blue?style=flat-square)
![S3](https://img.shields.io/badge/S3-MinIO-red?style=flat-square&logo=amazons3&logoColor=white)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-gold?style=flat-square)
![Parquet](https://img.shields.io/badge/Format-Parquet-50ABF1?style=flat-square)

---

## Auteurs

**Emmanuel KOURAOGO** & **Mylane PECH** — M2 IMSD · Paris-Saclay

[![GitHub](https://img.shields.io/badge/GitHub-EKOURAOGO-181717?style=flat-square&logo=github)](https://github.com/EKOURAOGO)

*Année universitaire 2025-2026*
