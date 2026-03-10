"""
SEP Data Pipeline — Ingestion Script
Uploads raw CSV files to GCS, then loads them into BigQuery (raw layer).

Requirements:
    pip install google-cloud-storage google-cloud-bigquery pandas

Usage:
    1. Authenticate: gcloud auth application-default login
    2. Run: python ingest_sep_to_gcp.py
"""

import os
import pandas as pd
from google.cloud import storage, bigquery

# ─────────────────────────────────────────
# CONFIG — à adapter si besoin
# ─────────────────────────────────────────
PROJECT_ID  = "project-f311ccd4-12f3-441a-822"
BUCKET_NAME = "sep-raw-data-project-f311ccd4-12f3-441a-822"
DATASET_ID  = "sep_raw"   # sera créé automatiquement dans BigQuery
LOCATION    = "EU"

# Fichiers locaux à ingérer
# Place tes CSV dans le même dossier que ce script
FILES = {
    "depenses": {
        "local_path": "depenses.csv",          # renomme ton fichier ainsi
        "gcs_path":   "raw/depenses.csv",
        "bq_table":   "depenses",
    },
    "effectifs": {
        "local_path": "effectifs.csv",          # renomme ton fichier ainsi
        "gcs_path":   "raw/effectifs.csv",
        "bq_table":   "effectifs",
    },
}

# ─────────────────────────────────────────
# ÉTAPE 1 — Upload CSV → Cloud Storage
# ─────────────────────────────────────────
def upload_to_gcs(bucket_name: str, local_path: str, gcs_path: str) -> None:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(gcs_path)

    print(f"  ↑ Upload : {local_path} → gs://{bucket_name}/{gcs_path}")
    blob.upload_from_filename(local_path)
    print(f"  ✓ Uploadé ({os.path.getsize(local_path) / 1_000_000:.1f} Mo)")


# ─────────────────────────────────────────
# ÉTAPE 2 — Créer le dataset BigQuery
# ─────────────────────────────────────────
def create_bq_dataset(client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = LOCATION
    dataset = client.create_dataset(dataset_ref, exists_ok=True)
    print(f"  ✓ Dataset BigQuery prêt : {PROJECT_ID}.{DATASET_ID}")


# ─────────────────────────────────────────
# ÉTAPE 3 — Charger GCS → BigQuery (raw)
# ─────────────────────────────────────────
def load_gcs_to_bq(client: bigquery.Client, gcs_path: str, table_id: str) -> None:
    uri        = f"gs://{BUCKET_NAME}/{gcs_path}"
    table_ref  = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format       = bigquery.SourceFormat.CSV,
        skip_leading_rows   = 1,          # ignore header
        autodetect          = True,       # détection automatique des types
        write_disposition   = bigquery.WriteDisposition.WRITE_TRUNCATE,
        encoding            = "UTF-8",
        field_delimiter     = ";",        # les CSV ameli utilisent le point-virgule
        allow_quoted_newlines = True,
    )

    print(f"  → Chargement : {uri}")
    print(f"     dans      : {table_ref}")

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # attend la fin du job

    table = client.get_table(table_ref)
    print(f"  ✓ {table.num_rows:,} lignes chargées dans {table_ref}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    print("\n========================================")
    print("  SEP Data Pipeline — Ingestion")
    print("========================================\n")

    bq_client = bigquery.Client(project=PROJECT_ID)

    # Créer le dataset raw dans BigQuery
    print("[1/3] Initialisation du dataset BigQuery...")
    create_bq_dataset(bq_client)

    for name, cfg in FILES.items():
        print(f"\n[{name.upper()}]")

        # Vérifier que le fichier local existe
        if not os.path.exists(cfg["local_path"]):
            print(f"  ⚠ Fichier non trouvé : {cfg['local_path']} — skipped")
            continue

        # Upload vers GCS
        print("[2/3] Upload vers Cloud Storage...")
        upload_to_gcs(BUCKET_NAME, cfg["local_path"], cfg["gcs_path"])

        # Chargement dans BigQuery
        print("[3/3] Chargement dans BigQuery...")
        load_gcs_to_bq(bq_client, cfg["gcs_path"], cfg["bq_table"])

    print("\n========================================")
    print("  ✅ Ingestion terminée !")
    print(f"  → BigQuery : {PROJECT_ID}.{DATASET_ID}")
    print(f"  → GCS      : gs://{BUCKET_NAME}/raw/")
    print("========================================\n")


if __name__ == "__main__":
    main()