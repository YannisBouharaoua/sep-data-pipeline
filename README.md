# SEP Data Pipeline
### Analyse épidémiologique & économique de la Sclérose en Plaques en France (2015–2023)

Pipeline de données end-to-end construit sur des données officielles de l'Assurance Maladie.  
**Stack :** Python · Google Cloud Storage · BigQuery · dbt · Looker Studio

---

## Pourquoi ce projet

La sclérose en plaques touche environ 120 000 personnes en France et représente plusieurs milliards d'euros de dépenses annuelles pour l'Assurance Maladie. Les données existent en open data mais sont brutes, volumineuses (5+ millions de lignes) et inexploitables sans pipeline.

L'objectif : construire une architecture data complète qui transforme ces fichiers CSV en analyses lisibles par un décideur.

---

## Architecture

```
data.gouv.fr / data.ameli.fr / data.ameli.fr (Open Medic)
        │
        ▼
  Python (ingestion + preprocessing)
        │
        ▼
  Google Cloud Storage       ← raw layer (CSV bruts)
        │
        ▼
  BigQuery
  ├── raw        (données brutes chargées telles quelles)
  ├── staging    (filtre SEP, nettoyage, renommage)
  └── mart       (agrégations analytiques prêtes à consommer)
        │
        ▼
  Looker Studio Dashboard
```

Les transformations raw → staging → mart sont gérées par **dbt** — versionnées dans Git, testées automatiquement, et documentées avec lineage graph.

---

## Données

| Source | Volume | Période |
|---|---|---|
| Effectifs patients par pathologie, sexe, âge, territoire (CNAM) | 5 216 400 lignes | 2015–2023 |
| Dépenses remboursées par pathologie (CNAM) | 22 320 lignes | 2015–2023 |
| Open Medic — prescriptions de médicaments (CNAM) | 7 349 511 lignes | 2019–2022 |

Données produites par la Caisse Nationale de l'Assurance Maladie — licence ODbL.

---

## Ce que les données révèlent

**Épidémiologie**
- La prévalence de la SEP est stable autour de 1,2 patient pour 1 000 personnes protégées
- Les femmes représentent environ 70% des patients sur toute la période — ratio constant
- Les régions Grand Est, Bourgogne-Franche-Comté et Normandie affichent la prévalence la plus élevée, cohérent avec le gradient nord/ensoleillement documenté dans la littérature médicale

**Économique**
- Le coût total pris en charge est passé de **3,6 Md€ en 2015 à 4,4 Md€ en 2023** (+23% en 8 ans)
- Les soins de ville représentent 40% des dépenses — devant les hospitalisations (33%), ce qui reflète le virage vers des traitements de fond ambulatoires
- Un creux est visible en 2019–2020, attribuable à la réduction du recours aux soins pendant le Covid

**Prescriptions de traitements de fond (Open Medic)**
- Le Fingolimod est le traitement le plus remboursé en 2019 (170 M€) mais recule à 120 M€ en 2022, probablement lié à l'arrivée des génériques
- Le Teriflunomide progresse en volume (+13% de boîtes entre 2019 et 2022) à coût stable — adoption croissante
- Les interférons de première génération (alfa-2a, alfa-2b) s'effacent progressivement au profit des traitements oraux

---

## Dashboard

[🔗 Voir le dashboard Looker Studio](https://lookerstudio.google.com/u/0/reporting/32588847-ac90-47e0-9abb-68fb80583430/page/B9erF)

4 visualisations interactives :
- Évolution du nombre de patients par année et par sexe
- Évolution des dépenses totales remboursées
- Prévalence par région française
- Répartition des dépenses par poste de soins

---

## Stack & Compétences mobilisées

| Domaine | Outils |
|---|---|
| Ingestion | Python, google-cloud-storage, google-cloud-bigquery |
| Preprocessing | pandas (normalisation encodage latin-1, formats numériques français) |
| Stockage | Google Cloud Storage (data lake raw) |
| Transformation | dbt (modèles SQL versionnés, tests, documentation) |
| Entrepôt | BigQuery (3 couches : raw / staging / mart) |
| Visualisation | Looker Studio |
| Environnement | gcloud CLI, venv, Git |

---

## Lancer le projet

```bash
git clone https://github.com/YannisBouharaoua/sep-data-pipeline.git
cd sep-data-pipeline
python3.11 -m venv venv && source venv/bin/activate
pip install google-cloud-storage google-cloud-bigquery pandas dbt-bigquery

# Authentification GCP
gcloud auth application-default login
gcloud config set project [PROJECT_ID]
```

Télécharge les CSV depuis [data.ameli.fr](https://data.ameli.fr) et [data.gouv.fr](https://www.data.gouv.fr), renomme-les `effectifs.csv`, `depenses.csv` et `openmedic_YYYY.csv`, puis :

```bash
# Preprocessing Open Medic (normalisation formats numériques)
python preprocess_openmedic.py

# Ingestion vers GCS et BigQuery
python ingest_sep_to_gcp.py

# Transformations dbt
cd sep_dbt
dbt run    # crée les modèles staging et mart
dbt test   # vérifie la qualité des données
dbt docs generate && dbt docs serve  # documentation + lineage graph
```

---

## Structure du projet

```
sep-data-pipeline/
├── README.md
├── ingest_sep_to_gcp.py        ← pipeline d'ingestion Python
├── preprocess_openmedic.py     ← nettoyage formats Open Medic
└── sep_dbt/
    ├── dbt_project.yml
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   ├── schema.yml              ← tests de qualité
        │   ├── stg_effectifs_sep.sql
        │   ├── stg_depenses_sep.sql
        │   └── stg_openmedic_sep.sql   ← filtre 9 codes ATC médicaments SEP
        └── marts/
            ├── mart_prevalence_region.sql
            ├── mart_depenses_poste.sql
            └── mart_prescriptions_sep.sql  ← agrégation par médicament/année/région
```

---

## Améliorations prévues

- Orchestration annuelle avec Cloud Composer (Airflow)
- Tests dbt supplémentaires sur les marts
- Intégration des données hospitalières (PMSI) pour compléter les prescriptions ville

---

**Yannis Bouharaoua** — Data Engineer Junior  
École 42 Toulouse · [GitHub](https://github.com/YannisBouharaoua) · [LinkedIn](https://www.linkedin.com/in/yannis-bouharaoua-541170298/)