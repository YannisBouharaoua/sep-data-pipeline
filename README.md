# SEP Data Pipeline
### Analyse épidémiologique & économique de la Sclérose en Plaques en France (2015–2023)

Pipeline de données end-to-end construit sur des données officielles de l'Assurance Maladie.  
**Stack :** Python · Google Cloud Storage · BigQuery · Looker Studio

---

## Pourquoi ce projet

La sclérose en plaques touche environ 120 000 personnes en France et représente plusieurs milliards d'euros de dépenses annuelles pour l'Assurance Maladie. Les données existent en open data mais sont brutes, volumineuses (5+ millions de lignes) et inexploitables sans pipeline.

L'objectif : construire une architecture data complète qui transforme ces fichiers CSV en analyses lisibles par un décideur.

---

## Architecture

```
data.gouv.fr / data.ameli.fr
        │
        ▼
  Python (ingestion)
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

Choix technique clé : séparer les 3 couches raw/staging/mart dans BigQuery permet de retracer l'origine de chaque transformation et de rejouer n'importe quelle étape sans retoucher les données sources.

---

## Données

| Source | Volume | Période |
|---|---|---|
| Effectifs patients par pathologie, sexe, âge, territoire (CNAM) | 5 216 400 lignes | 2015–2023 |
| Dépenses remboursées par pathologie (CNAM) | 22 320 lignes | 2015–2023 |

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
| Stockage | Google Cloud Storage (data lake raw) |
| Transformation | BigQuery SQL (3 couches : raw / staging / mart) |
| Visualisation | Looker Studio |
| Environnement | gcloud CLI, venv, Git |

---

## Lancer le projet

```bash
git clone https://github.com/[username]/sep-data-pipeline.git
cd sep-data-pipeline
python3 -m venv venv && source venv/bin/activate
pip install google-cloud-storage google-cloud-bigquery pandas

# Authentification GCP
gcloud auth application-default login
gcloud config set project [PROJECT_ID]
```

Télécharge les CSV depuis [data.ameli.fr](https://data.ameli.fr) et [data.gouv.fr](https://www.data.gouv.fr), renomme-les `effectifs.csv` et `depenses.csv`, puis :

```bash
python ingest_sep_to_gcp.py
```

Les requêtes SQL de transformation sont dans `sql/`.

---

## Améliorations prévues

- Intégration dbt pour versionner et tester les transformations
- Ajout des données Open Medic (prescriptions de traitements de fond sur 10 ans)
- Orchestration annuelle avec Cloud Composer (Airflow)

---

**Yannis Bouharaoua** — Data Engineering  
École 42 Toulouse · [GitHub](https://github.com/YannisBouharaoua) · [LinkedIn](https://www.linkedin.com/in/yannis-bouharaoua-541170298/)
