SELECT
    annee,
    ATC5                          AS code_medicament,
    L_ATC5                        AS nom_medicament,
    ATC3                          AS classe_atc,
    L_ATC3                        AS libelle_classe,
    region,
    SUM(BOITES)                   AS total_boites,
    SUM(REM)                      AS total_rembourse,
    COUNT(*)                      AS nb_lignes
FROM {{ ref('stg_openmedic_sep') }}
GROUP BY annee, ATC5, L_ATC5, ATC3, L_ATC3, region
ORDER BY annee, total_rembourse DESC
