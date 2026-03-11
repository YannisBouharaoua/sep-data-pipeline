with regions as (
    select * from unnest([
        struct(1 as code, 'Guadeloupe' as nom),
        struct(2, 'Martinique'),
        struct(3, 'Guyane'),
        struct(4, 'La Réunion'),
        struct(6, 'Mayotte'),
        struct(11, 'Île-de-France'),
        struct(24, 'Centre-Val de Loire'),
        struct(27, 'Bourgogne-Franche-Comté'),
        struct(28, 'Normandie'),
        struct(32, 'Hauts-de-France'),
        struct(44, 'Grand Est'),
        struct(52, 'Pays de la Loire'),
        struct(53, 'Bretagne'),
        struct(75, 'Nouvelle-Aquitaine'),
        struct(76, 'Occitanie'),
        struct(84, 'Auvergne-Rhône-Alpes'),
        struct(93, 'Provence-Alpes-Côte d\'Azur'),
        struct(94, 'Corse')
    ])
),

aggregated as (
    select
        annee,
        region,
        libelle_sexe,
        sum(nb_patients)        as total_patients,
        sum(nb_population)      as total_population,
        round(avg(prevalence), 2) as prevalence_moy
    from {{ ref('stg_effectifs_sep') }}
    where region is not null
    group by annee, region, libelle_sexe
)

select
    a.annee,
    r.nom as region,
    a.libelle_sexe,
    a.total_patients,
    a.total_population,
    a.prevalence_moy
from aggregated a
left join regions r on a.region = r.code
order by annee, prevalence_moy desc
