with source as (
    select * from {{ source('sep_raw', 'effectifs') }}
),

renamed as (
    select
        annee,
        patho_niv1,
        patho_niv2,
        top                  as code_pathologie,
        cla_age_5,
        libelle_classe_age,
        sexe,
        libelle_sexe,
        region,
        dept,
        Ntop                 as nb_patients,
        Npop                 as nb_population,
        prev                 as prevalence
    from source
    where top = 'NEU_SEP_IND'
      and dept is not null
)

select * from renamed
