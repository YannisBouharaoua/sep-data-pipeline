with source as (
    select * from {{ source('sep_raw', 'depenses') }}
),

renamed as (
    select
        annee,
        patho_niv2,
        top                    as code_pathologie,
        dep_niv_1              as poste,
        dep_niv_2              as sous_poste,
        montant,
        montant_moy,
        Ntop                   as nb_patients,
        N_recourant_au_poste   as nb_recourants,
        type_somme
    from source
    where lower(patho_niv2) like '%scl%'
)

select * from renamed
