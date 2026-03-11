select
    annee,
    poste,
    sous_poste,
    sum(montant)        as montant_total,
    avg(montant_moy)    as montant_moyen_patient
from {{ ref('stg_depenses_sep') }}
group by annee, poste, sous_poste
order by annee, montant_total desc
