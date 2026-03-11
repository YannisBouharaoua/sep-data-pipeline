{% set sep_codes = ['L03AB04','L03AB05','L03AB07','L03AB08','L03AB11','L03AB13',
                    'L04AA27','L04AA31','N07XX09'] %}

{% for year in [2019, 2020, 2021, 2022] %}
SELECT {{ year }} AS annee, ATC5, L_ATC5, ATC3, L_ATC3,
       age, SEXE, BEN_REG AS region, BOITES, REM, BSE
FROM {{ source('sep_raw', 'openmedic_' ~ year) }}
WHERE ATC5 IN ({% for c in sep_codes %}'{{ c }}'{% if not loop.last %},{% endif %}{% endfor %})
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
