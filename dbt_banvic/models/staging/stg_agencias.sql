with agencias as (

    select
        cod_agencia,
        nome,
        endereco,
        cidade,
        uf,
        data_abertura,
        tipo_agencia
    from {{ source('raw', 'agencias') }}

)

select
    cod_agencia,
    nome,
    endereco,
    cidade,
    uf,
    data_abertura,
    tipo_agencia
from agencias