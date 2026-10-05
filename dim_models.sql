-- dim_models
-- One row per AI model ever seen: who makes it, what it can do, its latest
-- price, and when it first and last appeared in the API.
-- This is the "dimension" table: the dashboard uses it to describe models.

with staging as (

    select * from {{ ref('stg_openrouter__models') }}

),

-- the newest row for each model (its latest known details)
latest as (

    select *
    from staging
    qualify row_number() over (partition by model_id order by snapshot_date desc) = 1

),

-- when each model first and last showed up in the daily snapshots
seen as (

    select
        model_id,
        min(snapshot_date) as first_seen_date,
        max(snapshot_date) as last_seen_date
    from staging
    group by model_id

),

-- how many price/limit versions the snapshot has recorded for each model
versions as (

    select model_id, count(*) as price_version_count
    from {{ ref('snap_model_pricing') }}
    group by model_id

)

select
    latest.model_id,
    latest.provider,
    latest.model_name,
    latest.model_created_at,
    latest.knowledge_cutoff,
    latest.context_length,
    latest.max_completion_tokens,
    latest.modality,
    latest.tokenizer,
    latest.is_moderated,
    cast(latest.input_price_per_1m_tokens as numeric)   as input_price_per_1m_tokens,
    cast(latest.output_price_per_1m_tokens as numeric)  as output_price_per_1m_tokens,
    latest.is_variable_pricing,
    latest.is_free,
    seen.first_seen_date,
    seen.last_seen_date,

    -- still listed in the newest snapshot? If not, the model was removed.
    seen.last_seen_date = (select max(snapshot_date) from staging)  as is_active,

    coalesce(versions.price_version_count, 0)            as price_version_count

from latest
join seen using (model_id)
left join versions using (model_id)