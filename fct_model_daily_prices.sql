-- fct_model_daily_prices
-- One row per model per day: the price and limits on that day.
-- This is the "fact" table behind the price-over-time charts.

-- Partitioned by day and clustered by provider/model, so a chart that
-- filters on a date range or a provider reads only the data it needs.
{{ config(
    materialized = 'table',
    partition_by = {'field': 'snapshot_date', 'data_type': 'date'},
    cluster_by   = ['provider', 'model_id']
) }}

with staging as (

    select * from {{ ref('stg_openrouter__models') }}

),

prices as (

    select
        model_snapshot_id,
        snapshot_date,
        model_id,
        provider,
        context_length,
        max_completion_tokens,
        -- NUMERIC instead of BIGNUMERIC: 6 decimals is plenty here, and
        -- Looker Studio handles NUMERIC better
        cast(input_price_per_1m_tokens as numeric)   as input_price_per_1m_tokens,
        cast(output_price_per_1m_tokens as numeric)  as output_price_per_1m_tokens,
        is_variable_pricing,
        is_free
    from staging

)

select
    *,

    -- Blended price: one number to compare models, assuming a typical app
    -- sends 3 input tokens for every 1 output token (a common industry rule of thumb).
    round((3 * input_price_per_1m_tokens + output_price_per_1m_tokens) / 4, 6)
                                                    as blended_price_per_1m_tokens

from prices