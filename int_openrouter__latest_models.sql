-- int_openrouter__latest_models
-- Only the newest day's snapshot: one row per model, as it looks right now.
-- The snapshot (snapshots/snap_model_pricing.yml) compares this to what it
-- saw last time, to spot price and capability changes.

with staging as (

    select * from {{ ref('stg_openrouter__models') }}

)

select
    model_id,
    provider,
    model_name,
    snapshot_date,
    context_length,
    max_completion_tokens,
    input_price_per_1m_tokens,
    output_price_per_1m_tokens,
    is_variable_pricing,
    is_free

from staging

-- keep only the most recent day that was loaded
where snapshot_date = (select max(snapshot_date) from staging)