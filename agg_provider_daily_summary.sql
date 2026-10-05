-- agg_provider_daily_summary
-- One row per provider per day: how many models they list, how many are
-- free, their typical and cheapest price. Pre-computed so the dashboard
-- reads a small table instead of scanning every model.

with daily as (

    select * from {{ ref('fct_model_daily_prices') }}

),

-- each provider's cheapest paid model on each day
cheapest as (

    select
        snapshot_date,
        provider,
        model_id                     as cheapest_paid_model_id,
        blended_price_per_1m_tokens  as cheapest_paid_blended_price
    from daily
    where blended_price_per_1m_tokens > 0
    qualify row_number() over (
        partition by snapshot_date, provider
        order by blended_price_per_1m_tokens, model_id     -- model_id breaks ties
    ) = 1

),

summary as (

    select
        snapshot_date,
        provider,
        count(*)                                    as model_count,
        countif(is_free)                            as free_model_count,
        max(context_length)                         as max_context_length,
        -- median blended price of paid models (approx_quantiles(x, 2)[1] = the middle value)
        approx_quantiles(if(blended_price_per_1m_tokens > 0, blended_price_per_1m_tokens, null), 2)[safe_offset(1)]
                                                    as median_paid_blended_price
    from daily
    group by snapshot_date, provider

)

select
    concat(summary.provider, '|', cast(summary.snapshot_date as string))  as provider_day_id,
    summary.*,
    cheapest.cheapest_paid_model_id,
    cheapest.cheapest_paid_blended_price
from summary
left join cheapest using (snapshot_date, provider)