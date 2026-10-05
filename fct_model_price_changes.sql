-- fct_model_price_changes
-- One row per change: every time a model's price or limits changed,
-- with the old value, the new value and the % change.
-- Built from the SCD Type 2 snapshot, which keeps every version of each model.

with versions as (

    select
        model_id,
        provider,
        model_name,
        snapshot_date                                    as changed_on,   -- day this version was first seen
        dbt_valid_from,
        dbt_valid_to,
        cast(input_price_per_1m_tokens as numeric)       as new_input_price,
        cast(output_price_per_1m_tokens as numeric)      as new_output_price,
        context_length                                   as new_context_length,
        is_variable_pricing,

        -- version 1, 2, 3 ... of this model, oldest first
        row_number() over w                              as version_number,

        -- the previous version's values (null for version 1)
        cast(lag(input_price_per_1m_tokens) over w as numeric)   as old_input_price,
        cast(lag(output_price_per_1m_tokens) over w as numeric)  as old_output_price,
        lag(context_length) over w                               as old_context_length,
        lag(is_variable_pricing) over w                          as old_is_variable_pricing

    from {{ ref('snap_model_pricing') }}
    window w as (partition by model_id order by dbt_valid_from)

),

changes as (

    select
        *,
        -- difference vs the previous version (0 when either side is unknown)
        coalesce(new_input_price - old_input_price, 0)   as input_diff,
        coalesce(new_output_price - old_output_price, 0) as output_diff
    from versions
    where version_number > 1        -- version 1 is the first sighting, not a change

)

select
    concat(model_id, '|', cast(version_number as string))  as price_change_id,
    model_id,
    provider,
    model_name,
    changed_on,
    version_number,
    old_input_price,
    new_input_price,
    round(safe_divide(new_input_price - old_input_price, old_input_price) * 100, 2)
                                                            as input_price_change_pct,
    old_output_price,
    new_output_price,
    round(safe_divide(new_output_price - old_output_price, old_output_price) * 100, 2)
                                                            as output_price_change_pct,
    old_context_length,
    new_context_length,

    -- what kind of change was it?
    case
        when (input_diff < 0 or output_diff < 0) and not (input_diff > 0 or output_diff > 0)
            then 'price_cut'
        when (input_diff > 0 or output_diff > 0) and not (input_diff < 0 or output_diff < 0)
            then 'price_increase'
        when input_diff != 0 or output_diff != 0
            then 'mixed_price_change'                       -- one price up, the other down
        when is_variable_pricing != old_is_variable_pricing
            then 'pricing_type_change'                      -- switched to/from variable pricing
        else 'limits_change'                                -- price same, context/max tokens changed
    end                                                     as change_type,

    dbt_valid_to is null                                    as is_current_version

from changes