-- Turns the raw JSON into clean, typed columns: one row per model per day.

with source as (

    select * from {{ source('openrouter', 'model_snapshots') }}

),

parsed as (

    select
        -- keys
        concat(model_id, '|', cast(snapshot_date as string))            as model_snapshot_id,
        snapshot_date,
        model_id,
        split(model_id, '/')[safe_offset(0)]                            as provider,

        -- descriptive fields
        json_value(raw_json, '$.name')                                  as model_name,
        timestamp_seconds(safe_cast(json_value(raw_json, '$.created') as int64))
                                                                        as model_created_at,
        safe.parse_date('%Y-%m-%d', json_value(raw_json, '$.knowledge_cutoff'))
                                                                        as knowledge_cutoff,

        -- capabilities
        safe_cast(json_value(raw_json, '$.context_length') as int64)    as context_length,
        safe_cast(json_value(raw_json, '$.top_provider.max_completion_tokens') as int64)
                                                                        as max_completion_tokens,
        json_value(raw_json, '$.architecture.modality')                 as modality,
        json_value_array(raw_json, '$.architecture.input_modalities')   as input_modalities,
        json_value_array(raw_json, '$.architecture.output_modalities')  as output_modalities,
        json_value(raw_json, '$.architecture.tokenizer')                as tokenizer,
        safe_cast(json_value(raw_json, '$.top_provider.is_moderated') as bool)
                                                                        as is_moderated,

        -- prices arrive as text, in US dollars per token
        safe_cast(json_value(raw_json, '$.pricing.prompt') as bignumeric)      as prompt_price_per_token,
        safe_cast(json_value(raw_json, '$.pricing.completion') as bignumeric)  as completion_price_per_token,

        loaded_at

    from source

),

final as (

    select
        * except (prompt_price_per_token, completion_price_per_token),

        -- OpenRouter uses -1 for "price depends on routing": keep those as NULL
        case when prompt_price_per_token >= 0
             then round(prompt_price_per_token * 1000000, 6) end        as input_price_per_1m_tokens,
        case when completion_price_per_token >= 0
             then round(completion_price_per_token * 1000000, 6) end    as output_price_per_1m_tokens,

        coalesce(prompt_price_per_token < 0 or completion_price_per_token < 0, false)
                                                                        as is_variable_pricing,
        coalesce(prompt_price_per_token = 0 and completion_price_per_token = 0, false)
                                                                        as is_free

    from parsed

)

select * from final
