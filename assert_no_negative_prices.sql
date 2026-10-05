-- Fails if any row comes back: no price should be below zero after cleaning.
select model_snapshot_id, input_price_per_1m_tokens, output_price_per_1m_tokens
from {{ ref('stg_openrouter__models') }}
where input_price_per_1m_tokens < 0
   or output_price_per_1m_tokens < 0
