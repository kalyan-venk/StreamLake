select
    merchant,
    category,
    txns,
    fraud_txns,
    total_amt,
    fraud_rate
from {{ source('lakehouse', 'merchant_risk_leaderboard') }}
