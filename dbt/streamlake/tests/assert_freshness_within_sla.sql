-- The freshness SLA as a test, not just a dashboard tile: if any curated table's newest record
-- is more than three days behind the end of the loaded period, fail the build.
select table_name, watermark_ts, lag_hours, freshness_status
from {{ ref('mart_data_freshness') }}
where freshness_status = 'breached'
