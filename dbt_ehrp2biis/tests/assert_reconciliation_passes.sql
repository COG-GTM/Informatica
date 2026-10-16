-- Fails if any layer of the reconciliation report does not match its expected row count.
select *
from {{ ref('reconciliation_report') }}
where status = 'FAIL'
