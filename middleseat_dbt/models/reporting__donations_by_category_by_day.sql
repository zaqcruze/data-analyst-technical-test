{{
    config(
        dist='wdl_client_code',
        sort=['wdl_client_code', 'likely_source_type']
    )
}}

SELECT
    wdl_client_code,
    CAST(et_created_at AS DATE) AS et_created_date,
    DATE_TRUNC('month', et_created_at) AS et_created_month,
    COALESCE(likely_source_type, 'None') AS likely_source_type,
    form_managing_entity_committee_name,
    committee_name,
    COALESCE(recurring_type, 'None') AS recurring_type,

    SUM(amount) AS gross_dollars_raised,
    SUM(post_refund_amount) AS net_dollars_raised,
    SUM(amount - post_refund_amount) AS refunded_amount,

    COUNT(DISTINCT wdl_transaction_id) AS number_of_donations,
    COUNT(DISTINCT email) AS unique_donors,
    AVG(post_refund_amount) AS avg_contribution,
    MAX(post_refund_amount) AS largest_single_donation,

    SUM(CASE WHEN COALESCE(recurring_type, 'None') IN ('New', 'Existing') THEN 1 ELSE 0 END) AS recurring_donation_count,
    SUM(CASE WHEN COALESCE(recurring_type, 'None') IN ('New', 'Existing') THEN post_refund_amount ELSE 0 END) AS recurring_dollars_raised,

    SUM(CASE WHEN COALESCE(recurring_type, 'None') = 'New' THEN 1 ELSE 0 END) AS new_recurring_donation_count,
    SUM(CASE WHEN COALESCE(recurring_type, 'None') = 'New' THEN post_refund_amount ELSE 0 END) AS new_recurring_dollars_raised,

    SUM(CASE WHEN COALESCE(recurring_type, 'None') = 'Existing' THEN 1 ELSE 0 END) AS existing_recurring_donation_count,
    SUM(CASE WHEN COALESCE(recurring_type, 'None') = 'Existing' THEN post_refund_amount ELSE 0 END) AS existing_recurring_dollars_raised

    -- Stretch goal: count maxed-out donors if contribution limit rules and a trusted donor-level rollup are added.
FROM {{ ref('core__donations') }}
GROUP BY
    wdl_client_code,
    et_created_date,
    et_created_month,
    likely_source_type,
    form_managing_entity_committee_name,
    committee_name,
    recurring_type
ORDER BY
    wdl_client_code,
    et_created_date DESC,
    likely_source_type
