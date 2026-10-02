USE bank_dw;

-- ============================================================
-- 1. Subscription Rate by Job
-- ============================================================

SELECT
    c.job,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
JOIN dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY c.job
ORDER BY subscription_rate DESC;


-- ============================================================
-- 2. Subscription Rate by Education
-- ============================================================

SELECT
    c.education,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
JOIN dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY c.education
ORDER BY subscription_rate DESC;


-- ============================================================
-- 3. Subscription by Contact Type
-- ============================================================

SELECT
    ct.contact_type,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
JOIN dim_contact ct
    ON f.contact_key = ct.contact_key
GROUP BY ct.contact_type
ORDER BY subscription_rate DESC;

-- ============================================================
-- 4. Average Call Duration by Subscription Status
-- ============================================================

SELECT
    f.subscribed,
    COUNT(*) AS total_contacts,
    ROUND(AVG(f.duration), 2) AS average_duration
FROM fact_campaign f
GROUP BY f.subscribed
ORDER BY f.subscribed;


-- ============================================================
-- 5. Subscription Rate by Month
-- ============================================================

SELECT
    f.month,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
GROUP BY f.month
ORDER BY subscription_rate DESC;


-- ============================================================
-- 6. Previous Campaign Outcome vs Subscription
-- ============================================================

SELECT
    c.previous_outcome,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
JOIN dim_campaign c
    ON f.campaign_key = c.campaign_key
GROUP BY c.previous_outcome
ORDER BY subscription_rate DESC;

-- ============================================================
-- 7. Number of Campaign Contacts vs Subscription
-- ============================================================

SELECT
    f.campaign,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
GROUP BY f.campaign
ORDER BY f.campaign;


-- ============================================================
-- 8. Subscription Rate by Marital Status
-- ============================================================

SELECT
    c.marital,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
JOIN dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY c.marital
ORDER BY subscription_rate DESC;


-- ============================================================
-- 9. Subscription Rate by Housing Loan
-- ============================================================

SELECT
    c.housing,
    c.loan,
    COUNT(*) AS total_contacts,
    SUM(f.subscribed) AS subscriptions,
    ROUND(100.0 * SUM(f.subscribed) / COUNT(*), 2) AS subscription_rate
FROM fact_campaign f
JOIN dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY c.housing, c.loan
ORDER BY subscription_rate DESC;


-- ============================================================
-- 10. Overall Campaign Performance
-- ============================================================

SELECT
    COUNT(*) AS total_contacts,
    SUM(subscribed) AS total_subscriptions,
    ROUND(100.0 * SUM(subscribed) / COUNT(*), 2) AS overall_subscription_rate,
    ROUND(AVG(duration), 2) AS average_call_duration,
    ROUND(AVG(campaign), 2) AS average_campaign_contacts
FROM fact_campaign;
