{{ config(
    materialized='table'
) }}

-- Step 1: Join Leads with Order Invoice
WITH customer_base AS (
    SELECT 
        L.LEAD_ID AS CUSTOMER_ID,
        L.LEAD_NAME AS CUSTOMER_NAME,
        L.SERVICE_CITY,
        L.SERVICE_STATE,
        L.SERVICE_POSTAL_CODE,
        L.LONGITUDE,
        L.LATITUDE,
        L.LEAD_CATEGORY AS CUSTOMER_CATEGORY,
        MIN(CAST(I.ORD_INV_INVOICE_CREATED AS DATE)) AS CUSTOMER_START_DATE,
        L.CAMPAIGN_ID,
        L.STORE_ID
    FROM {{ ref('d_leads') }} AS L
    INNER JOIN {{ source('dd_dwh', 'ORDER_INVOICE') }} AS I
        ON CAST(I.ORD_INV_INTERNAL_CUSTOMER_PARTY_ID AS FLOAT) = L.LEAD_ID
    GROUP BY 
        L.LEAD_ID, 
        L.LEAD_NAME, 
        L.SERVICE_CITY, 
        L.SERVICE_STATE, 
        L.SERVICE_POSTAL_CODE, 
        L.LONGITUDE, 
        L.LATITUDE, 
        L.LEAD_CATEGORY, 
        L.CAMPAIGN_ID, 
        L.STORE_ID
)

SELECT * FROM customer_base;
