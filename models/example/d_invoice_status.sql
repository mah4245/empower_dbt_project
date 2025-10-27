{{ config(
    materialized='incremental',
    unique_key='INVOICE_ID' 
) }}

WITH latest_records AS (
    SELECT
        I.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID,
        I.ORD_INV_STATUS AS STATUS,
        I.ORD_INV_START_DATE AS START_DATE,
        I.ORD_INV_END_DATE AS END_DATE
    FROM {{ source('dd_dwh', 'ORDER_INVOICE') }} AS I
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS O
        ON I.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID = O.PO_INTERNAL_PARTY_ID
    WHERE I.ORD_INV_END_DATE IS NULL
      AND COALESCE(O.PO_INTERNAL_NAME, 'nan') NOT IN (
          'Download limit reached',
          'Outdoor Lighting Perspectives - Franchsing',
          'Outdoor Lighting Perspectives - Franchising',
          'Outdoor Lighting Perspectives - Holding',
          'Outdoor Lighting Perspectives - Hospitality',
          'Outdoor Lighting Perspectives - Special Projects'
      )
),

remove_deleted AS (
    SELECT *
    FROM latest_records
    WHERE INVOICE_ID NOT IN (
        SELECT CAST(ENTITYID AS INT)
        FROM {{ source('prod_bk', 'SM_DELETION_EVENTS') }}
        WHERE ENTITYTYPE = 3
    )
)

SELECT *
FROM remove_deleted

{% if is_incremental() %}
WHERE INVOICE_ID NOT IN (SELECT INVOICE_ID FROM {{ this }})
{% endif %}
