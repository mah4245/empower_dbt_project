{{ config(
    materialized='incremental',
    unique_key='INVOICE_ID'  
) }}

WITH base_invoices AS (
    SELECT DISTINCT
        INV.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID,
        NULLIF(INV.ORD_INV_INVOICE_CREATED, '1900-01-01') AS CREATE_DATE,
        NULLIF(INV.ORD_INV_INVOICE_DATE, '1900-01-01') AS INVOICE_DATE,
        INV.ORD_INV_TOTAL_AMOUNT AS TOTAL,
        INV.ORD_INV_BALANCE_DUE AS BALANCE_DUE,
        INV.ORD_INV_SUBTOTAL AS SUBTOTAL,
        INV.ORD_INV_TAX_TOTAL AS TAX_TOTAL,
        INV.ORD_INV_QUANTITY AS QUANTITY,
        INV.ORD_INV_GROSS_SUBTOTAL AS GROSS_SUBTOTAL
    FROM {{ source('dd_dwh', 'ORDER_INVOICE') }} AS INV
    LEFT JOIN (
        SELECT *
        FROM {{ source('dd_dwh', 'PARTY_ORGANIZATION') }}
        WHERE PO_END_DATE IS NULL
    ) AS PO
        ON PO.PO_INTERNAL_PARTY_ID = INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE INV.ORD_INV_END_DATE IS NULL
      AND COALESCE(PO.PO_INTERNAL_NAME, 'nan') NOT IN (
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
    FROM base_invoices
    WHERE INVOICE_ID NOT IN (
        SELECT CAST(ENTITYID AS INT)
        FROM {{ source('prod_bk', 'SM_DELETION_EVENTS') }}
        WHERE ENTITYTYPE = 3
    )
),

remove_unapproved_or_voided AS (
    SELECT *
    FROM remove_deleted
    WHERE INVOICE_ID NOT IN (
        SELECT INVOICE_ID
        FROM {{ ref('d_invoice_status') }}
        WHERE END_DATE IS NULL
          AND STATUS IN ('Unapproved', 'Voided')
    )
)

SELECT *
FROM remove_unapproved_or_voided

{% if is_incremental() %}
-- only insert new records on incremental runs
WHERE INVOICE_ID NOT IN (SELECT INVOICE_ID FROM {{ this }})
{% endif %}
