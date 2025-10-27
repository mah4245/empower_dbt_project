{{ config(
    materialized='table'
) }}

-- Step 1: Base invoice data
WITH base_invoice AS (
    SELECT DISTINCT
        INV.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID,
        INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID,
        COALESCE(INV.ORD_INV_ROOT_PROPOSAL_ID, INV.ORD_INV_INTERNAL_INVOICE_ID) AS PROPOSAL_ID,
        INV.ORD_INV_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID,
        INV.ORD_INV_TYPE AS INVOICE_TYPE,
        INV.ORD_INV_SERVICE_NAME AS SERVICE,
        INV.ORD_INV_REVENUE_CATEGORY AS SERVICE_CATEGORY,
        INV.ORD_INV_OWNER AS TEAM_MEMBER
    FROM {{ source('dd_dwh', 'ORDER_INVOICE') }} AS INV
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
        ON INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID = PO.PO_INTERNAL_PARTY_ID
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

-- Step 2: Map services
mapped_invoice AS (
    SELECT
        B.INVOICE_ID,
        B.STORE_ID,
        B.PROPOSAL_ID,
        B.LEAD_ID,
        B.INVOICE_TYPE,
        NULL AS CAMPAIGN_ID,
        S.SERVICE_ID,
        B.TEAM_MEMBER
    FROM base_invoice AS B
    LEFT JOIN {{ ref('d_services') }} AS S
        ON S.SERVICE = B.SERVICE
       AND S.SERVICE_CATEGORY = B.SERVICE_CATEGORY
),

-- Step 3: Filter out deletion events
filtered_invoice AS (
    SELECT M.*
    FROM mapped_invoice AS M
    LEFT JOIN {{ source('prod_bk', 'SM_DELETION_EVENTS') }} AS DE
        ON CAST(DE.ENTITYID AS INT) = M.INVOICE_ID
       AND DE.ENTITYTYPE = 3
    WHERE DE.ENTITYID IS NULL
),

-- Step 4: Filter out unapproved/voided
final_invoice AS (
    SELECT F.*
    FROM filtered_invoice AS F
    LEFT JOIN {{ ref('d_invoice_status') }} AS ST
        ON F.INVOICE_ID = ST.INVOICE_ID
       AND ST.END_DATE IS NULL
       AND ST.STATUS IN ('Unapproved', 'Voided')
    WHERE ST.INVOICE_ID IS NULL
)

SELECT * FROM final_invoice;
