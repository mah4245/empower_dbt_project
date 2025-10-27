{{ config(
    materialized='table'
) }}

-- Step 1: Base Appointment Data
WITH base_appointment AS (
    SELECT 
        CJ_INTERNAL_APPT_ID AS APPOINTMENT_ID,
        CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID,
        CJ_APPT_INVOICE_ID AS INVOICE_ID,
        CJ_APPT_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID,
        CJ_APPT_SERVICE AS SERVICE,
        ITM.ITM_REVENUE_CATEGORY AS SERVICE_CATEGORY,
        CJ_APPT_SERVICE_AGENT_ID AS SERVICE_AGENT_ID,
        CJ_APPT_RECURRING_TYPE AS APPOINTMENT_TYPE,
        CJ_APPT_PROPOSAL_ID AS PROPOSAL_ID
    FROM {{ source('dd_dwh', 'CUSTOMER_JOURNEY_APPOINTMENTS') }} AS APPT
    LEFT JOIN (
        SELECT *
        FROM {{ source('dd_dwh', 'PARTY_ORGANIZATION') }}
        WHERE PO_END_DATE IS NULL
    ) AS O
        ON APPT.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID = O.PO_INTERNAL_PARTY_ID
    LEFT JOIN (
        SELECT ITM_NAME, ITM_STORE_ID, ITM_REVENUE_CATEGORY
        FROM (
            SELECT
                ITM_NAME,
                ITM_STORE_ID,
                ITM_REVENUE_CATEGORY,
                ROW_NUMBER() OVER (
                    PARTITION BY ITM_NAME, ITM_STORE_ID
                    ORDER BY ITM_REVENUE_CATEGORY
                ) AS rn
            FROM {{ source('dd_dwh', 'ITEM') }}
            WHERE ITM_END_DATE IS NULL
              AND ITM_TYPE = 0
        ) t
        WHERE rn = 1
    ) AS ITM
        ON ITM.ITM_NAME = APPT.CJ_APPT_SERVICE
       AND ITM.ITM_STORE_ID = APPT.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE APPT.CJ_APPT_END_DATE IS NULL
      AND COALESCE(O.PO_INTERNAL_NAME, 'nan') NOT IN (
          'Download limit reached',
          'Outdoor Lighting Perspectives - Franchsing',
          'Outdoor Lighting Perspectives - Franchising',
          'Outdoor Lighting Perspectives - Holding',
          'Outdoor Lighting Perspectives - Hospitality',
          'Outdoor Lighting Perspectives - Special Projects'
      )
),

-- Step 2: Join with Proposals and Services
mapped_appointment AS (
    SELECT 
        A.APPOINTMENT_ID,
        A.STORE_ID,
        A.INVOICE_ID,
        A.PROPOSAL_ID,
        A.LEAD_ID,
        NULL AS CAMPAIGN_ID,
        S.SERVICE_ID,
        A.SERVICE_AGENT_ID,
        A.APPOINTMENT_TYPE
    FROM base_appointment AS A
    LEFT JOIN {{ ref('d_proposal') }} AS P
        ON P.PROPOSAL_ID = A.PROPOSAL_ID
    LEFT JOIN {{ ref('d_services') }} AS S
        ON A.SERVICE = S.SERVICE
       AND A.SERVICE_CATEGORY = S.SERVICE_CATEGORY
),

-- Step 3: Filter out deleted appointments
filtered_appointment AS (
    SELECT M.*
    FROM mapped_appointment AS M
    LEFT JOIN {{ source('prod_bk', 'SM_DELETION_EVENTS') }} AS DE
        ON CAST(DE.ENTITYID AS INT) = M.APPOINTMENT_ID
       AND DE.ENTITYTYPE = 1
    WHERE DE.ENTITYID IS NULL
)

SELECT * FROM filtered_appointment;
