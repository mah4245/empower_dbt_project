{{ config(
    materialized='table'   
) }}

WITH base_proposals AS (

    SELECT DISTINCT
        PROP.CJ_INTERNAL_PROP_ID AS PROPOSAL_ID,
        PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID,
        PROP.CJ_PROP_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID,
        PROP.CJ_SERVICE_NAME AS SERVICE,
        PROP.CJ_REVENUE_CATEGORY AS REVENUE_CATEGORY,
        PROP.CJ_PROP_TYPE AS TYPE,
        PROP.CJ_PROP_TITLE AS PROPOSAL_TITLE,
        PROP.CJ_PROP_OWNER AS TEAM_MEMBER
    FROM {{ source('dd_dwh', 'CUSTOMER_JOURNEY_PROPOSALS') }} AS PROP
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE COALESCE(PO.PO_INTERNAL_NAME, 'nan') NOT IN (
        'Download limit reached',
        'Outdoor Lighting Perspectives - Franchsing',
        'Outdoor Lighting Perspectives - Franchising',
        'Outdoor Lighting Perspectives - Holding',
        'Outdoor Lighting Perspectives - Hospitality',
        'Outdoor Lighting Perspectives - Special Projects'
    )
    AND PROP.CJ_PROP_END_DATE IS NULL
),

final_proposals AS (

    SELECT
        P.PROPOSAL_ID,
        P.STORE_ID,
        P.LEAD_ID,
        S.SERVICE_ID,
        L.CAMPAIGN_ID,
        P.TYPE,
        P.PROPOSAL_TITLE,
        P.TEAM_MEMBER
    FROM base_proposals P
    LEFT JOIN {{ ref('d_services') }} AS S
        ON S.SERVICE = P.SERVICE
        AND S.SERVICE_CATEGORY = P.REVENUE_CATEGORY
    LEFT JOIN {{ ref('d_leads') }} AS L
        ON L.LEAD_ID = P.LEAD_ID
)

SELECT *
FROM final_proposals
WHERE PROPOSAL_ID NOT IN (
    SELECT CAST(ENTITYID AS INT)
    FROM {{ source('prod_bk', 'SM_DELETION_EVENTS') }}
    WHERE ENTITYTYPE = 2
)
