{{ config(
    materialized='table'
) }}

WITH party_contacts AS (
    SELECT DISTINCT
        PC.PC_INTERNAL_PARTY_ID AS LEAD_ID,
        PC.PC_NAME AS LEAD_NAME,
        PC.PC_SERVICE_CITY AS SERVICE_CITY,
        PC.PC_SERVICE_STATE AS SERVICE_STATE,
        PC.PC_SERVICE_POSTAL_CODE AS SERVICE_POSTAL_CODE,
        NULLIF(PC.PC_LONGITUDE, 0) AS LONGITUDE,
        NULLIF(PC.PC_LATITUDE, 0) AS LATITUDE,
        PC.PC_CATEGORY AS LEAD_CATEGORY,
        PC.PC_CREATED_AT AS LEAD_DATE,
        CAM.CAMPAIGN_ID,
        PC.PC_ORGANIZATION_ID
    FROM {{ source('dd_dwh', 'PARTY_CONTACT') }} AS PC

    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = PC.PC_ORGANIZATION_ID
        AND PO.PO_END_DATE IS NULL

    LEFT JOIN {{ ref('d_campaigns') }} AS CAM
        ON CAM.CAMPAIGN = PC.PC_CAMPAIGN
        AND CAM.CHANNEL = PC.PC_CHANNEL
        AND CAM.STORE_ID = PC.PC_ORGANIZATION_ID

    WHERE 
        COALESCE(PO.PO_INTERNAL_NAME, 'nan') NOT IN (
            'Download limit reached',
            'Outdoor Lighting Perspectives - Franchsing',
            'Outdoor Lighting Perspectives - Franchising',
            'Outdoor Lighting Perspectives - Holding',
            'Outdoor Lighting Perspectives - Hospitality',
            'Outdoor Lighting Perspectives - Special Projects'
        )
        AND PC.PC_END_DATE IS NULL
),

deletion_events AS (
    SELECT CAST(ENTITYID AS INT) AS LEAD_ID
    FROM {{ source('prod_bk', 'SM_DELETION_EVENTS') }}
    WHERE ENTITYTYPE = 0
),

filtered_leads AS (
    SELECT *
    FROM party_contacts
    WHERE LEAD_ID NOT IN (SELECT LEAD_ID FROM deletion_events)
)

SELECT
    LEAD_ID,
    LEAD_NAME,
    SERVICE_CITY,
    SERVICE_STATE,
    SERVICE_POSTAL_CODE,
    LONGITUDE,
    LATITUDE,
    LEAD_CATEGORY,
    LEAD_DATE,
    CAMPAIGN_ID,
    PC_ORGANIZATION_ID
FROM filtered_leads;
