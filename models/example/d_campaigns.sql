{{ config(
    materialized='table'
) }}

WITH base_campaigns AS (

    SELECT 
        PC.CAM_PARTY_ORGANIZATION_ID AS STORE_ID,
        PC.CAM_CHANNEL_NAME AS CHANNEL,
        PC.CAM_CAMPAIGN_NAME AS CAMPAIGN
    FROM {{ source('dd_dwh', 'CAMPAIGN') }} AS PC
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS O
        ON PC.CAM_PARTY_ORGANIZATION_ID = O.PO_INTERNAL_PARTY_ID
    WHERE
        PC.CAM_SOURCE = 'CONTACTS'
        AND PC.CAM_PARTY_ORGANIZATION_ID IS NOT NULL
        AND O.PO_END_DATE IS NULL
        AND COALESCE(O.PO_INTERNAL_NAME, 'nan') NOT IN (
            'Download limit reached',
            'Outdoor Lighting Perspectives - Franchsing',
            'Outdoor Lighting Perspectives - Franchising',
            'Outdoor Lighting Perspectives - Holding',
            'Outdoor Lighting Perspectives - Hospitality',
            'Outdoor Lighting Perspectives - Special Projects'
        )
    GROUP BY
        PC.CAM_PARTY_ORGANIZATION_ID,
        PC.CAM_CAMPAIGN_NAME,
        PC.CAM_CHANNEL_NAME
)

SELECT
    ROW_NUMBER() OVER (ORDER BY STORE_ID, CAMPAIGN, CHANNEL) AS CAMPAIGN_ID,
    STORE_ID,
    CHANNEL,
    CAMPAIGN
FROM base_campaigns;
