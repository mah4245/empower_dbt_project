{{ config(
    materialized='table'
) }}

WITH base_services AS (

    -- ITEM services
    SELECT DISTINCT
        ITM.ITM_REVENUE_CATEGORY AS SERVICE_CATEGORY,
        ITM.ITM_NAME AS SERVICE
    FROM {{ source('dd_dwh', 'ITEM') }} AS ITM
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
        ON ITM.ITM_STORE_ID = PO.PO_INTERNAL_PARTY_ID
    WHERE ITM.ITM_TYPE = 0
      AND COALESCE(PO.PO_INTERNAL_NAME, 'nan') NOT IN (
          'Outdoor Lighting Perspectives - Franchsing',
          'Outdoor Lighting Perspectives - Franchising',
          'Outdoor Lighting Perspectives - Holding',
          'Outdoor Lighting Perspectives - Hospitality',
          'Outdoor Lighting Perspectives - Special Projects'
      )

    UNION

    -- ORDER_INVOICE services
    SELECT DISTINCT
        INV.ORD_INV_REVENUE_CATEGORY AS SERVICE_CATEGORY,
        INV.ORD_INV_SERVICE_NAME AS SERVICE
    FROM {{ source('dd_dwh', 'ORDER_INVOICE') }} AS INV
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
        ON INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID = PO.PO_INTERNAL_PARTY_ID
    WHERE COALESCE(PO.PO_INTERNAL_NAME, 'nan') NOT IN (
          'Outdoor Lighting Perspectives - Franchsing',
          'Outdoor Lighting Perspectives - Franchising',
          'Outdoor Lighting Perspectives - Holding',
          'Outdoor Lighting Perspectives - Hospitality',
          'Outdoor Lighting Perspectives - Special Projects'
      )

    UNION

    -- CUSTOMER_JOURNEY_PROPOSALS services
    SELECT DISTINCT
        PROP.CJ_REVENUE_CATEGORY AS SERVICE_CATEGORY,
        PROP.CJ_SERVICE_NAME AS SERVICE
    FROM {{ source('dd_dwh', 'CUSTOMER_JOURNEY_PROPOSALS') }} AS PROP
    LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
        ON PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID = PO.PO_INTERNAL_PARTY_ID
    WHERE COALESCE(PO.PO_INTERNAL_NAME, 'nan') NOT IN (
          'Outdoor Lighting Perspectives - Franchsing',
          'Outdoor Lighting Perspectives - Franchising',
          'Outdoor Lighting Perspectives - Holding',
          'Outdoor Lighting Perspectives - Hospitality',
          'Outdoor Lighting Perspectives - Special Projects'
      )

)

SELECT
    ROW_NUMBER() OVER (ORDER BY SERVICE_CATEGORY, SERVICE) AS SERVICE_ID,
    SERVICE_CATEGORY,
    SERVICE
FROM base_services;
