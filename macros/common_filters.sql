-- Filter out unwanted organization names
{% macro exclude_invalid_organizations(column_name='PO.PO_INTERNAL_NAME') %}
COALESCE({{ column_name }}, 'nan') NOT IN (
    'Download limit reached',
    'Outdoor Lighting Perspectives - Franchsing',
    'Outdoor Lighting Perspectives - Franchising',
    'Outdoor Lighting Perspectives - Holding',
    'Outdoor Lighting Perspectives - Hospitality',
    'Outdoor Lighting Perspectives - Special Projects'
)
{% endmacro %}


-- Join to active PARTY_ORGANIZATION
{% macro join_active_party_org(table_alias, join_column) %}
LEFT JOIN {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} AS PO
    ON PO.PO_INTERNAL_PARTY_ID = {{ table_alias }}.{{ join_column }}
    AND PO.PO_END_DATE IS NULL
{% endmacro %}


-- Exclude deleted records from SM_DELETION_EVENTS
{% macro exclude_deleted_records(entity_id_column, entity_type) %}
{{ entity_id_column }} NOT IN (
    SELECT CAST(ENTITYID AS INT)
    FROM {{ source('prod_bk', 'SM_DELETION_EVENTS') }}
    WHERE ENTITYTYPE = {{ entity_type }}
)
{% endmacro %}
