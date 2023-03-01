{{ config(materialized="incremental",unique_key = "d_date") }}
select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM
WHERE D_DATE <= CURRENT_DATE -5

{% if is_incremental() %}
and D_DATE > (select max(D_DATE) from {{this}})
{% endif %}