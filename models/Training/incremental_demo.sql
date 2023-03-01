{{ config(materialized="incremental",unique_key = "d_date") }}
--{{ config(materialized="incremental",unique_key = "d_date",merge_update_columns = ['d_day_name'] )}}
--{{ config(materialized="incremental",unique_key = "d_date",incremental_strategy = 'append') }}
--{{ config(materialized="incremental",unique_key = "d_date",incremental_strategy = 'append',on_schema_change='sync_all_columns') }}

select 
A.* from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM A
WHERE A.D_DATE <= CURRENT_DATE -5

--select current_timestamp as isrt_ts,
--A.d_day_name from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM A
--WHERE A.D_DATE <= CURRENT_DATE -5

{% if is_incremental() %}
--and D_DATE > (select max(D_DATE) from {{this}})
and D_DATE > (select max(D_DATE)-1 from {{this}})
{% endif %}

