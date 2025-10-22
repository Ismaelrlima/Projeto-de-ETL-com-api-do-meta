from ..transform import run_etl_pipeline_leads
from ..load import load_data_to_db 

# Constantes
LEAD_TABLE = 'ads_lead_insights'
TOTAL_DAYS_HISTORIC = 1 # 6 meses de histórico


print(f'=== INICIANDO ETL: {LEAD_TABLE} (Leads Granular) ===')


try:
    # Esta função internamente fará as 2 extrações separadas e o Merge
    df_leads_final = run_etl_pipeline_leads(total_days=TOTAL_DAYS_HISTORIC)
    load_data_to_db(df_leads_final, table_name=LEAD_TABLE) 
    print(f'=== ETL {LEAD_TABLE} CONCLUÍDO COM SUCESSO ===')

except Exception as e:
    print(f'ERRO CRÍTICO NO FLUXO ETL {LEAD_TABLE}: {e}')
    exit(1)