from ..transform import run_etl_pipeline_campaigns
from ..load import load_data_to_db 

# Constantes para a tabela de Performance
CAMPAIGN_TABLE = 'ads_campaign_performance'
TOTAL_DAYS_HISTORIC = 1


print(f'=== INICIANDO ETL: {CAMPAIGN_TABLE} (Performance) ===')


try:
    df_campaign_final = run_etl_pipeline_campaigns(total_days=TOTAL_DAYS_HISTORIC)
    load_data_to_db(df_campaign_final, table_name=CAMPAIGN_TABLE) 
    print(f'=== ETL {CAMPAIGN_TABLE} CONCLUÍDO COM SUCESSO ===')

except Exception as e:
    print(f'ERRO CRÍTICO NO FLUXO ETL {CAMPAIGN_TABLE}: {e}')
    exit(1)