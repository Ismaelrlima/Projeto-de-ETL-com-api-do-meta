from ..transform import run_etl_pipeline_dim
from ..load import load_data_to_db 

# Constantes
DIM_TABLE = 'ads_dimension'
TOTAL_DAYS_DIM = 182 #total de dias

print(f'=== INICIANDO ETL: {DIM_TABLE} (Dimensão) ===')


try:
    df_dim = run_etl_pipeline_dim(total_days=TOTAL_DAYS_DIM) 
    load_data_to_db(df_dim, table_name=DIM_TABLE)
    print(f'=== ETL {DIM_TABLE} CONCLUÍDO COM SUCESSO ===')
    
except Exception as e:
    print(f'ERRO CRÍTICO NO FLUXO ETL {DIM_TABLE}: {e}')
    # Saída com código de erro, útil para orquestradores como o Airflow
    exit(1)