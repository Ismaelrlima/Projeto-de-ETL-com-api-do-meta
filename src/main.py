from transform import run_etl_pipeline_campaigns, run_etl_pipeline_leads, run_etl_pipeline_dim
from load import load_data_to_db 

if __name__ == '__main__':
    
    # Nomes das tabelas de destino
    DIM_TABLE = 'ads_dimension'
    CAMPAIGN_TABLE = 'ads_campaign_performance'
    LEAD_TABLE = 'ads_lead_insights'
    
    TOTAL_DAYS_HISTORIC = 1
    TOTAL_DAYS_DIM = 1       
    

    print('=== INICIANDO FLUXO ===')
    
    try:
        #CARGA DA DIMENSÃO (NOMES) - Extração mais leve
        print("\n 1. ETL: DIMENSÃO (NOMES E IDs) ")
        df_dim = run_etl_pipeline_dim(total_days=TOTAL_DAYS_DIM) 
        load_data_to_db(df_dim, table_name=DIM_TABLE)

        #CARGA DA PERFORMANCE (AGREGADA) - Extração separada
        print("\n 2. ETL: PERFORMANCE (AGREGADA) ")
        df_campaign_final = run_etl_pipeline_campaigns(total_days=TOTAL_DAYS_HISTORIC)
        load_data_to_db(df_campaign_final, table_name=CAMPAIGN_TABLE) 
        
        #CARGA DOS LEADS (GRANULAR) - Duas extrações separadas e combinadas
        print("\n 3. ETL: LEADS (ALTA GRANULARIDADE) ")
        df_leads_final = run_etl_pipeline_leads(total_days=TOTAL_DAYS_HISTORIC)
        load_data_to_db(df_leads_final, table_name=LEAD_TABLE) 
        
    except Exception as e:
        print(f'ERRO CRÍTICO NO FLUXO DE ORQUESTRAÇÃO ETL: {e}')

    print('\n FLUXO DE ORQUESTRAÇÃO ETL CONCLUÍDO \n')