from ..load import load_data_to_db
from .extract import get_raw_leads_data 
from .transform import transform_raw_leads_data

def run_raw_leads_etl():
    """Pipeline focado apenas na extração, transformação e carga de Leads Brutos (ads_raw_leads)."""
    
    TABLE_NAME = 'ads_raw_leads'
    
    print(f"\n=== INICIANDO ETL PARA {TABLE_NAME} ===")
    
    # 1. EXTRAÇÃO
    df_raw_leads = get_raw_leads_data(total_days=182)

    if df_raw_leads.empty:
        print(f"[CARGA: {TABLE_NAME}] DataFrame vazio. Nenhuma ação no banco.")
    else:
        # 2. TRANSFORMAÇÃO
        df_transformed = transform_raw_leads_data(df_raw_leads) 

        # 3. CARGA (Chamando a função com o nome correto)
        load_data_to_db(df_transformed, table_name=TABLE_NAME) 
    
    print(f"=== ETL {TABLE_NAME} CONCLUÍDO COM SUCESSO ===")

if __name__ == "__main__":
    run_raw_leads_etl()