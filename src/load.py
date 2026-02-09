

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import datetime

load_dotenv()
DB_DIALECT = os.getenv("DB_DIALECT")
DB_DRIVER = os.getenv("DB_DRIVER")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"{DB_DIALECT}+{DB_DRIVER}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    print(f"ERRO FATAL ao criar a conexão com o PostgreSQL: {e}")
    engine = None 
    

def load_data_to_db(df: pd.DataFrame, table_name: str):
    """Realiza o UPSERT (Merge) no PostgreSQL."""
    if df.empty or engine is None:
        print(f"[CARGA: {table_name}] DataFrame vazio ou conexão indisponível. Nenhuma ação no banco.")
        return

    print(f"[CARGA: {table_name}] Iniciando UPSERT de {len(df)} linhas...")

    try:
        
        for col in ['date_start', 'date_stop', 'created_time']:
            
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        
        temp_table = f'temp_{table_name}'
        
        
        df.to_sql(name=temp_table, con=engine, if_exists='replace', index=False, chunksize=5000)
        print(f"[CARGA: {table_name}] Dados inseridos na tabela temporária '{temp_table}'.")

        
        if table_name == 'ads_dimension':
            key_cols = ['ad_id']
        elif table_name == 'ads_campaign_performance':
            key_cols = ['date_start', 'ad_id']
        elif table_name == 'ads_lead_insights':
            
            key_cols = ['date_start', 'ad_id', 'age', 'gender', 'region']
        
        
        elif table_name == 'ads_raw_leads':
            key_cols = ['lead_id']

            
        else:
            raise ValueError(f"Tabela desconhecida: {table_name}. Defina as chaves primárias.")

        key_cols_sql = ', '.join(key_cols)
        update_cols = [col for col in df.columns if col not in key_cols]
        
        
        
        cols_for_insert = ', '.join([f'"{c}"' for c in df.columns])

        if table_name == 'ads_raw_leads':
            
            
            
            
            cols_for_select_safe = ', '.join([f'"{c}"' for c in df.columns if c != 'field_data'])
            
            
            field_data_cast = 'CASE WHEN "field_data" IS NULL THEN NULL ELSE "field_data"::JSONB END AS "field_data"'
            select_clause = f'{cols_for_select_safe}, {field_data_cast}'
            
            
            set_clause_list = []
            for col in update_cols:
                if col == 'field_data':
                    
                    set_clause_list.append(f'"{col}" = EXCLUDED."{col}"::JSONB')
                else:
                    set_clause_list.append(f'"{col}" = EXCLUDED."{col}"')
            set_clause = ', '.join(set_clause_list)

            
            upsert_query = f"""
                INSERT INTO {table_name} ({cols_for_insert})
                SELECT {select_clause} FROM {temp_table}
                ON CONFLICT ({key_cols_sql}) 
                DO UPDATE SET 
                    {set_clause};
            """
            
        else:
            
            
            cols_for_select = ', '.join([f'"{col}"' for col in df.columns])
            
            
            set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
            
            upsert_query = f"""
                INSERT INTO {table_name} ({cols_for_insert})
                SELECT {cols_for_select} FROM {temp_table}
                ON CONFLICT ({key_cols_sql}) 
                DO UPDATE SET 
                    {set_clause};
            """

        with engine.begin() as connection:
            connection.execute(text(upsert_query))
        
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE {temp_table}"))

        print(f"[CARGA: {table_name}] Concluída com sucesso.")

    except Exception as e:
        print(f'[CARGA: {table_name}] ERRO FATAL ao salvar no banco: {e}')
        
        try:
            with engine.begin() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        except:
            pass