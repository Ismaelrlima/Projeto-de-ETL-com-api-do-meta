import pandas as pd
import numpy as np
import json 
from typing import Dict, Any, List

# Função auxiliar para extrair um valor específico de um campo aninhado
def get_field_value(field_data: List[Dict[str, Any]], field_name: str) -> str | None:
    """Busca o valor de um campo específico na lista de dicionários field_data."""
    if not isinstance(field_data, list):
        return None
    for field in field_data:
        if field.get('name') == field_name:
            # Retorna o primeiro valor encontrado
            value = field.get('values', [None])[0]
            
            # Garante que o valor retornado seja uma string (ou None) e não um objeto complexo.
            return str(value) if value is not None else None 
    return None


def transform_raw_leads_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza transformações na base bruta de leads, incluindo:
    1. Extração de campos aninhados (contato, localidade, gênero) do field_data.
    2. Serialização do field_data para JSON string.
    3. Conversão de tipos de dados.
    """
    
    if df_raw.empty:
        return df_raw
    
    # Extração de Campos de Contato (vistos no seu log)
    df_raw['full_name'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'full_name'))
    df_raw['email'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'email'))
    df_raw['phone_number'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'phone_number'))
    
    # Extração de Localidade (visto no seu log)
    df_raw['city'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'city'))
    df_raw['state'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'state'))
    
    # Tentativa de Gênero (campo 'gender' se existir no formulário ou na API)
    df_raw['gender'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'gender'))
    
    # Extração de outros campos personalizados que podem ser úteis (vistos no seu log)
    df_raw['loja'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'LOJA'))
    df_raw['tipo'] = df_raw['field_data'].apply(lambda x: get_field_value(x, 'TIPO'))


    # TRATAMENTO DE VALORES NULOS E TIPOS DE DADOS PARA O POSTGRES
    # O PostgreSQL (via psycopg2) tem problemas com o objeto None em colunas VARCHAR/TEXT.
    # Convertemos a coluna para string (object) e depois substituímos o 'None' string.
    
    for col in ['full_name', 'email', 'phone_number', 'city', 'state', 'gender', 'loja', 'tipo']:
        # Converte para string (safeguard) e depois substitui o valor 'None' para None/NaN,
        # permitindo que o SQLAlchemy insira NULL no banco.
        if col in df_raw.columns:
            # Esta linha garante que o Pandas envie um tipo que o SQLAlchemy consiga mapear para NULL
            df_raw[col] = df_raw[col].replace('None', np.nan).replace('', np.nan).astype(object)

    # Converte 'created_time' para datetime e depois para date
    df_raw['created_time'] = pd.to_datetime(df_raw['created_time'], errors='coerce').dt.date

    # SERIALIZAÇÃO DO FIELD_DATA (Resolve o erro 'can't adapt type dict')
    # O PostgreSQL precisa que o JSON seja enviado como string (ou o tipo JSONB).
    try:
        df_raw['field_data'] = df_raw['field_data'].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else None
        )
    except Exception as e:
        print(f"Aviso: Falha ao serializar field_data para JSON. Erro: {e}")
        df_raw['field_data'] = None # Define como None se a serialização falhar

    # LIMPEZA E SELEÇÃO FINAL
    df_raw = df_raw.rename(columns={'id': 'lead_id'})

    # Lista de colunas que devem estar no DataFrame final e no seu SQL
    final_cols = [
        'lead_id', 'created_time', 'ad_id', 'adset_id', 'campaign_id', 'form_id', 
        'full_name', 'email', 'phone_number', 
        'city', 'state', 'gender', # Campos Geográficos e Gênero
        'loja', 'tipo', # Campos personalizados
        'field_data', # Campo JSON BRUTO
    ]
    
    # Garante que apenas as colunas válidas no DataFrame final sejam selecionadas
    cols_to_select = [col for col in final_cols if col in df_raw.columns]
    
    return df_raw[cols_to_select]