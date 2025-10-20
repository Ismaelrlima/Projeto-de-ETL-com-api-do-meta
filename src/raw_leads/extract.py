# ARQUIVO: src/raw_leads/extract.py

import os
import datetime
import pandas as pd
import requests 
from dotenv import load_dotenv

load_dotenv()
ACCESS_TOKEN = os.getenv('LEADS_ACCESS_TOKEN') # Token de Página
PAGE_ID = os.getenv('LEADS_PAGE_ID') # ID da Página

# Versão da API que será usada na URL base
API_VERSION = 'v18.0' 
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

def get_raw_leads_data(total_days: int = 182) -> pd.DataFrame:
    """Extrai dados brutos de leads usando chamadas diretas (requests) através do ID da Página."""
    
    if not all([ACCESS_TOKEN, PAGE_ID]):
        print('ERRO: Certifique-se de que ACCESS_TOKEN e LEADS_PAGE_ID estão configurados no .env.')
        return pd.DataFrame() 
        
    today = datetime.date.today()
    
    # O final da busca é o dia atual (hoje) para incluir leads recentes.
    date_end = today
    date_start = today - datetime.timedelta(days=total_days)
    
    print(f"\n[EXTRAÇÃO: Leads Brutos] Iniciando extração de {total_days} dias (De {date_start} até {date_end})...")
        
    all_leads = []
    
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    try:
        # 1. BUSCAR TODOS OS FORMULÁRIOS DA PÁGINA
        forms_url = f"{BASE_URL}/{PAGE_ID}/leadgen_forms"
        
        forms_params = {
            'fields': 'id', 
            'limit': 100 # Manter em 100 é seguro para a API
        }
        
        response = requests.get(forms_url, params=forms_params, headers=headers)
        response.raise_for_status() 
        
        forms_data = response.json().get('data', [])
        
        if not forms_data:
            print(f"[EXTRAÇÃO: Leads Brutos] NENHUM formulário encontrado na Página {PAGE_ID}.")
            return pd.DataFrame()

        # 2. ITERAR POR CADA FORMULÁRIO E BUSCAR SEUS LEADS (COM PAGINAÇÃO)
        for form_obj in forms_data:
            form_id = form_obj.get('id')
            
            leads_url = f"{BASE_URL}/{form_id}/leads"
            
            lead_fields = [
                'id', 'created_time', 'ad_id', 'campaign_id', 'adset_id',
                'form_id', 'field_data', 'ad_platform_data' 
            ]
            
            leads_params = {
                'fields': ','.join(lead_fields),
                'time_range': {'since': date_start.strftime('%Y-%m-%d'), 'until': date_end.strftime('%Y-%m-%d')}, 
                'limit': 100,
            }
            
            # 🚨 INÍCIO DA PAGINAÇÃO: next_url é a variável de controle do loop
            next_url = leads_url
            
            while next_url:
                if next_url == leads_url:
                    # Primeira chamada: usa URL base, params e headers
                    response = requests.get(leads_url, params=leads_params, headers=headers)
                else:
                    # Chamadas subsequentes: usa a URL 'next' completa
                    response = requests.get(next_url, headers=headers)
                    
                response.raise_for_status()

                response_data = response.json()
                
                # 3. EXTRAIR OS DADOS DA PÁGINA ATUAL
                for lead_data in response_data.get('data', []):
                    lead_data['lead_id'] = lead_data.pop('id') 
                    lead_data['form_id'] = form_id 
                    all_leads.append(lead_data)
                    
                # 4. Verificar paginação
                paging = response_data.get('paging', {})
                next_url = paging.get('next') 
                
                if next_url:
                    # Notificação visual de que o código está buscando mais dados
                    print(f"    Página de leads do formulário {form_id} carregada. Total atual: {len(all_leads)}. Buscando próxima...")
                else:
                    next_url = None 
            
        print(f"  --> Formulário {form_id} concluído.")


        print(f"[EXTRAÇÃO: Leads Brutos] Extraídos {len(all_leads)} leads de {len(forms_data)} formulários.")
        return pd.DataFrame(all_leads)

    except requests.exceptions.HTTPError as e:
        print(f'[EXTRAÇÃO: Leads Brutos] Erro HTTP ao tentar acesso: {e}')
        print(f'Resposta da API: {e.response.text}')
        return pd.DataFrame()
        
    except Exception as e:
        print(f'[EXTRAÇÃO: Leads Brutos] Erro fatal inesperado: {e}')
        return pd.DataFrame()