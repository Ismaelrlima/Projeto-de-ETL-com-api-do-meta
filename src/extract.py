import os
import datetime
import pandas as pd
from dotenv import load_dotenv

# Importações do SDK do Meta Ads (Facebook Business)
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.leadgenform import LeadgenForm
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

load_dotenv()

# Credenciais lidas do .env (LEADS_*)
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")


# FUNÇÕES AUXILIARES DE CONEXÃO E CONFIGURAÇÃO


def _init_api_and_get_timerange(total_days: int) -> tuple:
    """Inicializa a API do Meta e calcula o time_range para extrações."""
    if not all([APP_ID, APP_SECRET, ACCESS_TOKEN, AD_ACCOUNT_ID]):
        print('ERRO: Credenciais do Meta Ads (LEADS_) não encontradas no .env.')
        return None, None
    
    # Inicializa a API com as credenciais
    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
    api = FacebookAdsApi.get_default_api()

    # Calcula o intervalo de tempo para a extração
    today = datetime.date.today()
    # Extração de D-1 para D-N
    date_end = today - datetime.timedelta(days=1)
    date_start = date_end - datetime.timedelta(days=total_days)
    time_range = {
        'since': date_start.strftime('%Y-%m-%d'),
        'until': date_end.strftime('%Y-%m-%d')
    }

    # Formata o ID da conta de anúncios
    account_id_clean = AD_ACCOUNT_ID.replace("act_", "")
    formatted_account_id = f"act_{account_id_clean}"
    account = AdAccount(formatted_account_id, api=api)
    
    return account, time_range


# 1. EXTRAÇÃO DE LEADS BRUTOS (ads_raw_leads)


def get_raw_leads_data(total_days: int = 182) -> pd.DataFrame:
    """Extrai dados brutos de leads via API do Facebook."""
    account, time_range = _init_api_and_get_timerange(total_days)
    if account is None: return pd.DataFrame()

    print(f"\n[EXTRAÇÃO: Leads Brutos] Iniciando extração de {total_days} dias...")
    all_leads = []
    
    try:
        # Pega todos os formulários da conta
        forms_cursor = account.get_lead_gen_forms(
            fields=['id'],
            params={'limit': 100}
        )
        forms_data = [form.export_all_data() for form in forms_cursor]

        for form_obj in forms_data:
            form_id = form_obj.get('id')

            lead_fields = [
                'id', 'created_time', 'ad_id', 'campaign_id', 'adset_id',
                'form_id', 'field_data', 'ad_platform_data'
            ]

            # Pega os leads de cada formulário
            leads_cursor = LeadgenForm(form_id, api=account.api).get_leads(
                fields=lead_fields,
                params={'time_range': time_range, 'limit': 100}
            )

            for lead in leads_cursor:
                lead_data = lead.export_all_data()
                lead_data['lead_id'] = lead_data.pop('id', None)
                all_leads.append(lead_data)

        print(f"[EXTRAÇÃO: Leads Brutos] Extraídos {len(all_leads)} leads de {len(forms_data)} formulários.")
        return pd.DataFrame(all_leads)

    except Exception as e:
        print(f'[EXTRAÇÃO: Leads Brutos] Erro fatal na extração: {e}')
        return pd.DataFrame()


# 2. EXTRAÇÃO DE DIMENSÃO (ads_dimension)


def get_name_dim_raw(total_days: int = 1) -> pd.DataFrame:
    """Extrai IDs e Nomes para a tabela de Dimensão (ads_dimension) usando o endpoint /ads."""
       
    account, _ = _init_api_and_get_timerange(total_days)
    if account is None: return pd.DataFrame()
    
    print(f"\n[EXTRAÇÃO: Dimensão (Nomes)] Iniciando extração de IDs e Nomes...")

    try:
        # Pedindo nomes de níveis superiores com sintaxe de objeto aninhado ({name})
        ad_fields = [
            'id', 'name', 
            'adset_id', 
            'adset{name}',  
            'campaign_id', 
            'campaign{name}',
        ]
        
        params = {
            'filtering': [
                {'field': 'ad.effective_status', 'operator': 'IN', 'value': ['ACTIVE', 'PAUSED', 'PENDING_REVIEW']},
            ],
            'limit': 1000
        }
        
        ads_cursor = account.get_ads(
            fields=ad_fields,
            params=params
        )
        
        data = [ad.export_all_data() for ad in ads_cursor]
        df = pd.DataFrame(data)
        
        if not df.empty:
            # 1. Renomear o ID e Nome do AD
            df = df.rename(columns={'id': 'ad_id', 'name': 'ad_name'}, errors='ignore')
            
            # 2. Desaninha e Renomeia os campos de Adset e Campaign
            # A API retorna 'adset' e 'campaign' como dicionários (objetos)
            
            if 'adset' in df.columns:
                # Extrai 'name' do objeto 'adset'
                df['adset_name'] = df['adset'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
                # O ID pode ter vindo em 'adset_id' ou no objeto. Garantimos o ID do objeto, se necessário.
                df['adset_id'] = df['adset'].apply(lambda x: x.get('id') if isinstance(x, dict) and x.get('id') else x.get('id') if x else None)
                df = df.drop(columns=['adset'], errors='ignore')

            if 'campaign' in df.columns:
                # Extrai 'name' do objeto 'campaign'
                df['campaign_name'] = df['campaign'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
                # O ID pode ter vindo em 'campaign_id' ou no objeto.
                df['campaign_id'] = df['campaign'].apply(lambda x: x.get('id') if isinstance(x, dict) and x.get('id') else x.get('id') if x else None)
                df = df.drop(columns=['campaign'], errors='ignore')
            
            # 3. Garante que as 6 colunas existem, mesmo que com None/NaN
            required_cols = ['ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None # Cria a coluna se estiver faltando (essencial para o transform.py)
            
        print(f"[EXTRAÇÃO: Dimensão (Nomes)] Extraídos {len(df)} anúncios.")
        return df
    
    except Exception as e:
        print(f'[EXTRAÇÃO: Dimensão (Nomes)] Erro fatal na extração: {e}')
        return pd.DataFrame()



# 3. FUNÇÕES DE EXTRAÇÃO DE INSIGHTS (ads_campaign_performance & ads_lead_insights)


# Colunas padrão para Insights (NÃO incluem nomes, que vêm da dimensão)
INSIGHTS_FIELDS = [
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.actions,
    AdsInsights.Field.action_values,
]

def _get_insights_data(total_days: int, level: str, breakdown: list = None) -> pd.DataFrame:
    """Função genérica para extrair Ads Insights."""
    account, time_range = _init_api_and_get_timerange(total_days)
    if account is None: return pd.DataFrame()
    
    breakdown_str = ' + '.join(breakdown) if breakdown else 'Nenhum'
    print(f"\n[EXTRAÇÃO: Insights - {level} | Quebra: {breakdown_str}] Iniciando extração de {total_days} dias...")

    try:
        params = {
            'level': level,
            'time_range': time_range,
            'time_increment': 1, # Granularidade diária
            'filtering': [],
            'limit': 1000,
        }
        if breakdown:
            params['breakdowns'] = breakdown
        
        insights = account.get_insights(
            fields=INSIGHTS_FIELDS,
            params=params
        )

        data = [insight.export_all_data() for insight in insights]
        df = pd.DataFrame(data)
        
        print(f"[EXTRAÇÃO: Insights - {level} | Quebra: {breakdown_str}] Extraídas {len(df)} linhas.")
        return df

    except Exception as e:
        print(f'[EXTRAÇÃO: Insights - {level} | Quebra: {breakdown_str}] Erro fatal na extração: {e}')
        return pd.DataFrame()


# Funções Específicas
def get_campaign_data_raw(total_days: int = 182) -> pd.DataFrame:
    """Extrai Performance de Campanhas (Nível Ad) - Tabela Fato Agregada."""
    return _get_insights_data(total_days, level='ad', breakdown=[])


def get_lead_demographic_raw(total_days: int = 182) -> pd.DataFrame:
    """Extrai Leads com quebra por Demografia (Idade e Gênero)."""
    return _get_insights_data(total_days, level='ad', breakdown=['age', 'gender'])


def get_lead_geographic_raw(total_days: int = 182) -> pd.DataFrame:
    """Extrai Leads com quebra por Região (State/Province)."""
    return _get_insights_data(total_days, level='ad', breakdown=['region'])
