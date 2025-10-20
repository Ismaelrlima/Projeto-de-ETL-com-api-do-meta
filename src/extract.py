import os
import datetime
import pandas as pd
from dotenv import load_dotenv

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.leadgenform import LeadgenForm
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()

APP_ID = os.getenv("LEADS_APP_ID")
APP_SECRET = os.getenv("LEADS_APP_SECRET")
ACCESS_TOKEN = os.getenv("LEADS_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("LEADS_AD_ACCOUNT_ID")

def get_raw_leads_data(total_days: int = 182) -> pd.DataFrame:
    """Extrai dados brutos de leads via API do Facebook."""

    if not all([APP_ID, APP_SECRET, ACCESS_TOKEN, AD_ACCOUNT_ID]):
        print('ERRO: Credenciais do Meta Ads (LEADS_) não encontradas no .env.')
        return pd.DataFrame()

    today = datetime.date.today()
    date_end = today - datetime.timedelta(days=1)
    date_start = date_end - datetime.timedelta(days=total_days)
    time_range = {
        'since': date_start.strftime('%Y-%m-%d'),
        'until': date_end.strftime('%Y-%m-%d')
    }

    print(f"\n[EXTRAÇÃO: Leads Brutos] Iniciando extração de {total_days} dias...")

    account_id_clean = AD_ACCOUNT_ID.replace("act_", "")
    formatted_account_id = f"act_{account_id_clean}"

    try:
        FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
        api = FacebookAdsApi.get_default_api()

        account = AdAccount(formatted_account_id, api=api)

        forms_cursor = account.get_lead_gen_forms(
            fields=['id'],
            params={'limit': 100}
        )

        forms_data = [form.export_all_data() for form in forms_cursor]
        all_leads = []

        for form_obj in forms_data:
            form_id = form_obj.get('id')

            lead_fields = [
                'id', 'created_time', 'ad_id', 'campaign_id', 'adset_id',
                'form_id', 'field_data', 'ad_platform_data'
            ]

            leads_cursor = LeadgenForm(form_id, api=api).get_leads(
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
    