import pandas as pd

from .extract import get_campaign_data_raw, get_lead_demographic_raw, get_lead_geographic_raw, get_name_dim_raw 
import numpy as np 


ALLOWED_ACTION_COLUMNS = [
    
    'lead', 'purchase', 'link_click', 'page_engagement', 'post_engagement', 'video_view', 'comment',
    
    
    'offsite_complete_registration_add_meta_leads', 'onsite_conversion_lead_grouped',
    'offsite_search_add_meta_leads', 'offsite_content_view_add_meta_leads',
    'onsite_conversion_messaging_first_reply', 'onsite_conversion_messaging_conversation_started_7d',
    'onsite_conversion_total_messaging_connection', 'onsite_conversion_messaging_conversation_replied_7d',
    'offsite_conversion_fb_pixel_lead', 'offsite_conversion_fb_pixel_purchase',
    'onsite_conversion_messaging_block',
]



def _normalize_actions(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza a coluna 'actions' e converte tipos."""
    if df.empty: return df
    
    data = []
    for _, insight in df.iterrows(): 
        row = insight.to_dict()
        if 'actions' in row and isinstance(row['actions'], list):
            actions_list = row.pop('actions')
            for action in actions_list:
                action_type = action['action_type']
                value = action['value']
                
                safe_col_name = action_type.replace('.', '_')
                row[safe_col_name] = value 
        data.append(row)

    df_transformed = pd.DataFrame(data)
    
    
    non_count_cols = ['date_start', 'date_stop', 'ad_id', 'adset_id', 'campaign_id', 'age', 'gender', 'region', 'spend']
    
    
    required_metrics = ['spend', 'clicks', 'impressions', 'lead', 'purchase'] 
    
    for col in required_metrics:
        if col not in df_transformed.columns:
            df_transformed[col] = 0
            
    monetary_cols = ['spend']
    for col in monetary_cols:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0) 

    count_cols = [col for col in df_transformed.columns if col not in non_count_cols and col not in monetary_cols]
    
    for col in count_cols:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0)
            df_transformed[col] = df_transformed[col].astype(pd.Int64Dtype())

    return df_transformed


def _recalculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcula métricas derivadas (CPC, CPL, CAC, etc.)."""
    if df.empty: return df

    
    df = df.rename(columns={'impressions': 'total_impressions', 'clicks': 'total_clicks', 
                             'spend': 'total_spend', 
                             'lead': 'total_leads', 
                             
                             }, errors='ignore')

    
    if 'total_successes' not in df.columns:
        df['total_successes'] = 0 
        

    df['cpc'] = df['total_spend'] / df['total_clicks']
    df['cpl'] = df['total_spend'] / df['total_leads']
    df['ctr'] = df['total_clicks'] / df['total_impressions']
    

    df = df.replace([float('inf'), -float('inf')], 0).fillna(0)
    
    return df


def run_etl_pipeline_dim(total_days=1) -> pd.DataFrame:
    
    df_raw = get_name_dim_raw(total_days=total_days) 
    if df_raw.empty: return pd.DataFrame()
    
    df_final = df_raw.drop_duplicates(subset=['ad_id'], keep='last')
    return df_final[['ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name']]


def run_etl_pipeline_campaigns(total_days=182) -> pd.DataFrame:
    df_raw = get_campaign_data_raw(total_days=total_days)
    df_norm = _normalize_actions(df_raw)
    
    group_keys = ['date_start', 'ad_id', 'adset_id', 'campaign_id']
    df_agg = df_norm.groupby(group_keys, as_index=False).first()

    df_final = _recalculate_metrics(df_agg)
    
    
    final_cols = ['date_start', 'ad_id', 'adset_id', 'campaign_id', 'total_impressions', 'total_clicks', 
                  'total_spend', 'total_leads', 'total_successes', 'cpc', 'ctr', 'cpl'] 
    
    
    final_cols_safe = [col for col in final_cols if col in df_final.columns]
    
    return df_final[final_cols_safe]


def run_etl_pipeline_leads(total_days=182) -> pd.DataFrame:
    """Processa a tabela de Leads unindo as extrações Demográfica e Geográfica (2-Way Merge)."""
    df_demo_raw = get_lead_demographic_raw(total_days=total_days)
    df_geo_raw = get_lead_geographic_raw(total_days=total_days)
    

    if df_demo_raw.empty and df_geo_raw.empty:
        return pd.DataFrame()
        
    df_demo = _normalize_actions(df_demo_raw)
    df_geo = _normalize_actions(df_geo_raw)

    base_merge_keys = ['date_start', 'ad_id', 'adset_id', 'campaign_id', 'impressions', 'clicks', 'spend']
    merge_keys_base = [k for k in base_merge_keys if k not in ['spend', 'impressions', 'clicks']] 
    
    df_base = df_demo
    
    df_geo_subset = df_geo[['region'] + merge_keys_base].drop_duplicates()
    
    df_final = pd.merge(df_base, df_geo_subset, 
                        on=merge_keys_base,
                        how='outer',
                        suffixes=('_demo', '_geo')) 
    
    
    df_final = df_final.loc[:,~df_final.columns.duplicated()].copy()
    df_final = df_final.fillna(0)
    
    df_recalc = _recalculate_metrics(df_final)
    
    group_keys = ['date_start', 'ad_id', 'adset_id', 'campaign_id', 'age', 'gender', 'region']
    
    final_cols = group_keys + ['total_spend', 'total_leads'] + [col for col in ALLOWED_ACTION_COLUMNS if col in df_recalc.columns]
    final_cols = list(dict.fromkeys(final_cols))

    return df_recalc[[col for col in final_cols if col in df_recalc.columns]]