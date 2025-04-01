import pandas as pd
import numpy as np
import re

def preprocess_invoices_and_movements(data_path):
    invoices = pd.read_excel(data_path, sheet_name='SII')
    movements = pd.read_excel(data_path, sheet_name='FintocBank')
    invoices = preprocess_invoices(invoices)
    movements = preprocess_movements(movements)
    return invoices, movements

def preprocess_invoices(invoices):
    invoices = get_valid_invoices(invoices)
    invoices = get_invoice_sales(invoices)
    invoices = select_invoice_columns(invoices)
    invoices = set_invoice_dates(invoices)
    invoices = rename_invoice_columns(invoices)
    invoices = remove_dash_from_rut(invoices, ['inv_rut', 'inv_counterparty_rut'])
    return invoices

def preprocess_movements(movements):
    movements = get_movement_sales(movements)
    movements = select_movement_columns(movements)
    movements = set_movement_dates(movements)
    movements = rename_movement_columns(movements)
    movements = remove_dash_from_rut(movements, ['mov_rut', 'mov_counterparty_rut'])
    movements = remove_transfers_between_accounts(movements)
    movements = find_missing_rut_on_movement(movements)
    return movements

def get_valid_invoices(df):
    accepted_invoices = df[(df['confirmation_status'] != 'R') & 
                           (~df['invoice_status'].isin(['cancelled', 'rejected'])) &
                           (df['total_adjusted_amount'] > 0)]
    accepted_invoices_with_valid_types = accepted_invoices[(accepted_invoices['document_type'].isin([35,38,39,41,48,30,32,33,34,43,45,46,101,102,110,901,914]))]
    return accepted_invoices_with_valid_types

def get_invoice_sales(df):
    return df[df['issue_type'] == 'issued']

def select_invoice_columns(df):
    return df[['identity', 'number', 'invoice_date', 'total_adjusted_amount', 'counterparty_id']]

def set_invoice_dates(df):
    df['invoice_date'] = pd.to_datetime(df['invoice_date']).dt.date
    return df

def rename_invoice_columns(df):
    return df.rename(columns={'identity': 'inv_rut', 'number': 'inv_number', 'total_adjusted_amount': 'inv_amount', 
                              'counterparty_id': 'inv_counterparty_rut', 'invoice_date':'inv_date'})

def remove_dash_from_rut(df, columns):
    df[columns] = df[columns].apply(lambda col: col.astype(str).str.replace('-', ''))
    return df

def find_missing_rut_on_movement(df):
    df.loc[df['mov_counterparty_rut'].isin(['nan']), 'mov_counterparty_rut'] = df.loc[df['mov_counterparty_rut'].isin(['nan']), 'mov_description'].apply(extract_rut)
    return df

def extract_rut(text):
    match = re.search(r'(?<!\d)0*(\d+)\s*[-–—]?\s*([0-9kK])', text)
    if match:
        return match.group(1) + match.group(2).lower()
    return np.nan

def get_movement_sales(df):
    df = df[df['amount'] > 0]
    df['counterparty_id'] = df['sender_account_holder_id'].apply(
        lambda x: x if pd.notna(x) and x != '' else np.nan
    )
    return df

def select_movement_columns(df):
    return df[['id', 'identity', 'post_date', 'amount', 'description', 'counterparty_id']]

def set_movement_dates(df):
    df['post_date'] = pd.to_datetime(df['post_date']).dt.date
    return df

def rename_movement_columns(df):
    return df.rename(columns={'id': 'mov_id', 'identity': 'mov_rut', 'post_date': 'mov_date', 'amount': 'mov_amount', 
                              'description': 'mov_description', 'counterparty_id': 'mov_counterparty_rut'})

def remove_transfers_between_accounts(df):
    return df[df['mov_rut'] != df['mov_counterparty_rut']]

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    invoices, movements = preprocess_invoices_and_movements('Data_sample.xlsx')
    print(invoices.head())
    print(movements.head())
