import pandas as pd
import numpy as np
import re


def get_preprocessed_invoices_and_movements():
    try:
        invoices = pd.read_csv('Invoices.csv')
        movements = pd.read_csv('Movements.csv')
        invoices, movements = set_column_types(invoices, movements)
        return invoices, movements
    except:
        return preprocess_invoices_and_movements()
    

def set_column_types(invoices, movements):
    invoices = set_invoice_dates(invoices)
    movements = set_movement_dates(movements)
    return invoices, movements

def preprocess_invoices_and_movements(load=False):
    if load == False:
        invoices, movements = read_all_invoices_and_movements()
        invoices, movements = save_valid_invoices_and_movements(invoices, movements)
    else:
        invoices, movements = load_valid_invoices_and_movements()
    invoices = preprocess_invoices(invoices)
    movements = preprocess_movements(movements)
    invoices = remove_invoices_before_movements(invoices, movements)
    save_preprocessed_invoices_and_movements(invoices, movements)
    return invoices, movements

def read_all_invoices_and_movements():
    invoices = pd.read_csv('All_Invoices.csv')
    movements = pd.read_csv('All_Movements.csv')
    return invoices, movements

def save_valid_invoices_and_movements(invoices, movements):
    invoices = get_valid_invoices(invoices)
    invoices = get_invoice_sales(invoices)
    movements = get_movement_sales(movements)
    invoices.to_csv('Raw_Invoices.csv')
    movements.to_csv('Raw_Movements.csv')
    with pd.ExcelWriter('Raw_Dataset.xlsx') as writer:
        invoices.to_excel(writer, sheet_name='Invoices', index=False)
        movements.to_excel(writer, sheet_name='Movements', index=False)
    return invoices, movements

def get_valid_invoices(df):
    accepted_invoices = df[(df['confirmation_status'] != 'R') & 
                           (~df['invoice_status'].isin(['cancelled', 'rejected'])) &
                           (df['total_adjusted_amount'] > 0)]
    accepted_invoices_with_valid_types = accepted_invoices[(accepted_invoices['document_type'].isin([35,38,39,41,48,30,32,33,34,43,45,46,101,102,110,901,914]))]
    return accepted_invoices_with_valid_types

def get_invoice_sales(df):
    return df[df['issue_type'] == 'issued']

def get_movement_sales(df):
    df = df[df['amount'] > 0]
    return df

def load_valid_invoices_and_movements():
    invoices = pd.read_csv('Raw_Invoices.csv')
    movements = pd.read_csv('Raw_Movements.csv')
    return invoices, movements

def preprocess_invoices(invoices):
    invoices = select_invoice_columns(invoices)
    invoices = rename_invoice_columns(invoices)
    invoices = set_invoice_dates(invoices)
    invoices = format_rut(invoices, ['rut', 'counterparty_rut'])
    return invoices

def preprocess_movements(movements):
    movements = select_movement_columns(movements)
    movements = rename_movement_columns(movements)
    movements = set_movement_dates(movements)
    movements = format_rut(movements, ['rut', 'counterparty_rut'])
    movements = remove_transfers_between_accounts(movements)
    return movements

def remove_invoices_before_movements(invoices, movements):
    earliest_movements = movements.groupby('rut').min().reset_index()[['rut', 'mov_date']]
    earliest_movements['mov_date'] = earliest_movements['mov_date'] - pd.Timedelta(days=90)
    invoices = invoices.merge(earliest_movements, on='rut')
    invoices = invoices[invoices['inv_date'] >= invoices['mov_date']].drop(columns='mov_date')
    return invoices

def save_preprocessed_invoices_and_movements(invoices, movements):
    invoices.to_csv('Invoices.csv', index=False)
    movements.to_csv('Movements.csv', index=False)

def select_invoice_columns(df):
    return df[['identity', 'number', 'invoice_date', 'total_adjusted_amount', 'counterparty_id']]

def rename_invoice_columns(df):
    return df.rename(columns={'identity': 'rut', 'number': 'inv_number', 'total_adjusted_amount': 'inv_amount',
                              'counterparty_id': 'counterparty_rut', 'invoice_date':'inv_date'})

def set_invoice_dates(df):
    df['inv_date'] = pd.to_datetime(df['inv_date'], errors='coerce')
    df = df.dropna(subset=['inv_date'])
    df['inv_date'] = df['inv_date'].dt.date
    return df

def format_rut(df, columns):
    df[columns] = df[columns].apply(remove_dash_from_rut)
    df[columns] = df[columns].apply(lambda x: x.astype(str).str.lower())
    return df

def remove_dash_from_rut(rut):
    return rut.astype(str).str.replace('-', '')

# def find_missing_rut_on_movement(df):
#     df.loc[df['mov_counterparty_rut'].isin(['nan']), 'mov_counterparty_rut'] = df.loc[df['mov_counterparty_rut'].isin(['nan']), 'mov_description'].apply(extract_rut)
#     return df

# def extract_rut(text):
#     match = re.search(r'(?<!\d)0*(\d+)\s*[-–—]?\s*([0-9kK])', text)
#     if match:
#         return match.group(1) + match.group(2).lower()
#     return np.nan

def select_movement_columns(df):
    return df[['id', 'identity', 'post_date', 'amount', 'description', 'counterparty_id']]

def rename_movement_columns(df):
    return df.rename(columns={'id': 'mov_id', 'identity': 'rut', 'post_date': 'mov_date', 'amount': 'mov_amount', 
                              'description': 'mov_description', 'counterparty_id': 'counterparty_rut'})

def set_movement_dates(df):
    df['mov_date'] = pd.to_datetime(df['mov_date']).dt.date
    return df

def remove_transfers_between_accounts(df):
    return df[df['rut'] != df['counterparty_rut']]

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    invoices, movements = preprocess_invoices_and_movements(load=True)
    
