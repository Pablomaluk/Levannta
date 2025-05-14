import pandas as pd
import numpy as np
import os

PATH = "Preprocessing"

def get_preprocessed_invoices_and_movements():
    try:
        return read_preprocessed_invoices_and_movements()
    except FileNotFoundError:
        invoices, movements = read_invoices_and_movements()
        invoices, movements = get_valid_invoices_and_movements(invoices, movements)
        invoices = preprocess_invoices(invoices)
        movements = preprocess_movements(movements)
        save_dates(invoices, movements)
        invoices = remove_invoices_before_movements(invoices, movements)
        save_preprocessed_invoices_and_movements(invoices, movements)
        return invoices, movements

def read_preprocessed_invoices_and_movements():
    invoices = pd.read_csv(os.path.join(PATH, 'Preprocessed Invoices.csv'))
    movements = pd.read_csv(os.path.join(PATH,'Preprocessed Movements.csv'))
    return set_invoices_date_type(invoices), set_movements_date_type(movements) 

def read_invoices_and_movements():
    invoices = pd.read_csv(os.path.join(PATH,'All Invoices.csv'))
    movements = pd.read_csv(os.path.join(PATH,'All Movements.csv'))
    return invoices, movements

def get_valid_invoices_and_movements(invoices, movements):
    invoices = get_valid_invoices(invoices)
    invoices = get_invoice_sales(invoices)
    movements = get_movement_sales(movements)
    return invoices, movements

def get_valid_invoices(df):
    accepted_invoices = df[(df['confirmation_status'] != 'R') & 
                           (df['total_adjusted_amount'] > 0)]
    invoice_document_types = [35,38,39,41,48,30,32,33,34,43,45,46,101,102,110,901,914]
    accepted_invoices_with_valid_types = accepted_invoices[
        (accepted_invoices['document_type'].isin(invoice_document_types))
    ]
    return accepted_invoices_with_valid_types

def get_invoice_sales(df):
    return df[df['issue_type'] == 'issued']

def get_movement_sales(df):
    df = df[df['amount'] > 0]
    return df

def preprocess_invoices(invoices):
    invoices = select_invoice_columns(invoices)
    invoices = rename_invoice_columns(invoices)
    invoices = set_date_type(invoices, ['inv_date'])
    invoices = format_rut(invoices, ['rut', 'counterparty_rut'])
    add_invoice_group_columns(invoices)
    return invoices

def preprocess_movements(movements):
    movements = select_movement_columns(movements)
    movements = rename_movement_columns(movements)
    movements = set_date_type(movements, ['mov_date'])
    movements = format_rut(movements, ['rut', 'counterparty_rut'])
    movements = remove_transfers_between_accounts(movements)
    add_movement_group_columns(movements)
    return movements

def save_dates(invoices, movements):
    min_mov_dates = movements.groupby('rut').min().reset_index()[['rut', 'mov_date']]
    min_inv_dates = invoices.groupby('rut').min().reset_index()[['rut', 'inv_date']]
    new_invs = remove_invoices_before_movements(invoices, movements)
    new_inv_dates = new_invs.groupby('rut').min().reset_index()[['rut', 'inv_date']]
    dates = pd.merge(new_inv_dates, pd.merge(min_mov_dates, min_inv_dates, on="rut"), on="rut")
    dates = dates[['rut', 'mov_date', 'inv_date_y', 'inv_date_x']]
    dates.columns = ['RUT', 'Primer movimiento registrado', 'Primera factura registrada','Primera factura utilizada']
    dates.to_csv("Dates.csv", index=False)

def remove_invoices_before_movements(invoices, movements):
    earliest_movements = movements.groupby('rut').min().reset_index()[['rut', 'mov_date']]
    earliest_movements['mov_date'] = earliest_movements['mov_date'] - pd.Timedelta(days=90)
    invoices = invoices.merge(earliest_movements, on='rut')
    invoices = invoices[invoices['inv_date'] >= invoices['mov_date']].drop(columns='mov_date')
    return invoices

def save_preprocessed_invoices_and_movements(invoices, movements):
    invoices.to_csv(os.path.join(PATH, 'Preprocessed Invoices.csv'), index=False)
    movements.to_csv(os.path.join(PATH, 'Preprocessed Movements.csv'), index=False)

def select_invoice_columns(df):
    return df[['identity', 'number', 'invoice_date', 'total_adjusted_amount', 'counterparty_id']]

def rename_invoice_columns(df):
    return df.rename(columns={'identity': 'rut', 'number': 'inv_number', 'total_adjusted_amount': 'inv_amount',
                              'counterparty_id': 'counterparty_rut', 'invoice_date':'inv_date'})

def set_invoices_date_type(invoices):
    invoices = set_date_type(invoices, ['inv_date'])
    invoices =  set_date_type(invoices, ['first_inv_date'])
    return  set_date_type(invoices, ['last_inv_date'])

def set_movements_date_type(movements):
    movements = set_date_type(movements, ['mov_date'])
    movements =  set_date_type(movements, ['first_mov_date'])
    return  set_date_type(movements, ['last_mov_date'])

def set_date_type(df, col):
    df[col] = df[col].apply(lambda x: pd.to_datetime(x, errors='coerce'))
    df = df.dropna(subset=col)
    df[col] = df[col].apply(lambda x: x.dt.date)
    return df

def add_invoice_group_columns(invoices):
    invoices['is_inv_group'] = False
    invoices['inv_group_len'] = 1
    invoices['first_inv_date'] = invoices['inv_date']
    invoices['last_inv_date'] = invoices['inv_date']
    #invoices['inv_group_numbers'] = np.nan

def format_rut(df, columns):
    df[columns] = df[columns].apply(remove_dash_from_rut)
    df[columns] = df[columns].apply(lambda x: x.astype(str).str.lower())
    return df

def remove_dash_from_rut(rut):
    return rut.astype(str).str.replace('-', '')

def select_movement_columns(df):
    return df[['id', 'identity', 'post_date', 'amount', 'description', 'counterparty_id']]

def rename_movement_columns(df):
    return df.rename(columns={'id': 'mov_id', 'identity': 'rut', 'post_date': 'mov_date', 'amount': 'mov_amount', 
                              'description': 'mov_description', 'counterparty_id': 'counterparty_rut'})

def remove_transfers_between_accounts(df):
    return df[df['rut'] != df['counterparty_rut']]

def add_movement_group_columns(movements):
    movements['is_mov_group'] = False
    movements['mov_group_len'] = 1
    movements['first_mov_date'] = movements['mov_date']
    movements['last_mov_date'] = movements['mov_date']
    #movements['mov_group_ids'] = np.nan

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    inv, mov = get_preprocessed_invoices_and_movements()
    print(len(inv))
    print(len(mov))
