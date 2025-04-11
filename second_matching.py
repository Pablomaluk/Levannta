import pandas as pd
import numpy as np
import datetime as dt
import recordlinkage
from recordlinkage.preprocessing import clean
from rapidfuzz import fuzz
import re
from first_matching import get_current_matches_and_pending_invoices_and_movements
from helpers import read_stage_dfs, save_stage_dfs, print_matches_percentage_per_rut
pd.set_option('display.max_columns', None)

def get_matches_in_date_range(matches):
    return matches[(matches['mov_date'] >= matches['inv_date'] - dt.timedelta(days=14)) &
                   (matches['mov_date'] <= matches['inv_date'] + dt.timedelta(days=90))]

#Relative difference
def compare_amount_difference(invoices, movements, matches):
    links = pd.merge(invoices, movements, on=['rut','counterparty_rut'])
    links = get_matches_in_date_range(links)
    links['rel_amount_diff'] = \
        abs(links['inv_amount'] - links['mov_amount'])/links['inv_amount']
    links['amount_similarity'] = gaussian_similarity(links['rel_amount_diff'])
    matches['rel_amount_diff'] = 0
    matches['amount_similarity'] = 1
    return links, matches

def gaussian_similarity(series):
    scale = 0.05  # ~5% diferencia
    return np.exp(-(series / scale) ** 2)

def assign_gaussian_matches(matches):
    pending_matches = matches[matches['amount_similarity'] > 0.2].sort_values(by='amount_similarity', ascending=False)
    matches = matches.iloc[0:0]
    while pending_matches['inv_number'].nunique() and pending_matches['mov_id'].nunique():
        new_matches = get_best_gaussian_matches(pending_matches)
        matches = pd.concat([matches, new_matches])
        pending_matches = pending_matches[
            ~pending_matches['inv_number'].isin(matches['inv_number']) &
            ~pending_matches['mov_id'].isin(matches['mov_id'])]
    return matches

def get_best_gaussian_matches(matches):
    most_similar_mov_to_inv = matches.drop_duplicates(subset='inv_number')
    most_similar_inv_to_mov = matches.drop_duplicates(subset='mov_id')
    merged = pd.merge(most_similar_mov_to_inv, most_similar_inv_to_mov, on=['mov_id', 'inv_number'], suffixes=('', '_'))
    return merged[matches.columns]

# Visualización de datos

def most_active_counterparties(invoices, movements):
    set_counterparty_counts_and_balances(invoices, movements)
    counterparty_ruts = (
        invoices[['counterparty_rut', 'inv_count', 'inv_amount_sum']]
        .drop_duplicates()
        .sort_values(by=['inv_count', 'inv_amount_sum'], ascending=False)['counterparty_rut']
        .head(5).values
    )
    invoices = invoices[['rut', 'counterparty_rut', 'inv_date', 'inv_amount', 'inv_number']]
    invoices.columns = ['RUT', 'RUT contraparte', 'Fecha factura', 'Monto factura', 'Número SII']
    movements = movements[movements['is_mov_group'] == False]
    movements = movements[['rut', 'counterparty_rut', 'mov_date', 'mov_amount', 'mov_id', 'mov_description']]
    movements.columns = ['RUT', 'RUT contraparte', 'Fecha movimiento', 'Monto movimiento', 'ID movimiento', 'Descripción']

    dfs_invoices = []
    dfs_movements = []

    for rut in counterparty_ruts:
            dfs_invoices.append(invoices[invoices['RUT contraparte'] == rut].sort_values(by='Fecha factura'))
            dfs_movements.append(movements[movements['RUT contraparte'] == rut].sort_values(by='Fecha movimiento'))

    return counterparty_ruts, dfs_invoices, dfs_movements

def set_counterparty_counts_and_balances(invoices, movements):
    invoices['inv_amount_sum'] = invoices.groupby('counterparty_rut')['inv_amount'].transform('sum')
    invoices['inv_count'] = invoices.groupby('counterparty_rut')['inv_amount'].transform('count')
    movements['mov_amount_sum'] = movements.groupby('counterparty_rut')['mov_amount'].transform('sum')
    movements['mov_count'] = movements.groupby('counterparty_rut')['mov_amount'].transform('count')

def save_detailed(matches, pending_invoices, pending_movements, invoices, movements):
    matches = matches.sort_values(by=['counterparty_rut', 'inv_date', 'mov_date']).drop(columns=['is_mov_group', 'mov_group_ids', 'mov_group_dates'])
    pending_invoices = pending_invoices.sort_values(by=['counterparty_rut', 'inv_date'])
    pending_movements = pending_movements.sort_values(by=['counterparty_rut', 'mov_date']).drop(columns=['is_mov_group', 'mov_group_ids', 'mov_group_dates'])

    matches = matches[['rut', 'counterparty_rut', 'inv_amount', 'mov_amount', 'inv_date', 'mov_date', 'inv_number', 'mov_description']]
    matches.columns = ['RUT', 'RUT contraparte', 'Monto facturado', 'Monto depositado', 'Fecha factura', 'Fecha depósito', 'Número SII', 'Descripción depósito']

    ruts, invoice_dfs, movement_dfs = most_active_counterparties(invoices, movements)

    with pd.ExcelWriter('output.xlsx') as writer:
        matches.to_excel(writer, sheet_name='Matches', index=False)
        pending_invoices.to_excel(writer, sheet_name='Pending Invoices', index=False)
        pending_movements.to_excel(writer, sheet_name='Pending Movements', index=False)
        for i in range(len(ruts)):
            rut = ruts[i]
            invoice_dfs[i].to_excel(writer, sheet_name=f"Facturas {rut[:-1]}-{rut[-1]}", index=False)
            start_row = len(invoice_dfs[i]) + 2
            movement_dfs[i].to_excel(writer, sheet_name=f"Facturas {rut[:-1]}-{rut[-1]}", index=False, startrow=start_row)
            start_row += len(movement_dfs[i]) + 2
            matches[matches['RUT contraparte'] == rut].to_excel(writer, sheet_name=f"Facturas {rut[:-1]}-{rut[-1]}", index=False, startrow=start_row)

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    previous_matches, invoices, movements = get_current_matches_and_pending_invoices_and_movements()
    #invoices = invoices[invoices['counterparty_rut'] != '555555555']
    #print(invoices['counterparty_rut'].value_counts().head(10))
    #print(movements['counterparty_rut'].value_counts().head(10))
    #print(invoices['rut'].value_counts(), movements['rut'].value_counts())
    matches, previous_matches = compare_amount_difference(invoices, movements, previous_matches)
    matches = assign_gaussian_matches(matches)

    invoices = invoices[~invoices['inv_number'].isin(matches['inv_number'])]
    movements = movements[~movements['mov_id'].isin(matches['mov_id'])]
    matches = pd.concat([previous_matches, matches])
    save_stage_dfs(matches, invoices, movements, 2)


    print('Matching Round 2')
    print_matches_percentage_per_rut(matches, invoices, movements)