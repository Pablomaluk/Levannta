import pandas as pd
import numpy as np
import datetime as dt
import helpers
from params import MAX_MOV_DAYS_BEFORE_INV, MAX_MOV_DAYS_AFTER_INV, GAUSSIAN_SIMILARITY_SCALE, MAX_REL_AMOUNT_DIFF, MAX_GREEDY_ITERATIONS

PATH = "Similar Amounts"

def get_current_dfs(dfs):
    return helpers.get_current_dfs(lambda: main(*dfs), PATH)

def main(invoices, movements, previous_matches):
    matches = compare_amount_difference(invoices, movements, previous_matches)
    matches = assign_gaussian_matches(matches)
    matches = pd.concat([previous_matches, matches])
    pending_invoices = invoices[~invoices['inv_number'].isin(matches['inv_number'])]
    pending_movements = movements[~movements['mov_id'].isin(matches['mov_id'])]
    return pending_invoices, pending_movements, matches

def get_matches_in_date_range(matches):
    return matches[(matches['mov_date'] >= matches['inv_date'] - dt.timedelta(days=MAX_MOV_DAYS_BEFORE_INV)) &
                   (matches['mov_date'] <= matches['inv_date'] + dt.timedelta(days=MAX_MOV_DAYS_AFTER_INV))]

def compare_amount_difference(invoices, movements, matches):
    links = pd.merge(invoices, movements, on=['rut','counterparty_rut'])
    links = get_matches_in_date_range(links)
    links['rel_amount_diff'] = abs(links['inv_amount'] - links['mov_amount'])/links['inv_amount']
    links = links[links['rel_amount_diff'] <= MAX_REL_AMOUNT_DIFF]
    links['amount_similarity'] = gaussian_similarity(links['rel_amount_diff'])
    return links

def gaussian_similarity(series):
    return np.exp(-(series / GAUSSIAN_SIMILARITY_SCALE) ** 2)

def assign_gaussian_matches(matches):
    pending_matches = matches.sort_values(by='amount_similarity', ascending=False)
    matches = matches.iloc[0:0]
    for i in range(MAX_GREEDY_ITERATIONS):
        if not (pending_matches['inv_number'].nunique() and pending_matches['mov_id'].nunique()):
            break
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

def save_detailed(matches, pending_invoices, pending_movements):
    matches = matches.sort_values(by=['counterparty_rut', 'inv_date', 'mov_date']).drop(columns=['is_mov_group', 'mov_group_ids', 'mov_group_dates'])
    pending_invoices = pending_invoices.sort_values(by=['counterparty_rut', 'inv_date'])
    pending_movements = pending_movements.sort_values(by=['counterparty_rut', 'mov_date']).drop(columns=['is_mov_group', 'mov_group_ids', 'mov_group_dates'])
    worst_matches = matches.sort_values(by='amount_similarity', ascending=True).head(50)
    best_matches = matches.sort_values(by='amount_similarity', ascending=False).head(50)

    matches = matches[['rut', 'counterparty_rut', 'inv_amount', 'mov_amount', 'inv_date', 'mov_date', 'inv_number', 'mov_description']]
    matches.columns = ['RUT', 'RUT contraparte', 'Monto facturado', 'Monto depositado', 'Fecha factura', 'Fecha depósito', 'Número SII', 'Descripción depósito']

    worst_matches = worst_matches[['rut', 'counterparty_rut', 'inv_amount', 'mov_amount', 'inv_date', 'mov_date', 'inv_number', 'mov_description', 'rel_amount_diff']]
    worst_matches.columns = ['RUT', 'RUT contraparte', 'Monto facturado', 'Monto depositado', 'Fecha factura', 'Fecha depósito', 'Número SII', 'Descripción depósito', 'Diferencia porcentual']

    best_matches = best_matches[['rut', 'counterparty_rut', 'inv_amount', 'mov_amount', 'inv_date', 'mov_date', 'inv_number', 'mov_description', 'rel_amount_diff']]
    best_matches.columns = ['RUT', 'RUT contraparte', 'Monto facturado', 'Monto depositado', 'Fecha factura', 'Fecha depósito', 'Número SII', 'Descripción depósito', 'Diferencia porcentual']


    with pd.ExcelWriter('output.xlsx') as writer:
        matches.to_excel(writer, sheet_name='Matches', index=False)
        pending_invoices.to_excel(writer, sheet_name='Pending Invoices', index=False)
        pending_movements.to_excel(writer, sheet_name='Pending Movements', index=False)
        worst_matches.to_excel(writer, sheet_name='Worst Matches', index=False)
        best_matches.to_excel(writer, sheet_name='Best Matches', index=False)
