import pandas as pd
import numpy as np
import itertools
import datetime as dt
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from Preprocessing.preprocessing import get_preprocessed_invoices_and_movements

def get_path(file):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(BASE_DIR, file)

def get_current_matches_and_pending_invoices_and_movements():
    try:
        matches, pending_invoices, pending_movements = read_dfs()
        return matches, pending_invoices, pending_movements
    except FileNotFoundError:
        invoices, movements = get_preprocessed_invoices_and_movements()
        movements = get_combined_movements_and_movement_groups(invoices, movements)
        matches = get_exact_matches(invoices, movements)
        matches_dict = assign_matches(matches)
        matches = get_matches_from_dict(invoices, movements, matches_dict)
        pending_invoices, pending_movements = get_pending_invoices_and_movements(invoices, movements, matches_dict)
        save_dfs(matches, pending_invoices, pending_movements)
        return matches, pending_invoices, pending_movements
    finally:
        print(f"Initial Matches\nInvoices assigned: {round(100*matches['inv_number'].nunique()/(matches['inv_number'].nunique()+len(pending_invoices)), 1)}%")


def read_dfs():
    matches = pd.read_csv(get_path('Matches.csv'))
    invoices = pd.read_csv(get_path('Pending Invoices.csv'))
    movements = pd.read_csv(get_path('Pending Movements.csv'))
    invoices['inv_date'] = pd.to_datetime(invoices['inv_date']).dt.date
    movements['mov_date'] = pd.to_datetime(movements['mov_date']).dt.date
    return matches, invoices, movements

def save_dfs(matches, pending_invoices, pending_movements):
    matches.to_csv(get_path('Matches.csv'), index=False)
    pending_invoices.to_csv(get_path('Pending Invoices.csv'), index=False)
    pending_movements.to_csv(get_path('Pending Movements.csv'), index=False)

def get_combined_movements_and_movement_groups(invoices, movements):
    exact_matches = get_exact_matches(invoices, movements)
    pending_movements = get_pending_movements(movements, exact_matches)
    groups_with_match = group_movements_and_return_groups_with_matches(pending_movements, invoices)
    movements = pd.concat([movements, groups_with_match], ignore_index=True)
    return movements

def get_exact_matches(invoices, movements):
    matches = pd.merge(invoices, movements,
                                left_on=['rut', 'inv_amount', 'counterparty_rut'], 
                                right_on=['rut', 'mov_amount', 'counterparty_rut'])
    return get_matches_in_date_range(matches)

def get_matches_in_date_range(matches):
    return matches[(matches['mov_date'] >= matches['inv_date'] - dt.timedelta(days=14)) &
                   (matches['mov_date'] <= matches['inv_date'] + dt.timedelta(days=90))]

def get_pending_movements(movements, matches):
    return movements[~movements['mov_id'].isin(matches['mov_id'])]

def group_movements_and_return_groups_with_matches(pending_movements, invoices):
    movement_groups_with_matches = []
    pending_movements = pending_movements[(~pending_movements['counterparty_rut'].isna()) & (pending_movements['counterparty_rut'] != 'nan')]
    for counterparty_rut, movements in pending_movements.groupby('counterparty_rut'):
        counterparty_invoices = invoices[invoices['counterparty_rut'] == counterparty_rut]
        groups = get_movement_groups(movements)
        for group in groups:
            movement_group = create_movement_group(group)
            if movement_group['mov_amount'] in counterparty_invoices['inv_amount'].values:
                movement_groups_with_matches.append(movement_group)
    return pd.DataFrame(movement_groups_with_matches)

def get_movement_groups(df):
    subgroups = []
    df = df.sort_values(by='mov_date', ascending=True).to_dict('records')
    for index in range(len(df)-4):
        for length in range(2,5):
            subgroups.extend([list(comb) for comb in itertools.combinations(df[index:index+5], length)])
    return subgroups

def create_movement_group(movements):
    group = movements[0].copy()
    group['mov_amount'] = sum(map(lambda x: x['mov_amount'], movements))
    group['is_mov_group'] = True
    group['mov_group_ids'] = list(map(lambda x: x['mov_id'], movements))
    group['mov_group_dates'] = list(map(lambda x: x['mov_date'], movements))
    group['mov_id'] = np.nan
    group['mov_description'] = np.nan
    return group

def assign_matches(matches):
    matches_dict = {}
    matches['date_diff'] = matches['mov_date'] - matches['inv_date']
    matches = matches.sort_values(['date_diff'])
    for invoice_number, matched_movements in matches.groupby('inv_number'):
        for index, matched_movement in matched_movements.iterrows():
            if invoice_number in matches_dict.values():
                break
            if matched_movement['is_mov_group']:
                if matched_movement['mov_group_ids'][0] in matches_dict.keys():
                    continue
                for mov_id in matched_movement['mov_group_ids']:
                    matches_dict[mov_id] = invoice_number
            else:
                if matched_movement['mov_id'] in matches_dict.keys():
                    continue
                matches_dict[matched_movement['mov_id']] = invoice_number
    return matches_dict

def get_matches_from_dict(invoices, movements, matches_dict):
    matches = pd.DataFrame(list(matches_dict.items()), columns=['mov_id', 'inv_number'])
    matches = pd.merge(matches, invoices, on='inv_number')
    matches = pd.merge(matches, movements, on=['mov_id', 'rut', 'counterparty_rut'])
    return matches

def get_pending_invoices_and_movements(invoices, movements, matches_dict):
    pending_invoices = invoices[~invoices['inv_number'].isin(matches_dict.values())]
    pending_movements = movements[~movements['is_mov_group'] & ~movements['mov_id'].isin(matches_dict.keys())]
    return pending_invoices, pending_movements


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
    get_current_matches_and_pending_invoices_and_movements()