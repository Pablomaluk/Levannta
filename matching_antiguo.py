import pandas as pd
import numpy as np
import itertools
import datetime as dt
from preprocessing import get_preprocessed_invoices_and_movements

def get_combined_movements_and_movement_groups(invoices, movements):
    add_movement_group_columns(movements)
    exact_single_matches = get_exact_matches(invoices, movements)
    movement_groups_with_exact_matches = get_movement_groups_with_exact_matches(invoices, movements, exact_single_matches)
    movements = pd.concat([movements, movement_groups_with_exact_matches], ignore_index=True)
    return movements


def get_exact_matches(invoices, movements):
    matches = pd.merge(invoices, movements,
                                left_on=['rut', 'inv_amount', 'counterparty_rut'], 
                                right_on=['rut', 'mov_amount', 'counterparty_rut'])
    return get_matches_in_date_range(matches)

def get_matches_in_date_range(matches):
    return matches[(matches['mov_date'] >= matches['inv_date'] - dt.timedelta(days=14)) &
                   (matches['mov_date'] <= matches['inv_date'] + dt.timedelta(days=90))]



def add_movement_group_columns(movements):
    movements['is_mov_group'] = False
    movements[['mov_group_ids', 'mov_group_dates']] = np.nan


def get_movement_groups_with_exact_matches(invoices, movements, exact_matches):
    pending_movements = get_movements_without_exact_match(movements, exact_matches)
    groups_with_match = group_and_match_pending_movement_groups(pending_movements, invoices)
    return pd.DataFrame(groups_with_match)

def get_movements_without_exact_match(movements, matches):
    return movements[~movements['mov_id'].isin(matches['mov_id'])]

def group_and_match_pending_movement_groups(pending_movements, invoices):
    match_groups = []
    pending_movements = pending_movements.sort_values(by='mov_date', ascending=True)
    pending_movements = pending_movements[(~pending_movements['counterparty_rut'].isna()) & (pending_movements['counterparty_rut'] != 'nan')]
    for counterparty_rut, group in pending_movements.groupby('counterparty_rut'):
        if len(group) < 2:
            continue
        subgroups = get_all_ordered_subgroups_from_dataframe(group)
        matchable_invoices = invoices[invoices['counterparty_rut'] == counterparty_rut]
        for subgroup in subgroups:
            mov_subgroup = create_movement_group_from_group_list(subgroup)
            if mov_subgroup['mov_amount'] in matchable_invoices['inv_amount'].values:
                match_groups.append(mov_subgroup)
    return match_groups

def get_all_ordered_subgroups_from_dataframe(df):
    subgroups = []
    df = df.to_dict('records')
    for index in range(len(df)-4):
    #for length in range(2,5):
        #combination_tuples = itertools.combinations(df, length)
        subgroups.extend([list(comb) for comb in itertools.combinations(df[index:index+5], 2)])
        subgroups.extend([list(comb) for comb in itertools.combinations(df[index:index+5], 3)])
    return subgroups

def create_movement_group_from_group_list(group_list):
    group = group_list[0].copy()
    group['mov_amount'] = sum(map(lambda x: x['mov_amount'], group_list))
    group['is_mov_group'] = True
    group['mov_group_ids'] = list(map(lambda x: x['mov_id'], group_list))
    group['mov_group_dates'] = list(map(lambda x: x['mov_date'], group_list))
    group['mov_id'] = np.nan
    group['mov_description'] = np.nan
    return group

def assign_matches(matches):
    matched_movements_count = 0
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
                matched_movements_count += 1
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

def save(matches, pending_invoices, pending_movements, invoices, movements):
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
    invoices, movements = get_preprocessed_invoices_and_movements()
    invoices = invoices[invoices['rut'].isin([763653773])]
    movements = movements[movements['rut'].isin([763653773])]
    movements = get_combined_movements_and_movement_groups(invoices, movements)
    matches = get_exact_matches(invoices, movements)
    matches_dict = assign_matches(matches)
    matches = get_matches_from_dict(invoices, movements, matches_dict)
    pending_invoices, pending_movements = get_pending_invoices_and_movements(invoices, movements, matches_dict)
    save(matches, pending_invoices, pending_movements, invoices, movements)
    print(f"Invoices assigned: {round(100*len(set(matches_dict.values()))/(len(set(matches_dict.values()))+len(pending_invoices)), 1)}%")
