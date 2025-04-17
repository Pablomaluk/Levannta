import pandas as pd
import numpy as np
import datetime as dt
import itertools
import helpers

PATH = "Grouped Invoices"

def get_current_dfs(dfs):
    return helpers.get_current_dfs(lambda: main(*dfs), PATH)

def main(invoices, movements, previous_matches):
        inv_groups = group_invoices_and_return_groups_with_matches(invoices, movements)
        matches = get_exact_matches(inv_groups, movements)
        matches_dict = assign_matches(matches)
        matches = pd.DataFrame(list(matches_dict.items()), columns=['inv_number', 'mov_id'])
        matches = get_merged_matches(matches, invoices, movements)
        matches = helpers.set_exact_match_params(matches)
        save_new_matches(matches)
        matches = pd.concat([previous_matches, matches])
        pending_invoices = invoices[~invoices['inv_number'].isin(matches['inv_number'])]
        pending_movements = movements[~movements['mov_id'].isin(matches['mov_id'])]
        return pending_invoices, pending_movements, matches


def get_matches_in_date_range(matches):
    cond = matches.apply(lambda x: x['mov_date'] >= x['inv_date'] and
                         x['mov_date'] >= x['inv_group_dates'][-1] - dt.timedelta(days=90), axis=1)
    return matches[cond]

def get_exact_matches(invoices, movements):
    matches = pd.merge(invoices, movements,
                                left_on=['rut', 'inv_amount', 'counterparty_rut'], 
                                right_on=['rut', 'mov_amount', 'counterparty_rut'])
    return get_matches_in_date_range(matches)

def group_invoices_and_return_groups_with_matches(pending_invoices, pending_movements):
    invoice_groups_with_matches = []
    pending_movements = pending_movements[(~pending_movements['counterparty_rut'].isna()) & (pending_movements['counterparty_rut'] != 'nan')]
    pending_invoices = get_invoices_from_counterparties_with_movements(pending_invoices, pending_movements)
    for counterparty_rut, invoices in pending_invoices.groupby('counterparty_rut'):
        counterparty_movements = pending_movements[pending_movements['counterparty_rut'] == counterparty_rut]
        groups = get_invoice_groups(invoices)
        for group in groups:
            invoice_group = create_invoice_group(group)
            if invoice_group['inv_amount'] in counterparty_movements['mov_amount'].values:
                invoice_groups_with_matches.append(invoice_group)
    invoice_groups_with_matches = pd.DataFrame(invoice_groups_with_matches)
    date_range_filter = invoice_groups_with_matches.apply(lambda x:
                    x['inv_group_dates'][0] >= x['inv_group_dates'][-1] - dt.timedelta(days=90), axis=1)
    return invoice_groups_with_matches[date_range_filter]

def get_invoices_from_counterparties_with_movements(invoices, movements):
    counterparties_with_movements = movements['counterparty_rut'].unique()
    return invoices[invoices['counterparty_rut'].isin(counterparties_with_movements)]

def get_invoice_groups(df):
    subgroups = []
    df = df.sort_values(by='inv_date', ascending=True).to_dict('records')
    for index in range(len(df)-4):
        for length in range(2,5):
            subgroups.extend([list(comb) for comb in itertools.combinations(df[index:index+5], length) \
                if comb[0]['inv_date'] >= comb[-1]['inv_date'] - dt.timedelta(days=90)])
    return subgroups

def create_invoice_group(invoices):
    group = invoices[0].copy()
    group['inv_amount'] = sum(map(lambda x: x['inv_amount'], invoices))
    group['is_inv_group'] = True
    group['inv_group_numbers'] = list(map(lambda x: x['inv_number'], invoices))
    group['inv_group_dates'] = list(map(lambda x: x['inv_date'], invoices))
    group['inv_number'] = np.nan
    return group

def assign_matches(matches):
    matches_dict = {}
    matched_invoice_numbers = []
    matches['date_diff'] = matches['mov_date'] - matches['inv_date']
    matches = matches.sort_values(['mov_date', 'date_diff'])
    for mov_id, matched_invoice_groups in matches.groupby('mov_id'):
        for index, matched_invoice_group in matched_invoice_groups.iterrows():
            if any(inv_num in matched_invoice_numbers for inv_num in matched_invoice_group['inv_group_numbers']):
                continue
            for inv_num in matched_invoice_group['inv_group_numbers']:
                matches_dict[inv_num] = mov_id
            break
    return matches_dict

def get_merged_matches(matches, invoices, movements):
    matches = pd.merge(matches, invoices, on='inv_number')
    matches = pd.merge(matches, movements, on=['mov_id', 'rut', 'counterparty_rut'])
    return matches

def save_new_matches(new_matches):
    matches = new_matches.sort_values(by=['counterparty_rut', 'inv_date', 'mov_date']).drop(columns=['is_mov_group', 'mov_group_ids', 'mov_group_dates'])
    matches = matches[['rut', 'counterparty_rut', 'inv_amount', 'mov_amount', 'inv_date', 'mov_date', 'inv_number', 'mov_id','mov_description']]
    matches.columns = ['RUT', 'RUT contraparte', 'Monto facturado', 'Monto depositado', 'Fecha factura', 'Fecha depósito', 'Número SII','ID Movimiento','Descripción depósito']

    with pd.ExcelWriter('Group Invoice Matches.xlsx') as writer:
        matches.to_excel(writer, sheet_name='Matches', index=False)