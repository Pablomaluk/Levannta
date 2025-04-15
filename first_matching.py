import pandas as pd
import numpy as np
import itertools
import datetime as dt
from Preprocessing.preprocessing import get_preprocessed_invoices_and_movements
from helpers import read_stage_dfs, save_stage_dfs, set_exact_match_params, print_matches_percentage_per_rut

def get_first_matches_and_pending_invoices_and_movements():
    try:
        matches, pending_invoices, pending_movements = read_stage_dfs(1)
    except FileNotFoundError:
        invoices, movements = get_preprocessed_invoices_and_movements()
        movements = get_combined_movements_and_movement_groups(invoices, movements)
        matches = get_exact_matches(invoices, movements)
        matches_dict = assign_matches(matches)
        matches = get_matches_from_dict(invoices, movements, matches_dict)
        matches = set_exact_match_params(matches)
        pending_invoices, pending_movements = get_pending_invoices_and_movements(invoices, movements, matches_dict)
        save_stage_dfs(matches, pending_invoices, pending_movements, 1)
    finally:
        print('Matching round 1')
        print_matches_percentage_per_rut(matches, pending_invoices, pending_movements)
        return matches, pending_invoices, pending_movements

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


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    get_first_matches_and_pending_invoices_and_movements()