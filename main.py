import pandas as pd
from time import time
from recordlinkage.index import SortedNeighbourhood
from preprocessing import get_preprocessed_invoices_and_movements
from inv_groups import get_invoice_groups
from mov_groups import get_movement_groups
from amount_similarity import get_matches_with_similar_amounts
from ilp import optimize
import params

def main():
    invoices, movements = get_preprocessed_invoices_and_movements()
    # invoices = invoices[invoices['rut'] != 763614220]
    # movements = movements[movements['rut'] != 763614220]
    return match_with_counterparty_rut(invoices, movements)
    #match_without_counterparty_rut(invoices, movements)

def match_with_counterparty_rut(invoices, movements):
    tiempo = time()
    invoices = invoices[invoices['counterparty_rut'].isin(movements['counterparty_rut'])]
    movements = movements[movements['counterparty_rut'].isin(invoices['counterparty_rut'])]
    inv_num_map = map_invoices(invoices)
    mov_id_map = map_movements(movements)
    invoices = get_invoices_and_invoice_groups(invoices)
    movements = get_movements_and_movement_groups(movements)
    print("Getting candidates")
    pair_indexes = get_candidate_pairs(invoices, movements)
    invoices, movements = get_useful_columns(invoices, movements)
    print("Building candidates")
    candidates = build_and_filter_candidate_pairs(invoices, movements, pair_indexes)
    print("Optimizing candidates")
    matches = optimize(candidates)
    print("Tiempo total", time()-tiempo)
    return save_results(matches, inv_num_map, mov_id_map)

def map_invoices(invoices):
    inv_num_map = {v: i for i, v in enumerate(invoices[['rut', 'inv_number']].drop_duplicates().itertuples(index=False, name=None))}
    invoices.loc[:, 'inv_number'] = invoices.apply(lambda row: inv_num_map[(row['rut'], row['inv_number'])], axis=1)
    return inv_num_map    

def map_movements(movements):
    mov_id_map = {v: i for i, v in enumerate(movements['mov_id'].unique())}
    movements.loc[:, 'mov_id'] = movements['mov_id'].map(lambda x: mov_id_map[x])
    return mov_id_map

def get_invoices_and_invoice_groups(invoices):
    invoices = invoices.copy()
    groups = get_invoice_groups(invoices)
    invoices['inv_group_numbers'] = invoices['inv_number'].map(lambda x: [x])
    return pd.concat([invoices, groups]).reset_index(drop=True)

def get_movements_and_movement_groups(movements):
    movements = movements.copy()
    groups = get_movement_groups(movements)
    movements['mov_group_ids'] = movements['mov_id'].map(lambda x: [x])
    return pd.concat([movements, groups]).reset_index(drop=True)

def get_candidate_pairs(invoices, movements):
    invoices['amt_bin'] = invoices['inv_amount'] // params.AMOUNT_BIN
    movements['amt_bin'] = movements['mov_amount'] // params.AMOUNT_BIN
    inv_groups = invoices.groupby(['amt_bin','rut','counterparty_rut']).groups
    mov_groups = movements.groupby(['amt_bin','rut','counterparty_rut']).groups
    common_keys = set(inv_groups.keys()) & set(mov_groups.keys())
    pairs = []

    for key in common_keys:
        inv_group = invoices.loc[inv_groups[key]]
        mov_group = movements.loc[mov_groups[key]]
        pairs.extend(
            SortedNeighbourhood(left_on='inv_amount', right_on='mov_amount', window=params.WINDOW_SIZE)
            .index(inv_group, mov_group)
        )
    return pairs

def get_useful_columns(invoices, movements):
    invoices = invoices[['rut', 'counterparty_rut', 'inv_amount', 'first_inv_date', 'last_inv_date',
                         'inv_group_len', 'inv_group_numbers']]
    movements = movements[['rut', 'counterparty_rut', 'mov_amount', 'first_mov_date', 'last_mov_date',
                         'mov_group_len', 'mov_group_ids']]
    return invoices, movements

def build_and_filter_candidate_pairs(invoices, movements, indexes):
    df = pd.DataFrame(indexes, columns=['inv_index','mov_index'])
    df = pd.merge(df, invoices, left_on='inv_index', right_index=True).drop(columns=['rut', 'counterparty_rut', 'inv_index'])
    df = pd.merge(df, movements, left_on='mov_index', right_index=True).drop(columns=['mov_index'])
    df = get_candidates_in_valid_date_range(df)
    df = get_matches_with_similar_amounts(df)
    return df[[
        'inv_group_numbers','mov_group_ids',
        'amount_similarity','date_diff'
    ]]

def get_candidates_in_valid_date_range(candidates):
    mov_days_after_inv = (candidates['last_mov_date'] - candidates['first_inv_date']).apply(lambda x: x.days)
    mov_days_before_inv = (candidates['first_inv_date'] - candidates['first_mov_date']).apply(lambda x: x.days)
    max_diff = (candidates['last_inv_date'] - candidates['first_mov_date']).apply(lambda x: abs(x.days))
    candidates['date_diff'] = pd.concat([mov_days_after_inv.abs(), mov_days_before_inv.abs(), max_diff], axis=1).max(axis=1)
    return candidates[
        (mov_days_after_inv <= params.MAX_MOV_DAYS_AFTER_INV) &
        (mov_days_before_inv <= params.MAX_MOV_DAYS_BEFORE_INV)
    ]

def save_results(matches, inv_id_map, mov_id_map):
    invoices, movements = get_preprocessed_invoices_and_movements()
    res = []
    inv_map = {v:k for k,v in inv_id_map.items()}
    mov_map = {v:k for k,v in mov_id_map.items()}
    for i, row in matches.iterrows():
        if len(row['inv_group_numbers']) == 1 or len(row['mov_group_ids']) == 1:
            for inv in row.inv_group_numbers:
                for mov in row.mov_group_ids:
                    res.append({'rut':inv_map[inv][0],'inv_number':inv_map[inv][1], 'mov_id':mov_map[mov], 'score':row.score}) #'amount_match':min(mov['mov_amount'], inv['inv_amount']),
        else:
            continue
            group_invs = pd.DataFrame([{'rut': inv_map[inv_id][0], 'inv_number': inv_map[inv_id][1]} for inv_id in row.inv_group_numbers])
            group_invs = pd.merge(invoices, group_invs, on=["rut", "inv_number"]).sort_values(by="inv_number")
            group_movs = pd.DataFrame([{'mov_id': mov_map[mov_id]} for mov_id in row.mov_group_ids])
            group_movs = pd.merge(movements, group_movs, on=["mov_id"]).sort_values(by="mov_date")
            mov_index = 0
            #print(group_invs)
            #print(group_movs)
            mov_remaining = group_movs.iloc[0]['mov_amount']
            for _, inv in group_invs.iterrows():
                inv_remaining = inv['inv_amount']
                #print("Inv amount:",inv['inv_amount'], "Inv remaining:",inv_remaining, "Mov amount:",group_movs.iloc[mov_index]['mov_amount'], "Mov remaining:", mov_remaining)
                while inv_remaining > 0 and mov_index < len(group_movs):
                    alloc = min(inv_remaining, mov_remaining)
                    res.append({'rut':inv['rut'], 'inv_number':inv['inv_number'], 'mov_id': group_movs.iloc[mov_index]['mov_id'], 'amount_match': alloc,'score': row['score']})
                    inv_remaining  -= alloc
                    mov_remaining  -= alloc
                    if mov_remaining <= 0:
                        mov_index += 1
                        if mov_index < len(group_movs):
                            mov_remaining = group_movs.iloc[mov_index]['mov_amount']

    matches = pd.DataFrame(res)
    matches = pd.merge(invoices, matches, on=['rut', 'inv_number'])
    matches = pd.merge(movements, matches, on=['rut', 'counterparty_rut', 'mov_id'], suffixes=["_",""])
    return matches

def match_without_counterparty_rut(invoices, movements):
    pass


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    main()