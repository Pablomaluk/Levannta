import pandas as pd
from time import time
from recordlinkage.index import SortedNeighbourhood
from preprocessing import get_preprocessed_invoices_and_movements
from inv_groups import get_invoice_groups
from mov_groups import get_movement_groups
from amount_similarity import get_matches_with_similar_amounts
from assign import assign
from params import MAX_MOV_DAYS_BEFORE_INV, MAX_MOV_DAYS_AFTER_INV, AMOUNT_BIN, WINDOW_SIZE

def main():
    invoices, movements = get_preprocessed_invoices_and_movements()
    invoices = invoices[invoices['rut'] != 763614220]
    movements = movements[movements['rut'] != 763614220]
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
    pair_indexes = get_candidate_pairs(invoices, movements)
    invoices, movements = get_useful_columns(invoices, movements)
    candidates = build_and_filter_candidate_pairs(invoices, movements, pair_indexes)
    matches = assign(candidates)
    print(len(matches[
        (matches['inv_group_numbers'].map(lambda x: len(x)) > 1) & 
        (matches['mov_group_ids'].map(lambda x: len(x)) > 1)
        ]))
    print(matches[(matches['inv_group_numbers'].map(lambda x: len(x)) > 1) & (matches['mov_group_ids'].map(lambda x: len(x)) > 1)].head(5))
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
    invoices['amt_bin'] = invoices['inv_amount'] // AMOUNT_BIN
    movements['amt_bin'] = movements['mov_amount'] // AMOUNT_BIN
    inv_groups = invoices.groupby(['amt_bin','rut','counterparty_rut']).groups
    mov_groups = movements.groupby(['amt_bin','rut','counterparty_rut']).groups
    common_keys = set(inv_groups.keys()) & set(mov_groups.keys())
    pairs = []

    for key in common_keys:
        inv_group = invoices.loc[inv_groups[key]]
        mov_group = movements.loc[mov_groups[key]]
        pairs.extend(
            SortedNeighbourhood(left_on='inv_amount', right_on='mov_amount', window=WINDOW_SIZE)
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
    df['match_size'] = df.inv_group_len * df.mov_group_len
    return df[[
        'inv_group_numbers','mov_group_ids',
        'amount_similarity','match_size','date_diff'
    ]]

def get_candidates_in_valid_date_range(candidates):
    mov_days_after_inv = (candidates['last_mov_date'] - candidates['first_inv_date']).apply(lambda x: x.days)
    mov_days_before_inv = (candidates['first_inv_date'] - candidates['first_mov_date']).apply(lambda x: x.days)
    max_diff = (candidates['last_inv_date'] - candidates['first_mov_date']).apply(lambda x: abs(x.days))
    candidates['date_diff'] = pd.concat([mov_days_after_inv.abs(), mov_days_before_inv.abs(), max_diff], axis=1).max(axis=1)
    return candidates[
        (mov_days_after_inv <= MAX_MOV_DAYS_AFTER_INV) &
        (mov_days_before_inv <= MAX_MOV_DAYS_BEFORE_INV)
    ]

def save_results(matches, inv_id_map, mov_id_map):
    invoices, movements = get_preprocessed_invoices_and_movements()
    res = []
    inv_map = {v:k for k,v in inv_id_map.items()}
    mov_map = {v:k for k,v in mov_id_map.items()}
    for i, row in matches.iterrows():
        if len(row['inv_group_numbers']) == 1 or len(row['mov_group_ids']) == 1:
            continue
            for inv in row.inv_group_numbers:
                for mov in row.mov_group_ids:
                    res.append({'rut':inv_map[inv][0],'inv_number':inv_map[inv][1], 'mov_id':mov_map[mov], 'score':row.score})
        else:
            for inv in row.inv_group_numbers:
                for mov in row.mov_group_ids:
                    res.append({'rut':inv_map[inv][0],'inv_number':inv_map[inv][1], 'mov_id':mov_map[mov], 'score':row.score})
    matches = pd.DataFrame(res)
    matches = pd.merge(invoices, matches, on=['rut', 'inv_number'])
    matches = pd.merge(movements, matches, on=['rut', 'counterparty_rut', 'mov_id'], suffixes=["_",""])
    facturas_conciliadas = matches.groupby(['rut','inv_number']).ngroups
    facturas_totales = invoices.groupby(['rut','inv_number']).ngroups
    print(f"Facturas totales: {facturas_totales}, Facturas conciliadas: {facturas_conciliadas}, Conciliación: {100*facturas_conciliadas/facturas_totales}%")
    with pd.ExcelWriter('Matches.xlsx') as writer:
        matches.to_excel(writer, sheet_name="Matches", index=False)
    return matches


def match_without_counterparty_rut(invoices, movements):
    pass


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    main()