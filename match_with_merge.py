import pandas as pd
from time import time
from preprocessing import get_preprocessed_invoices_and_movements
from inv_groups import get_invoice_groups
from mov_groups import get_movement_groups
from amount_similarity import get_matches_with_similar_amounts
from ilp import assign
from params import MAX_MOV_DAYS_BEFORE_INV, MAX_MOV_DAYS_AFTER_INV


def main():
    start = time()
    invoices, movements = get_preprocessed_invoices_and_movements()
    invoices = invoices[invoices['counterparty_rut'].isin(movements['counterparty_rut'])]
    movements = movements[movements['counterparty_rut'].isin(invoices['counterparty_rut'])]
    mov_id_map = {v: i for i, v in enumerate(movements['mov_id'].unique())}
    inv_id_map = {v: i for i, v in enumerate(invoices[['rut', 'inv_number']].drop_duplicates().itertuples(index=False, name=None))}
    invoices, movements = get_mapped_invoices_and_movements(invoices, movements, inv_id_map, mov_id_map)
    print(len(invoices), len(movements))
    candidates = build_candidates_df(invoices, movements)
    print(len(candidates))
    candidates.to_parquet('Candidates.parquet', index=False)
    #candidates = pd.read_parquet("Candidates.parquet")
    matches = assign(candidates)
    print(len(matches))
    # matches = pd.read_parquet("Results.parquet")
    print("Tiempo total", time()-start)
    return save_results(matches, inv_id_map, mov_id_map)

def get_mapped_invoices_and_movements(invoices, movements, inv_id_map, mov_id_map):
    inv_groups = get_invoice_groups(invoices)
    mov_groups = get_movement_groups(movements)

    invoices['inv_group_numbers'] = invoices.apply(lambda row: [inv_id_map[(row['rut'], row['inv_number'])]], axis=1)
    movements['mov_group_ids'] = movements['mov_id'].map(lambda x: [mov_id_map[x]])
    inv_groups['inv_group_numbers'] = inv_groups.apply(
        lambda row: [inv_id_map[(row['rut'], inv)] for inv in row['inv_group_numbers']], axis=1)
    mov_groups['mov_group_ids'] = mov_groups['mov_group_ids'].apply(lambda lst: [mov_id_map[m] for m in lst])

    invoices = pd.concat([invoices, inv_groups])
    movements = pd.concat([movements, mov_groups])
    invoices = invoices.drop(columns='inv_number')
    movements = movements.drop(columns='mov_id')
    return invoices, movements

def get_candidate_matches_in_valid_date_range(candidate_matches):
    mov_days_after_inv = (candidate_matches['last_mov_date'] - candidate_matches['first_inv_date']).apply(lambda x: x.days)
    mov_days_before_inv = (candidate_matches['first_inv_date'] - candidate_matches['first_mov_date']).apply(lambda x: x.days)
    max_diff = (candidate_matches['last_inv_date'] - candidate_matches['first_mov_date']).apply(lambda x: abs(x.days))
    candidate_matches['date_diff'] = pd.concat([mov_days_after_inv.abs(), mov_days_before_inv.abs(), max_diff], axis=1).max(axis=1)
    return candidate_matches[
        (mov_days_after_inv <= MAX_MOV_DAYS_AFTER_INV) &
        (mov_days_before_inv <= MAX_MOV_DAYS_BEFORE_INV)
    ]

def build_candidates_df(invoices, movements):
    rows = []
    mov_group_dict = {key: group for key, group in movements.groupby(['rut', 'counterparty_rut'])}
    for key, inv_group in invoices.groupby(['rut', 'counterparty_rut']):
        if key not in mov_group_dict:
            continue
        mov_group = mov_group_dict[key]
        merged = pd.merge(inv_group, mov_group, on=['rut', 'counterparty_rut'])
        merged = get_candidate_matches_in_valid_date_range(merged)
        merged = get_matches_with_similar_amounts(merged)
        merged['match_size'] = merged['mov_group_len']*merged['inv_group_len']
        merged = merged[['inv_group_numbers', 'mov_group_ids','amount_similarity', 'match_size', 'date_diff']]
        rows.append(merged)
    return pd.concat(rows, ignore_index=True)

def save_results(matches, inv_id_map, mov_id_map):
    invoices, movements = get_preprocessed_invoices_and_movements()
    res = []
    inv_map = {v:k for k,v in inv_id_map.items()}
    mov_map = {v:k for k,v in mov_id_map.items()}
    for i, row in matches.iterrows():
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

if __name__ == '__main__':
    main()