import pandas as pd
from preprocessing import get_preprocessed_invoices_and_movements
from rut_inv_groups import get_invoice_groups
from rut_mov_groups import get_movement_groups_with_counterparty_invoices
from amount_similarity import get_matches_with_similar_amounts
from params import MAX_MOV_DAYS_BEFORE_INV, MAX_MOV_DAYS_AFTER_INV


def main():
    invoices, movements = get_preprocessed_invoices_and_movements()
    inv_groups = get_invoice_groups(invoices)
    mov_groups = get_movement_groups_with_counterparty_invoices(invoices, movements)
    invoices = pd.concat([invoices, inv_groups])
    print("MB usage:", invoices.memory_usage(deep=True)//(1024**2))
    movements = pd.concat([movements, mov_groups])
    candidates = build_candidates_df(invoices, movements)
    print("MB usage:", candidates.memory_usage(deep=True)//(1024**2))



def get_candidate_matches_in_valid_date_range(candidate_matches):
    mov_days_after_inv = (candidate_matches['last_mov_date'] - candidate_matches['first_inv_date']).apply(lambda x: x.days)
    mov_days_before_inv = (candidate_matches['first_inv_date'] - candidate_matches['first_mov_date']).apply(lambda x: x.days)
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
        merged = pd.merge(inv_group, mov_group, on=['rut', 'counterparty_rut']).copy()
        merged = get_candidate_matches_in_valid_date_range(merged)
        merged = get_matches_with_similar_amounts(merged)
        merged = merged[[
            'rut', 'counterparty_rut', 'inv_number', 'inv_group_numbers',
            'mov_id', 'mov_group_ids', 'inv_amount', 'mov_amount', 'amount_similarity'
        ]]
        rows.append(merged)
    return pd.concat(rows, ignore_index=True)

if __name__ == "__main__":
    main()