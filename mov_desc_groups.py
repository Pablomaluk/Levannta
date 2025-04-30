import pandas as pd
import numpy as np
from preprocessing import get_preprocessed_invoices_and_movements
from main import main
import recordlinkage
import itertools

MAX_GROUP_LEN = 5

def get_mov_groups_with_similar_descriptions():
    movements = get_movements_without_rut_associated_invoices()
    pair_indexes = get_pair_indexes(movements)
    movements = movements.set_index('mov_id')
    mov_pairs = get_similar_movement_pairs(movements, pair_indexes)
    mov_groups = group_movements(movements, mov_pairs)
    return mov_groups

# Hacer este proceso solo para movimientos sin facturas con el mismo rut de contraparte
# La idea es no agrupar de nuevo los movimientos ya revisados antes en etapas más claras
def get_movements_without_rut_associated_invoices():
    invoices, movements = get_preprocessed_invoices_and_movements()
    return movements[~movements['counterparty_rut'].isin(invoices['counterparty_rut'])]

def get_invoices_without_rut_associated_movements():
    invoices, movements = get_preprocessed_invoices_and_movements()
    return invoices[~invoices['counterparty_rut'].isin(movements['counterparty_rut'])]


def get_pair_indexes(movements):
    links = pd.merge(movements,movements, on=["rut", "counterparty_rut"], suffixes=["_left", "_right"])
    links = links[links["mov_date_left"] <= links["mov_date_right"]]
    keep = links.apply(lambda x: True if x['mov_date_left'] < x['mov_date_right'] else x['mov_id_left'] < x['mov_id_right'], axis=1)
    links = links[keep]
    links["date_diff"] = (links["mov_date_right"]-links["mov_date_left"]).apply(lambda x: x.days)
    links = links[links["date_diff"] <= 14]
    links = links.sort_values(by="mov_date_right", ascending=True)
    pair_indexes = pd.MultiIndex.from_frame(links[['mov_id_left', 'mov_id_right']])
    return pair_indexes

def get_similar_movement_pairs(movements, pair_indexes):
    compare = recordlinkage.Compare()
    compare.string('mov_description', 'mov_description',
               method='jarowinkler', threshold=0.95, label='description_sim')
    mov_pairs = compare.compute(pair_indexes, movements)
    mov_pairs = mov_pairs[mov_pairs['description_sim'] >= 0.90].reset_index().drop(columns="description_sim")
    return mov_pairs
    

def group_movements(movements, movement_pairs):
    mov_groups = []
    movement_pairs = pd.merge(movements, movement_pairs, left_on="mov_id", right_on="mov_id_right")
    for first_mov_id, mov_pairs in movement_pairs.groupby('mov_id_left'):
        first_mov = movements.loc[[first_mov_id]]
        first_mov = first_mov.reset_index()
        mov_ids = mov_pairs['mov_id_right'].unique()
        movs = movements.loc[mov_ids].reset_index()
        groups = get_movement_groups(first_mov, movs)
        mov_groups.extend(list(map(lambda x: create_movement_group(x), groups)))
    return pd.DataFrame(mov_groups)

# 2 versiones:
# a) tomar todos los posibles subgrupos
# b) tomar solo en subgrupos consecutivos. Va a perder grupos cuando haya 
#    un grupo donde todos tienen la misma fecha, pero reduce infinitamente 
#    el total de grupos generados. Se puede mitigar posiblemente guardando fecha + tiempo
def get_movement_groups(first_mov, movs):
    # subgroups = []
    # max_group_len = min(MAX_GROUP_LEN, len(movs))
    # first_mov = first_mov.to_dict('records')
    # movs = movs.to_dict('records')
    # for length in range(1, max_group_len+1):
    #     combs = [list(comb) for comb in itertools.combinations(movs, length)]
    #     full_combs = list(map(lambda x: first_mov + x, combs))
    #     subgroups.extend(full_combs)
    # return subgroups

    subgroups = []
    max_group_len = min(MAX_GROUP_LEN, len(movs))
    first_mov = first_mov.to_dict('records')
    movs = movs.to_dict('records')

    for i in range(max(1, len(movs)-4)):
        max_group_len = min(MAX_GROUP_LEN, len(movs)-i)
        for length in range(1,max_group_len+1):
            combs = [list(comb) for comb in itertools.combinations(movs[i:i+max_group_len], length)]
            full_combs = list(map(lambda x: first_mov + x, combs))
            subgroups.extend(full_combs)
    return subgroups

def create_movement_group(movements):
    group = movements[0].copy()
    group['mov_amount'] = sum(map(lambda x: x['mov_amount'], movements))
    group['is_mov_group'] = True
    group['mov_group_ids'] = tuple((map(lambda x: x['mov_id'], movements)))
    group['mov_group_dates'] = tuple(map(lambda x: x['mov_date'], movements))
    group['mov_id'] = np.nan
    group['mov_description'] = np.nan
    return group


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    mov_groups = get_mov_groups_with_similar_descriptions()
    invs = get_invoices_without_rut_associated_movements()
    exact_matches = pd.merge(invs, mov_groups, left_on=["rut", "inv_amount"], right_on=["rut", "mov_amount"])
    exact_matches['date_diff'] = (exact_matches['mov_date']-exact_matches['inv_date']).apply(lambda x: x.days)
    exact_matches = exact_matches[(-14 <= exact_matches['date_diff']) & (exact_matches['date_diff'] <= 90)]
    print(len(mov_groups), len(invs), len(exact_matches))
    with pd.ExcelWriter('descriptions.xlsx') as writer:
        exact_matches.to_excel(writer, sheet_name="Posibles matches exactos", index=False)
        mov_groups.to_excel(writer, sheet_name="Posibles agrupaciones", index=False)
        invs.to_excel(writer, sheet_name="Facturas sin pagos asociables", index=False)
        



