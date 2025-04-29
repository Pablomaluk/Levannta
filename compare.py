import pandas as pd
import datetime as dt
import re
from preprocess_clay import get_clay_preprocessed_data
from main import main
from preprocessing import get_preprocessed_invoices_and_movements

pd.set_option('display.max_columns', None)

def fix_column_types(invoices, movements, matches, clay):
    matches['rut'] = matches['rut'].astype(str)
    matches['counterparty_rut'] = matches['counterparty_rut'].astype(str)
    invoices['rut'] = invoices['rut'].astype(str)
    invoices['counterparty_rut'] = invoices['counterparty_rut'].astype(str)
    movements['rut'] = movements['rut'].astype(str)
    movements['counterparty_rut'] = movements['counterparty_rut'].astype(str)
    matches['inv_number'] = matches['inv_number'].apply(lambda x: re.sub(r'\D', '', str(int(x))))
    clay['inv_number'] = clay['inv_number'].apply(lambda x: re.sub(r'\D', '', str(x)))
    invoices['inv_number'] = invoices['inv_number'].apply(lambda x: re.sub(r'\D', '', str(int(x))))
    matches['mov_date'] = pd.to_datetime(matches['mov_date']).dt.date
    matches['inv_date'] = pd.to_datetime(matches['inv_date']).dt.date
    clay['mov_date'] = pd.to_datetime(clay['mov_date']).dt.date
    clay['inv_date'] = pd.to_datetime(clay['inv_date']).dt.date
    return invoices, movements, matches, clay

def get_correct_matches(matches, clay):
    merge = pd.merge(matches, clay, on=['rut', 'counterparty_rut', 'mov_amount', 'inv_number'], suffixes=["_matches","_clay"])
    merge = merge[['rut','counterparty_rut', 'inv_amount', 'mov_amount', 'inv_number',
               'inv_date_matches', 'inv_date_clay', 'mov_date_matches', 'mov_date_clay', 
               'mov_description_matches', 'mov_description_clay']]
    merge['day_diff'] = (merge['mov_date_matches'] - merge['mov_date_clay']).apply(lambda x: abs(x.days))
    merge = merge.sort_values(by=['day_diff'], ascending=False)
    return merge

def get_missing_matches(matches, clay, merge):
    missing_clay = clay[~clay['inv_number'].isin(merge['inv_number'])]
    wrong_matches = matches[matches['inv_number'].isin(missing_clay['inv_number'])]
    wrong_matches = pd.merge(wrong_matches, missing_clay, on=["rut", "counterparty_rut", "inv_number"], suffixes=["_match", "_clay"])
    missing_matches = matches[~matches['inv_number'].isin(merge['inv_number']) & ~matches['inv_number'].isin(wrong_matches['inv_number'])]
    pending_invoices = pd.merge(invoices, missing_clay, on=["rut", "counterparty_rut", "inv_number"])
    pending_movements = pd.merge(movements, missing_clay, on=["rut", "counterparty_rut", "mov_amount"], suffixes=["", "_clay"])
    return wrong_matches, missing_matches, missing_clay
#pending_movements = pending_movements[pending_movements.apply(lambda x: x[''], axis=0)]

def save_comp(merge, wrong_matches, missing_matches, missing_clay, all_invs, all_movs):
    with pd.ExcelWriter('Compare.xlsx') as writer:
        merge.to_excel(writer, sheet_name="Matches exitosos", index=False)
        wrong_matches.to_excel(writer, sheet_name="Matches distintos", index=False)
        missing_matches.to_excel(writer, sheet_name="Matches faltantes en Clay", index=False)
        missing_clay.to_excel(writer, sheet_name="Matches de Clay pendientes", index=False)
        all_invs.to_excel(writer, sheet_name="Todas las facts BQ", index=False)
        all_movs.to_excel(writer, sheet_name="Todos los movs BQ", index=False)
        # invoices.to_excel(writer, sheet_name="Todas las facts", index=False)
        # movements.to_excel(writer, sheet_name="Todos los movs", index=False)
        # pending_invoices.to_excel(writer, sheet_name="Facts pend existentes", index=False)
        # pending_movements.to_excel(writer, sheet_name="Movs pend existentes", index=False)


if __name__ == "__main__":
    all_invs, all_movs = get_preprocessed_invoices_and_movements()
    all_movs = all_movs[all_movs['rut'] == 761341235]
    min_mov_date = all_movs['mov_date'].min()
    print(min_mov_date, type(min_mov_date))
    invoices, movements, matches = main()
    clay = get_clay_preprocessed_data()
    invoices, movements, matches, clay = fix_column_types(invoices, movements, matches, clay)
    invoices = invoices[invoices['rut'] == '761341235']
    movements = movements[movements['rut'] == '761341235']
    all_invs = all_invs[all_invs['rut'] == 761341235]
    matches = matches[matches['rut'] == '761341235']
    clay = clay[(clay['rut'] == '761341235') & (clay['mov_date'] >= min_mov_date)]
    #clay = clay[clay['rut'] == '761341235']
    merge = get_correct_matches(matches, clay)
    wrong_matches, missing_matches, missing_clay = get_missing_matches(matches, clay, merge)
    print("Clay:", len(clay))
    print("Merge:", len(merge))
    print("Incorrectos:", len(wrong_matches))
    print("Matches no existentes en Clay:", len(missing_matches))
    print("Matches de Clay pendientes:", len(missing_clay))
    print("Facturas pendientes:", len(all_invs))
    print("Movimientos pendientes:", len(all_movs))
    save_comp(merge, wrong_matches, missing_matches, missing_clay, all_invs, all_movs)
