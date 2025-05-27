import pandas as pd
import datetime as dt
import re
from preprocess_clay import get_clay_preprocessed_data
from main import main
from preprocessing import get_preprocessed_invoices_and_movements

pd.set_option('display.max_columns', None)

MATCH_COLUMNS = ["rut", "counterparty_rut", "inv_date", "mov_date", "mov_description",
                "inv_number", "inv_amount", "mov_amount", "mov_id"]
MERGE_COLUMNS = ['rut', 'counterparty_rut', 'inv_number', 'inv_date_match', 'inv_date_clay',
                'mov_date_match', 'mov_date_clay', 'inv_amount', 'mov_amount_match', 'mov_amount_clay', '_merge']
TOLERANCE = 7

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

def eval_matches(matches, clay):
    print(len(matches), len(clay))
    merge = clay.merge(matches, on=['rut','inv_number', 'counterparty_rut'], how='outer', 
                       suffixes=('_clay','_match'), indicator=True)
    merge = merge[MERGE_COLUMNS]
    merge['mov_date_clay'] = pd.to_datetime(merge['mov_date_clay'], errors='coerce')
    merge['mov_date_match'] = pd.to_datetime(merge['mov_date_match'], errors='coerce')
    merge['date_diff'] = (merge['mov_date_clay']-merge['mov_date_match']).abs().dt.days
    correct = merge[(merge['_merge']=='both') &
                    (merge['mov_amount_clay'] == merge['mov_amount_match']) &
                    (merge['date_diff'] <= TOLERANCE)]
    correct = correct.sort_values(by='date_diff', ascending=False).drop(columns='_merge')
    mismatch = merge[(merge['_merge']=='both') & ~(merge.index.isin(correct.index))]
    missing_clay = merge[merge['_merge']=='left_only']
    extra_matches    = merge[merge['_merge']=='right_only']
    print("Correct matches:", 100*len(correct)/len(merge))
    print("Incorrect matches:", 100*len(mismatch)/len(merge))
    #print("Matches evaluated:", 100*len(merge)/len(matches))
    print("Matches only in matches:", 100*len(extra_matches)/len(merge))
    print("Matches only in clay:", 100*len(missing_clay)/len(merge))
    return correct, mismatch, missing_clay, extra_matches
    

def save_comp(merge, wrong_matches, missing_matches, missing_clay, all_invs, all_movs):
    with pd.ExcelWriter('Compare.xlsx') as writer:
        merge.to_excel(writer, sheet_name="Matches exitosos", index=False)
        wrong_matches.to_excel(writer, sheet_name="Matches distintos", index=False)
        missing_matches.to_excel(writer, sheet_name="Matches faltantes en Clay", index=False)
        missing_clay.to_excel(writer, sheet_name="Matches de Clay pendientes", index=False)
        all_invs.to_excel(writer, sheet_name="Todas las facts", index=False)
        all_movs.to_excel(writer, sheet_name="Todos los movs", index=False)

if __name__ == "__main__":
    invoices, movements = get_preprocessed_invoices_and_movements()
    _, __, matches = main()
    print(type(matches), matches.head(5))
    clay = get_clay_preprocessed_data()
    invoices, movements, matches, clay = fix_column_types(invoices, movements, matches, clay)
    #clay = clay[clay['rut'] != '763614220']
    matches = matches[MATCH_COLUMNS]
    correct, mismatch, missing_clay, extra_matches = eval_matches(matches, clay)
    save_comp(correct, mismatch, extra_matches, missing_clay, invoices, movements)
