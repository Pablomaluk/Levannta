import pandas as pd
from toolz import pipe
import preprocessing
import exact_amounts
import similar_amounts
import grouped_invoices
from helpers import save_results
from preprocess_clay import get_clay_preprocessed_data
import re

pd.set_option('display.max_columns', None)

invoices, movements, matches = pipe(
    preprocessing.get_preprocessed_invoices_and_movements(),
    exact_amounts.get_current_dfs,
    similar_amounts.get_current_dfs,
    grouped_invoices.get_current_dfs,
    save_results
)

clay = get_clay_preprocessed_data()
matches['rut'] = matches['rut'].astype(str)
matches['counterparty_rut'] = matches['counterparty_rut'].astype(str)
invoices['rut'] = invoices['rut'].astype(str)
invoices['counterparty_rut'] = invoices['counterparty_rut'].astype(str)
movements['rut'] = movements['rut'].astype(str)
movements['counterparty_rut'] = movements['counterparty_rut'].astype(str)
matches['inv_number'] = matches['inv_number'].apply(lambda x: re.sub(r'\D', '', str(int(x))))
clay['inv_number'] = clay['inv_number'].apply(lambda x: re.sub(r'\D', '', str(x)))
invoices['inv_number'] = invoices['inv_number'].apply(lambda x: re.sub(r'\D', '', str(int(x))))


merge = pd.merge(matches, clay, on=['rut', 'counterparty_rut', 'mov_amount'], suffixes=["_matches","_clay"])
merge = merge[['rut','counterparty_rut', 'inv_amount', 'mov_amount',
               'inv_number_matches', 'inv_number_clay', 
               'inv_date_matches', 'inv_date_clay', 
               'mov_date_matches', 'mov_date_clay', 'mov_description', 'desc1', 'desc2', 'desc3']]


merge = merge[merge['inv_number_matches'] == merge['inv_number_clay']]
missing_matches = matches[~matches['inv_number'].isin(merge['inv_number_matches'])]
missing_clay = clay[~clay['inv_number'].isin(merge['inv_number_matches'])]
pending_invoices = pd.merge(invoices, missing_clay, on=["rut", "counterparty_rut", "inv_number"])
pending_movements = pd.merge(movements, missing_clay, on=["rut", "counterparty_rut", "mov_amount"], suffixes=["", "_clay"])
#pending_movements = pending_movements[pending_movements.apply(lambda x: x[''], axis=0)]

print("Clay:", len(clay), "Matches:", len(matches), "Merge:", len(merge))

with pd.ExcelWriter('Compare.xlsx') as writer:
        merge.to_excel(writer, sheet_name="Matches exitosos", index=False)
        missing_matches.to_excel(writer, sheet_name="Matches faltantes de matches", index=False)
        missing_clay.to_excel(writer, sheet_name="Matches faltantes de clay", index=False)
        invoices.to_excel(writer, sheet_name="Todas las facts", index=False)
        movements.to_excel(writer, sheet_name="Todos los movs", index=False)
        pending_invoices.to_excel(writer, sheet_name="Facts pend existentes", index=False)
        pending_movements.to_excel(writer, sheet_name="Movs pend existentes", index=False)