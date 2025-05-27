import pandas as pd
import re
from preprocess_clay import get_clay_preprocessed_data
from main import main
from preprocessing import get_preprocessed_invoices_and_movements
from sklearn.model_selection import ParameterGrid
import params

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

def eval_matches(invoices, matches, clay):
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
    total_invs = invoices.groupby(["rut", "inv_number"]).ngroups
    matched_invs = matches.groupby(["rut", "inv_number"]).ngroups
    print(f"Correct matches: {round(100*len(correct)/len(merge),2)}%")
    print(f"Incorrect matches: {round(100*len(mismatch)/len(merge),2)}%")
    print(f"Matches only in matches: {round(100*len(extra_matches)/len(merge),2)}%")
    print(f"Matches only in clay: {round(100*len(missing_clay)/len(merge),2)}%")
    print(f"Precision: {round(100*len(correct)/(len(correct)+len(mismatch)),2)}%")
    print(f"Coverage: {100*matched_invs/total_invs}%")
    return correct, mismatch, missing_clay, extra_matches

def calculate_score(invoices, matches, clay):
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
    total_invs = invoices.groupby(["rut", "inv_number"]).ngroups
    matched_invs = matches.groupby(["rut", "inv_number"]).ngroups
    print(f"Precision: {round(100*len(correct)/(len(correct)+len(mismatch)),2)}%")
    print(f"Coverage: {100*matched_invs/total_invs}%")
    precision = len(correct)/(len(correct)+len(mismatch))
    coverage = matched_invs/total_invs
    score = 2*precision*coverage/(precision+coverage)
    return score

def tune_params():
    invoices, movements = get_preprocessed_invoices_and_movements()
    clay = get_clay_preprocessed_data()
    best_score = -1.0
    best_cfg   = None
    # grid = {"MAX_GROUP_LEN": [5,6],
    #         "MAX_GROUP_DATE_DIFF": [90, 95, 100],
    #         "MAX_MOV_DAYS_BEFORE_INV": [10, 14, 20],
    #         "MAX_MOV_DAYS_AFTER_INV": [90,100,110],
    #         "MAX_REL_AMOUNT_DIFF": [0.02, 0.03, 0.04],
    #         "GAUSSIAN_SIMILARITY_SCALE": [0.0005, 0.001, 0.005],
    #         "MAX_GREEDY_ITERATIONS": [3,4]}
    grid = {"MAX_GROUP_LEN": [5],
            "MAX_GROUP_DATE_DIFF": [365],
            "MAX_MOV_DAYS_BEFORE_INV": [30],
            "MAX_MOV_DAYS_AFTER_INV": [145],
            "MAX_REL_AMOUNT_DIFF": [0.0012],
            "GAUSSIAN_SIMILARITY_SCALE": [0.0000025],
            "MAX_GREEDY_ITERATIONS": [4]}
    
    counter = 1
    pg = ParameterGrid(grid)

    for cfg in pg:
        for name, val in cfg.items():
            setattr(params, name, val)

        _, __, matches = main()
        invoices, movements, matches, clay = fix_column_types(invoices, movements, matches, clay)
        score = calculate_score(invoices, matches, clay)
        print(f"Config {counter}/{len(pg)}", "Score:", score, "Best score:", best_score)
        if score > best_score:
            best_score = score
            best_cfg   = cfg.copy()
            print("Best params:", best_cfg)
            best_cfg = pd.DataFrame([best_cfg]).to_csv('best_params.csv', index=False) 
        counter += 1
    
def save_comp(merge, wrong_matches, missing_matches, missing_clay, all_invs, all_movs):
    with pd.ExcelWriter('Compare.xlsx') as writer:
        merge.to_excel(writer, sheet_name="Matches exitosos", index=False)
        wrong_matches.to_excel(writer, sheet_name="Matches distintos", index=False)
        missing_matches.to_excel(writer, sheet_name="Matches faltantes en Clay", index=False)
        missing_clay.to_excel(writer, sheet_name="Matches de Clay pendientes", index=False)
        all_invs.to_excel(writer, sheet_name="Todas las facts", index=False)
        all_movs.to_excel(writer, sheet_name="Todos los movs", index=False)

if __name__ == "__main__":
    # invoices, movements = get_preprocessed_invoices_and_movements()
    # _, __, matches = main()
    # print(type(matches), matches.head(5))
    # clay = get_clay_preprocessed_data()
    # invoices, movements, matches, clay = fix_column_types(invoices, movements, matches, clay)
    # #clay = clay[clay['rut'] != '763614220']
    # matches = matches[MATCH_COLUMNS]
    # correct, mismatch, missing_clay, extra_matches = eval_matches(invoices, matches, clay)
    # save_comp(correct, mismatch, extra_matches, missing_clay, invoices, movements)
    tune_params()
