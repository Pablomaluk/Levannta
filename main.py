import pandas as pd
from toolz import pipe
import preprocessing
import exact_amounts
import similar_amounts
import grouped_invoices
from helpers import save_results

def main():
    return pipe(
        preprocessing.get_preprocessed_invoices_and_movements(),
        exact_amounts.get_current_dfs,
        similar_amounts.get_current_dfs,
        grouped_invoices.get_current_dfs,
        save_results
    )
