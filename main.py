import pandas as pd
import os
from toolz import pipe
import preprocessing
import exact_amounts
import similar_amounts
import grouped_invoices

def main():
    pipe(
        preprocessing.get_preprocessed_invoices_and_movements(),
        exact_amounts.get_current_dfs,
        similar_amounts.get_current_dfs,
        grouped_invoices.get_current_dfs

    )

main()