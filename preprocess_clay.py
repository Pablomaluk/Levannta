import pandas as pd
import numpy as np

def get_clay_preprocessed_data():
    df = pd.read_csv('Clay.csv')

    df = df.dropna(subset=['emisor_obligacion_rut'], axis=0)
    df['rut'] = (df['emisor_obligacion_rut'].astype(int).astype(str) + df['emisor_obligacion_dv']).astype(str).apply(lambda x: x.lower())
    df['counterparty_rut'] = (df.apply(
        lambda x: str(int(x['receptor_obligacion_rut'])) + x['receptor_obligacion_dv'] if not pd.isna(x['receptor_obligacion_rut']) else np.nan, axis=1)).astype(str).apply(lambda x: x.lower())
    
    df['descripción'] = df['descripción'].apply(lambda x: x[:30])

    df = df[['fecha_movimiento_humana', 'fecha_emision_obligacion_humana', 'folio',
            'descripción', 'monto_match', 
            'monto_original_movimiento', 'rut', 'counterparty_rut']]

    df.columns = ['mov_date', 'inv_date', 'inv_number',
            'mov_description', 'match_amount', 
            'mov_amount', 'rut', 'counterparty_rut']
    
    with pd.ExcelWriter('Clay.xlsx') as writer:
        df.to_excel(writer, sheet_name="Clay Preprocesado", index=False)
    
    return df

