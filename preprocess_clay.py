import pandas as pd
import numpy as np

def get_clay_preprocessed_data():
    df = pd.read_csv('Clay.csv')

    df = df.dropna(subset=['emisor_obligacion_rut'], axis=0)
    df['rut'] = (df['emisor_obligacion_rut'].astype(int).astype(str) + df['emisor_obligacion_dv']).astype(str).apply(lambda x: x.lower())
    df['counterparty_rut'] = (df.apply(
        lambda x: str(int(x['receptor_obligacion_rut'])) + x['receptor_obligacion_dv'] if not pd.isna(x['receptor_obligacion_rut']) else np.nan, axis=1)).astype(str).apply(lambda x: x.lower())

    df = df[['fecha_movimiento_humana', 'fecha_emision_obligacion_humana', 'folio',
            'descripción', 'monto_match', 
            'monto_original_movimiento', 'comentario',
            'descripcion_primer_item', 'rut', 'counterparty_rut']]

    df.columns = ['mov_date', 'inv_date', 'inv_number',
            'desc1', 'match_amount', 
            'mov_amount', 'desc2',
            'desc3', 'rut', 'counterparty_rut']
    
    with pd.ExcelWriter('Clay.xlsx') as writer:
        df.to_excel(writer, sheet_name="Clay Preprocesado", index=False)
    
    return df

