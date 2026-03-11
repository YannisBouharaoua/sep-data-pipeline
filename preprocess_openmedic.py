import pandas as pd
import sys

def clean_french_number(val):
    if pd.isna(val):
        return val
    s = str(val).strip()
    # Supprime les points de milliers, remplace virgule décimale par point
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return None

files = ['openmedic_2019', 'openmedic_2020', 'openmedic_2021', 'openmedic_2022']

for name in files:
    print(f"Traitement {name}...")
    df = pd.read_csv(f'{name}.csv', sep=';', dtype=str, low_memory=False, encoding='latin-1')
    for col in ['REM', 'BSE', 'BOITES']:
        if col in df.columns:
            df[col] = df[col].apply(clean_french_number)
    out = f'{name}_clean.csv'
    df.to_csv(out, sep=';', index=False)
    print(f"  ✓ {out} ({len(df)} lignes)")

print("Done.")
