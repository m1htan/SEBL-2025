import pandas as pd

df = pd.read_csv('/Users/minhtan/Documents/GitHub/SEBL-2025/data/data_clean/volume_A/Q1_(b).csv')
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df)