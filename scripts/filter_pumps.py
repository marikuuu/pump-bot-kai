import pandas as pd
df = pd.read_csv('5x_pumpers_list_futures.csv')

# Only keep ones that took 7 to 90 days (real sustained pumps)
# And ignore impossible anomalies > 300x or ones that are basically 0.000...
df_clean = df[(df['days_to_peak'] >= 7) & (df['days_to_peak'] <= 90) & (df['gain_x'] >= 5) & (df['gain_x'] <= 300)]
df_top = df_clean.sort_values('gain_x', ascending=False).head(30)

markdown = df_top.to_string(index=False)
with open('filtered_5x_pumps.txt', 'w', encoding='utf-8') as f:
    f.write(markdown)
