import pandas as pd
import numpy as np

movies = pd.read_csv('/content/movies.csv')
tv_shows = pd.read_csv('/content/tv_shows.csv')
netflix_titles = pd.read_csv('/content/_netflix_titles.csv')

dfs = [movies, tv_shows, netflix_titles]

fill_values = {
    'description': '',
    'country': 'Not Specified',
    'director': 'Unknown',
    'cast': 'Unknown'
}

for i, df in enumerate(dfs):
    print(df.isnull().sum())

    if 'duration' in df.columns:
        df.drop(columns=['duration'], inplace=True)

    df.fillna(fill_values, inplace=True)

    if 'date_added' in df.columns:
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df['date_added'] = df['date_added'].dt.date

    for col in ['type', 'title']:
        if col in df.columns:
            df[col] = df[col].str.lower().str.strip()

    dfs[i] = df

movies, tv_shows, netflix_titles = dfs

combined = pd.concat([movies, tv_shows], ignore_index=True)


merged_df = pd.merge(
    combined,
    netflix_titles,
    on=['title', 'release_year'],
    how='outer',
    suffixes=('_collected', '_netflix')
)


conflicts = merged_df[
    merged_df['type_collected'].notnull() &
    merged_df['type_netflix'].notnull() &
    (merged_df['type_collected'] != merged_df['type_netflix'])
]


cols_to_clean = [
    'type', 'country', 'description', 'date_added',
    'director', 'cast', 'rating', 'listed_in'
]

for col in cols_to_clean:
    netflix_col = col + '_netflix'
    collected_col = col + '_collected'

    if netflix_col in merged_df.columns and collected_col in merged_df.columns:
        merged_df[col] = merged_df[netflix_col].combine_first(
            merged_df[collected_col]
        )
        merged_df.drop(columns=[netflix_col, collected_col], inplace=True)

final_df = merged_df.drop_duplicates(subset=['title', 'release_year'])

final_df.to_csv('cleaned_netflix_movies.csv', index=False)
