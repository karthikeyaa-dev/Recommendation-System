import pandas as pd
import os

movies_df=pd.read_csv('clean_data/movies_clean.csv')
ratings_df=pd.read_csv('clean_data/ratings_clean.csv')
users_df=pd.read_csv('clean_data/users_clean.csv')

print(movies_df.head())
print(ratings_df.head())
print(users_df.head())

df=ratings_df.merge(users_df, on='user_id').merge(movies_df, on='movie_id')
print(df.head())

os.makedirs('merged_data', exist_ok=True)
df.to_csv('merged_data/merged_dataset.csv', index=False)