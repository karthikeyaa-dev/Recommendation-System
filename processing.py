import pandas as pd
import os

items_df=pd.read_csv('processed_data/items.csv')
ratings_df=pd.read_csv('processed_data/ratings.csv')
users_df=pd.read_csv('processed_data/users.csv')

movies_df=items_df.drop(columns=['video_release_date', 'imdb_url'])
movies_df = movies_df.dropna(subset=['release_date'])
movies_df['release_date']=pd.to_datetime(movies_df['release_date'], format='%d-%b-%Y')
movies_df['title']=movies_df['title'].str.replace(r'\(\d{4}\)', '', regex=True).str.strip()
print(movies_df.head())
print(movies_df.describe())
print(movies_df.info())

ratings_df['rating_date']=pd.to_datetime(ratings_df['unix_timestamp'], unit='s')
ratings_df=ratings_df.drop(columns=['unix_timestamp'])
ratings_df=ratings_df.dropna(subset=['rating_date'])
print(ratings_df.head())
print(ratings_df.describe())
print(ratings_df.info())

users_df['sex'] = users_df['sex'].astype('category')
users_df['occupation'] = users_df['occupation'].astype('category')
users_df = pd.get_dummies(users_df, columns=['sex', 'occupation'])
users_df = users_df.drop(columns=['zip_code'])
users_df['age_group'] = pd.cut(
    users_df['age'],
    bins=[0, 12, 18, 25, 35, 45, 100],
    labels=['Child', 'Teen', 'YoungAdult', 'Adult', 'MidAge', 'Senior']
)
users_df=pd.get_dummies(users_df, columns=['age_group'])
print(users_df.head())
print(users_df.describe())
print(users_df.head())

print("Duplicate movies:", movies_df.duplicated().sum())
print("Duplicate ratings:", ratings_df.duplicated().sum())
print("Duplicate users:", users_df.duplicated().sum())

os.makedirs('clean_data', exist_ok=True)
movies_df.to_csv('clean_data/movies_clean.csv', index=False)
ratings_df.to_csv('clean_data/ratings_clean.csv', index=False)
users_df.to_csv('clean_data/users_clean.csv', index=False)
print("\n✅ Cleaned data saved to 'clean_data' directory.")