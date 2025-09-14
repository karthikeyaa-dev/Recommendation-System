import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import lightgbm as lgb
import numpy as np



df=pd.read_csv('merged_data/merged_dataset.csv')
print(df.head())

# Drop columns not needed for modeling
drop_cols = ['title', 'rating_date']  # You can drop 'title' and 'rating_date' for now

data = df.drop(columns=drop_cols)

# Define X and y
X = data.drop(columns=['rating'])
# If release_date is a string, first convert to datetime
X['release_date'] = pd.to_datetime(X['release_date'])

# Extract year, month, day as separate features
X['release_year'] = X['release_date'].dt.year
X['release_month'] = X['release_date'].dt.month
X['release_day'] = X['release_date'].dt.day

# Drop the original release_date column
X = X.drop(columns=['release_date'])

y = data['rating']

print(y.head())

print(X.shape, y.shape)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Optional: Specify categorical features
categorical_features = ['user_id', 'movie_id', 'sex_F', 'sex_M'] + \
                       [col for col in X.columns if col.startswith('age_group_')]

train_data=lgb.Dataset(X_train, label=y_train, categorical_feature=categorical_features)
test_data=lgb.Dataset(X_test, label=y_test, categorical_feature=categorical_features)

params={
    'objective':'regression',
    'metric':'rmse',
    'verbosity':-1
}

model = lgb.train(
    params,
    train_data,
    valid_sets=[test_data],
    valid_names=['valid'],
    num_boost_round=100,
    callbacks=[lgb.early_stopping(stopping_rounds=10)]
)

# Predict
y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
print(f"Test RMSE: {rmse:.4f}")

def recommend_movies(user_id, model, movies_df, df, top_n=20):
    # Movies the user has already rated
    rated_movie_ids = df[df['user_id'] == user_id]['movie_id'].unique()

    # Movies not yet rated by user
    unrated_movies = movies_df[~movies_df['movie_id'].isin(rated_movie_ids)]

    # Create feature dataframe for prediction
    user_row = df[df['user_id'] == user_id].iloc[0]
    user_features = user_row.drop(['movie_id', 'rating', 'title', 'rating_date', 'release_date'], errors='ignore')

    candidates = []
    for _, movie in unrated_movies.iterrows():
        features = user_features.copy()
        features['movie_id'] = movie['movie_id']

        # Add genre features
        for genre in movies_df.columns[3:]:  # assuming first 3 cols are movie_id, title, release_year/month/day
            features[genre] = movie[genre]

        candidates.append(features)

    pred_df = pd.DataFrame(candidates)

    # Drop any datetime or object date columns before prediction
    datetime_cols = pred_df.select_dtypes(include=['datetime64']).columns.tolist()
    object_date_cols = [col for col in pred_df.columns if pred_df[col].dtype == 'object' and 'date' in col]
    cols_to_drop = datetime_cols + object_date_cols
    if cols_to_drop:
        print(f"Dropping columns before prediction: {cols_to_drop}")
        pred_df = pred_df.drop(columns=cols_to_drop)

    preds = model.predict(pred_df)

    pred_df['predicted_rating'] = preds
    pred_df['movie_id'] = unrated_movies['movie_id'].values

    # Merge back with titles
    recommended = pred_df.merge(movies_df[['movie_id', 'title']], on='movie_id')
    top_recs = recommended.sort_values('predicted_rating', ascending=False).head(top_n)

    return top_recs[['title', 'predicted_rating']]



movies_df=pd.read_csv('clean_data/movies_clean.csv')
movies_df['release_date'] = pd.to_datetime(movies_df['release_date'])
movies_df['release_year'] = movies_df['release_date'].dt.year
movies_df['release_month'] = movies_df['release_date'].dt.month
movies_df['release_day'] = movies_df['release_date'].dt.day
movies_df = movies_df.drop(columns=['release_date'])
df=pd.read_csv('merged_data/merged_dataset.csv')
# If release_date is a string, first convert to datetime
'''df['release_date'] = pd.to_datetime(df['release_date'])

# Extract year, month, day as separate features
df['release_year'] = df['release_date'].dt.year
df['release_month'] = df['release_date'].dt.month
df['release_day'] = df['release_date'].dt.day'''
recommendations = recommend_movies(user_id=42, model=model, movies_df=movies_df, df=df, top_n=20)
print(recommendations)
