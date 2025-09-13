import pandas as pd

df=pd.read_csv('merged_data/merged_dataset.csv')
movies_df=pd.read_csv('clean_data/movies_clean.csv')

# Pivot table: users as rows, movies as columns, values are ratings
user_item_matrix = df.pivot_table(index='user_id', columns='movie_id', values='rating')

print(user_item_matrix.head())

import numpy as np

def cosine_similarity(u1, u2):
    mask = ~np.isnan(u1) & ~np.isnan(u2)
    if np.sum(mask) == 0:
        return 0.0
    u1 = u1[mask]
    u2 = u2[mask]
    if np.linalg.norm(u1) == 0 or np.linalg.norm(u2) == 0:
        return 0.0
    return np.dot(u1, u2) / (np.linalg.norm(u1) * np.linalg.norm(u2))


def predict_rating_user_cf(user_id, movie_id, user_item_matrix, k=5):
    if movie_id not in user_item_matrix.columns:
        return np.nan

    target_user_vector = user_item_matrix.loc[user_id].values
    similarities = []
    ratings = []

    for other_user in user_item_matrix.index:
        if other_user == user_id:
            continue
        other_user_vector = user_item_matrix.loc[other_user].values
        sim = cosine_similarity(target_user_vector, other_user_vector)
        other_rating = user_item_matrix.loc[other_user, movie_id]

        if not np.isnan(other_rating):
            similarities.append(sim)
            ratings.append(other_rating)

    if len(similarities) == 0:
        return np.nan

    # Convert to arrays
    similarities = np.array(similarities)
    ratings = np.array(ratings)

    # Take top-k similar users
    if len(similarities) > k:
        top_k_idx = np.argsort(similarities)[-k:]
        similarities = similarities[top_k_idx]
        ratings = ratings[top_k_idx]

    if np.sum(np.abs(similarities)) == 0:
        return np.mean(ratings)

    return np.dot(similarities, ratings) / np.sum(np.abs(similarities))

movie_id_to_name = dict(zip(movies_df['movie_id'], movies_df['title']))

def recommend_movies_user_cf(user_id, user_item_matrix, movie_id_to_name, k=5, n=10):
    predictions = {}

    for movie_id in user_item_matrix.columns:
        # Skip movies already rated
        if not np.isnan(user_item_matrix.loc[user_id, movie_id]):
            continue

        # Predict rating
        predicted_rating = predict_rating_user_cf(user_id, movie_id, user_item_matrix, k)

        if not np.isnan(predicted_rating):
            predictions[movie_id] = predicted_rating

    # Sort predictions by rating
    top_n = sorted(predictions.items(), key=lambda x: x[1], reverse=True)[:n]

    # Convert movie IDs to titles using the mapping
    recommendations = [(movie_id_to_name.get(mid, f"Movie {mid}"), rating) for mid, rating in top_n]

    return recommendations

# Assuming you have a user-item matrix like this:
# user_item_matrix = pd.DataFrame(...)

recommendations = recommend_movies_user_cf(
    user_id=42,
    user_item_matrix=user_item_matrix,
    movie_id_to_name=movie_id_to_name,
    k=5,
    n=10
)

for movie, score in recommendations:
    print(f"Recommend: {movie} (Predicted rating: {score:.2f})")
