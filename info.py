import pandas as pd
import os

def meta():
    users_cols = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
    
    try:
        users = pd.read_csv(
            r'data\ml-100k\u.user',
            sep='|',
            names=users_cols,
            encoding='latin-1'
        )
    except FileNotFoundError:
        print("❌ File not found. Make sure the path is correct and data is extracted.")
        return
    except Exception as e:
        print(f"❌ Error reading the file: {e}")
        return

    print("\n📋 First 5 Rows:")
    print(users.head())

    print("\n🧾 Info:")
    print(users.info())

    print("\n📊 Statistical Summary (Numerical Columns):")
    print(users.describe())

    genere_col=['genere_col', 'id']
    genere=pd.read_csv(r'data\ml-100k\u.genre', sep='|', names=genere_col, encoding='latin-1')

    generes=genere['genere_col'].unique()

    print(f"\n🎬 Unique Genres:{generes}")

    items_cols = ['movie_id', 'title', 'release_date', "video_release_date", "imdb_url"] + list(generes)

    #items_cols = ['movie_id', 'title', 'release_date','genere', "video_release_date", "imdb_url"]
    items_raw = pd.read_csv(r'data\ml-100k\u.item', sep='|', names=items_cols, encoding='latin-1')

    print(items_raw.columns)

    print("\n📋 First 5 Rows:")
    print(items_raw.head())

    print("\n🧾 Info:")
    print(items_raw.info())

    print("\n📊 Statistical Summary (Numerical Columns):")
    print(items_raw.describe())

    ratings_cols = ['user_id', 'movie_id', 'rating', 'unix_timestamp']
    ratings=pd.read_csv(r'data\ml-100k\u.data', sep='\t', names=ratings_cols, encoding='latin-1')

    print(ratings.columns)

    print("\n📋 First 5 Rows:")
    print(ratings.head())

    print("\n🧾 Info:")
    print(ratings.info())

    print("\n📊 Statistical Summary (Numerical Columns):")
    print(ratings.describe())
    
    os.makedirs('processed_data', exist_ok=True)

    users.to_csv('processed_data/users.csv', index=False)
    items_raw.to_csv('processed_data/items.csv', index=False)
    ratings.to_csv('processed_data/ratings.csv', index=False)
    print("\n✅ Processed data saved to 'processed_data' directory.")


if __name__ == "__main__":
    meta()
