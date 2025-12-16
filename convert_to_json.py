# This file is to convert the csv files into json to use for MongoDB
import pandas as pd

# Convert netflix
df = pd.read_csv('project_data/_netflix_titles.csv').fillna('')
df.to_json('netflix.json', orient='records', indent=2)

# Convert movies
df = pd.read_csv('project_data/movies.csv').fillna('')
df.to_json('movies.json', orient='records', indent=2)

print("✓ Created netflix.json")
print("✓ Created movies.json")