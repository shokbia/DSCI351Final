# streamlit_netflix.py
# Lara Li, Bia Shok, Chloe Tjangnaka, Jasmine Luong
# Professor Seymen
# DSCI 351: Foundations of Data Management
# 15 December, 2025

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# read the datasets
netflix = pd.read_csv("_netflix_titles.csv")
movies = pd.read_csv("movies.csv")

# title streamlit project
st.title("DSCI351 Streamlit Final Project")

# radio buttons for user to pick which visualization
choice = st.radio(
    "Select a visualization:",
    [
        "Movies vs TV Shows Bar Chart",
        "Distribution of Movie Ratings Histogram",
        "Popularity vs Rating Scatterplot",
        "Top 10 Genres Bar Chart",
        "Movie Ratings Distribution Pie Chart"
    ]
)

# 1. Movies vs TV Shows Bar Chart
if choice == "Movies vs TV Shows Bar Chart":
    counts = netflix["type"].value_counts()

    plt.figure()
    plt.bar(counts.index, counts.values)
    plt.xlabel("Type")
    plt.ylabel("Count")
    plt.title("Movies vs TV Shows on Netflix")

    st.pyplot(plt)

# 2. Histogram of vote_average
elif choice == "Distribution of Movie Ratings Histogram":
    plt.figure()
    plt.hist(movies["vote_average"].dropna())
    plt.xlabel("Vote Average")
    plt.ylabel("Frequency")
    plt.title("Distribution of Movie Ratings")

    st.pyplot(plt)

# 3. Scatter plot of popularity vs rating
elif choice == "Popularity vs Rating Scatterplot":
    plt.figure()
    plt.scatter(movies["popularity"], movies["vote_average"])
    plt.xlabel("Popularity")
    plt.ylabel("Vote Average")
    plt.title("Popularity vs Rating")

    st.pyplot(plt)

# 4. Bar chart: Top 10 genres
elif choice == "Top 10 Genres Bar Chart":
    genre_series = movies["genres"].dropna().str.split(", ")
    genres = genre_series.explode()
    top_genres = genres.value_counts().head(10)

    plt.figure()
    plt.bar(top_genres.index, top_genres.values)
    plt.xlabel("Genre")
    plt.ylabel("Count")
    plt.title("Top 10 Most Common Genres")
    plt.xticks(rotation=45)

    st.pyplot(plt)

# 5. Pie chart: Movie ratings (top 5 + Other)
elif choice == "Movie Ratings Distribution Pie Chart":
    movies_only = netflix[netflix["type"] == "Movie"]
    rating_counts = movies_only["rating"].value_counts()

    top5 = rating_counts.head(5)
    other_count = rating_counts.iloc[5:].sum()
    top5["Other"] = other_count

    plt.figure()
    plt.pie(top5.values, labels=top5.index, autopct="%1.1f%%")
    plt.title("Movie Rating Distribution")

    st.pyplot(plt)