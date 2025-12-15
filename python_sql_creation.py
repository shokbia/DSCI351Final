from sqlalchemy import create_engine, text
# import password
import pandas as pd

def connect():
    # Connect to the MySQL database
    # Enter your own credentials below:
    user = "lrli"
    connection = "tacmysql.usc.edu"
    # passw = password.password_return() #ignore
    passw = "1129814278"
    # can enter your password here
    engine = create_engine(
    f"mysql+mysqlconnector://{user}:{passw}@{connection}/{user}")
    return engine

def create_mysql_tables(engine):
    # Define your SQL statements
    drop_director = "DROP TABLE IF EXISTS Director;"
    drop_genre = "DROP TABLE IF EXISTS Genre;"
    drop_person = "DROP TABLE IF EXISTS Person;"
    drop_showCast = "DROP TABLE IF EXISTS ShowCast;"
    drop_netflixShow = "DROP TABLE IF EXISTS NetflixShow;"
    drop_directs = "DROP TABLE IF EXISTS Directs;"
    drop_hasGenre = "DROP TABLE IF EXISTS HasGenre;"
    drop_inCast = "DROP TABLE IF EXISTS InCast;"

    create_director = """
    CREATE TABLE Director (
        d_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(300) NOT NULL
    ) ;
    """

    create_genre = """
    CREATE TABLE Genre (
        g_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );
    """
    create_person = """
    CREATE TABLE Person (
        p_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );
    """
    create_showCast = """
    CREATE TABLE ShowCast (
        c_id INT AUTO_INCREMENT PRIMARY KEY,
        cast_names VARCHAR(1000) NOT NULL
    );
    """
    create_netflixShow = """
    CREATE TABLE NetflixShow (
        ns_id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        type VARCHAR(50),
        release_year SMALLINT,
        date_added DATE,
        country VARCHAR(255),
        rating FLOAT,
        description TEXT,
        language VARCHAR(50),
        popularity FLOAT,
        vote_count INT,
        vote_average FLOAT,
        budget FLOAT,
        revenue FLOAT, 
        c_id INT, 
        FOREIGN KEY (c_id) REFERENCES ShowCast(c_id)
    );
    """

    create_directs = """
    CREATE TABLE Directs (
        d_id INT,
        ns_id INT,
        PRIMARY KEY (d_id, ns_id),
        FOREIGN KEY (d_id) REFERENCES Director(d_id),
        FOREIGN KEY (ns_id) REFERENCES NetflixShow(ns_id)
    );
    """
    create_hasGenre = """
    CREATE TABLE HasGenre (
        ns_id INT,
        g_id INT,
        PRIMARY KEY (ns_id, g_id),
        FOREIGN KEY (ns_id) REFERENCES NetflixShow(ns_id),
        FOREIGN KEY (g_id) REFERENCES Genre(g_id)
    );
    """
    create_inCast = """
    CREATE TABLE InCast (
        c_id INT,
        p_id INT,
        PRIMARY KEY (c_id, p_id),
        FOREIGN KEY (c_id) REFERENCES ShowCast(c_id),
        FOREIGN KEY (p_id) REFERENCES Person(p_id)
    );
    """

    # Execute the SQL commands
    with engine.connect() as conn:
        print("Dropping existing tables...")
        conn.execute(text(drop_inCast))
        conn.execute(text(drop_hasGenre))
        conn.execute(text(drop_directs))
        conn.execute(text(drop_netflixShow))
        conn.execute(text(drop_showCast))
        conn.execute(text(drop_person))
        conn.execute(text(drop_genre))
        conn.execute(text(drop_director))
        conn.commit()

        print("Creating new tables...")
        conn.execute(text(create_director))
        conn.execute(text(create_genre))
        conn.execute(text(create_person))
        conn.execute(text(create_showCast))
        conn.execute(text(create_netflixShow))
        conn.execute(text(create_directs))
        conn.execute(text(create_hasGenre))
        conn.execute(text(create_inCast))
        conn.commit()

    print("All tables created successfully!")


def insert_sample_data(engine, csv_file):
    df = pd.read_csv(csv_file)

    with engine.connect() as conn:
        # --- Fill Director Entity Table ---
        directors = set()
        for director_list in df.head(100)['director'].dropna():
            for name in director_list.split(','):
                name = name.strip()
                directors.add(name)
        rows = [{"name": d} for d in directors]
        with conn.begin():
            conn.execute(
                text("INSERT IGNORE INTO Director (name) VALUES (:name)"),
                rows
            )

        # --- Fill Persons Entity Table ---
        persons = set()
        for cast_list in df.head(100)['cast'].dropna():
            for name in cast_list.split(','):
                name = name.strip()
                persons.add(name)
        rows = [{"name": p} for p in persons]
        with conn.begin():
            conn.execute(
                text("INSERT IGNORE INTO Person (name) VALUES (:name)"),
                rows
            )

        # --- Fill Genre Entity Table ---
        genres = set()
        for genre_list in df['genres'].dropna():
            for genre_name in genre_list.split(','):
                genre_name = genre_name.strip()
                genres.add(genre_name)

        rows = [{"name": g} for g in genres]

        with conn.begin():
            conn.execute(
                text("INSERT IGNORE INTO Genre (name) VALUES (:name)"),
                rows
            )
        # --- Fill ShowCast Entity Table---
        cast = df.head(100)['cast'].dropna().unique()
        rows = [{"cast_names": c} for c in cast]
        with conn.begin():
            conn.execute(
                text("INSERT IGNORE INTO ShowCast (cast_names) VALUES (:cast_names)"),
                rows
            )

        # --- Fill NetflixShow Entity Table---
        rows = []
        for _, row in df.head(100).iterrows():

            c_id = None
            if pd.notna(row["cast"]):
                c_id = conn.execute(
                    text("SELECT c_id FROM ShowCast WHERE cast_names = :cast"),
                    {"cast": row["cast"]}
                ).scalar()

            rows.append({
                "title": row.get("title"),
                "type": row.get("type"),
                "release_year": row.get("release_year"),
                "date_added": row.get("date_added"),
                "country": row.get("country"),
                "rating": row.get("rating"),
                "description": row.get("description"),
                "language": row.get("language"),
                "popularity": row.get("popularity"),
                "vote_count": row.get("vote_count"),
                "vote_average": row.get("vote_average"),
                "budget": row.get("budget"),
                "revenue": row.get("revenue"),
                "c_id": c_id
            })

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT IGNORE INTO NetflixShow (
                        title, type, release_year, date_added, country, rating,
                        description, language, popularity, vote_count,
                        vote_average, budget, revenue, c_id
                    )
                    VALUES (
                        :title, :type, :release_year, :date_added, :country, :rating,
                        :description, :language, :popularity, :vote_count,
                        :vote_average, :budget, :revenue, :c_id
                    )
                """),
                rows
            )

        # --- Fill HasGenre Relationship Table ---
        with engine.begin() as conn:
            for _, row in df.head(100).iterrows():
                ns_id = conn.execute(
                    text("SELECT ns_id FROM NetflixShow WHERE title = :title"),
                    {"title": row["title"]}
                ).scalar()

                if pd.isna(row["genres"]):
                    continue

                # Split the genres string into individual names
                for genre_name in row["genres"].split(","):
                    genre_name = genre_name.strip()
                    g_id = conn.execute(
                        text("SELECT g_id FROM Genre WHERE name = :name"),
                        {"name": genre_name}
                    ).scalar()

                    if g_id:
                        # Insert ns_id and g_id into HasGenre
                        conn.execute(
                            text("""
                                INSERT IGNORE INTO HasGenre (ns_id, g_id)
                                VALUES (:ns_id, :g_id)
                            """),
                            {"ns_id": ns_id, "g_id": g_id}
                        )

        # --- Fill Directs Relationship Table ---
        with engine.begin() as conn:
            for _, row in df.head(100).iterrows():
                ns_id = conn.execute(
                    text("SELECT ns_id FROM NetflixShow WHERE title = :title"),
                    {"title": row["title"]}
                ).scalar()

                if pd.isna(row["director"]):
                    continue

                # Split the director string into individual names
                for director_name in row["director"].split(","):
                    director_name = director_name.strip()
                    d_id = conn.execute(
                        text("SELECT d_id FROM Director WHERE name = :name"),
                        {"name": director_name}
                    ).scalar()

                    if d_id:
                        # Insert d_id and g_id into Directs
                        conn.execute(
                            text("""
                                INSERT IGNORE INTO Directs (ns_id, d_id)
                                VALUES (:ns_id, :d_id)
                            """),
                            {"ns_id": ns_id, "d_id": d_id}
                        )

        # --- Fill InCast Relationship Table ---
        with engine.begin() as conn:
            for _, row in df.head(100).iterrows():
                c_id = conn.execute(
                    text("SELECT c_id FROM NetflixShow WHERE title = :title"),
                    {"title": row["title"]}
                ).scalar()

                if pd.isna(row["cast"]):
                    continue

                # Split the cast string into individual names
                for cast_name in row["cast"].split(","):
                    cast_name = cast_name.strip()
                    p_id = conn.execute(
                        text("SELECT p_id FROM Person WHERE name = :name"),
                        {"name": cast_name}
                    ).scalar()

                    if p_id:
                        # Insert p_id and c_id into InCast
                        conn.execute(
                            text("""
                                INSERT IGNORE INTO InCast (c_id, p_id)
                                VALUES (:c_id, :p_id)
                            """),
                            {"c_id": c_id, "p_id": p_id}
                        )


def main():
    engine = connect()
    print("After engine function")
    create_mysql_tables(engine)
    print("After table creation")
    insert_sample_data(engine, "cleaned_netflix_movies (1).csv")
    # mysql_query_examples(engine)

if __name__ == "__main__":
    main()


