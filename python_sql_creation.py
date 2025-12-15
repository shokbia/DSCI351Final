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
        ns_id INT NOT NULL,
        cast_collected VARCHAR(1000),
        FOREIGN KEY (ns_id) REFERENCES NetflixShow(ns_id)
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
        revenue FLOAT
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
        conn.execute(text(drop_showCast))
        conn.execute(text(drop_netflixShow))
        conn.execute(text(drop_person))
        conn.execute(text(drop_genre))
        conn.execute(text(drop_director))
        conn.commit()

        print("Creating new tables...")
        conn.execute(text(create_director))
        conn.execute(text(create_genre))
        conn.execute(text(create_person))
        conn.execute(text(create_netflixShow))
        conn.execute(text(create_showCast))
        conn.execute(text(create_directs))
        conn.execute(text(create_hasGenre))
        conn.execute(text(create_inCast))
        conn.commit()

    print("All tables created successfully!")


def insert_sample_data(engine, csv_file):
    df = pd.read_csv(csv_file)

    with engine.connect() as conn:
        # --- Insert Directors ---
        directors = df['director_netflix'].dropna().unique()
        for d in directors:
            conn.execute(text(f"INSERT IGNORE INTO Director (name) VALUES (:name)"), {"name": d})

        # --- Insert Persons (actors) ---
        persons = set()
        for cast_list in df['cast_netflix'].dropna():
            for name in cast_list.split(','):
                persons.add(name.strip())
        for p in persons:
            conn.execute(text(f"INSERT IGNORE INTO Person (name) VALUES (:name)"), {"name": p})

        # --- Insert NetflixShow ---
        for idx, row in df.iterrows():
            conn.execute(text(
                "INSERT IGNORE INTO NetflixShow (name, audience_rating, year_firstreleased) VALUES (:name, :rating, :year)"
            ), {
                "name": row['title'],
                "rating": row.get('rating_netflix', None),
                "year": row.get('release_year', None)
            })

        # --- Insert ShowCast ---
        for idx, row in df.iterrows():
            # Get show_id
            show_id_res = conn.execute(text("SELECT ns_id FROM NetflixShow WHERE name = :name"), {"name": row['title']})
            show_id = show_id_res.fetchone()[0]
            conn.execute(text(
                "INSERT INTO ShowCast (show_id, source) VALUES (:show_id, :source)"
            ), {"show_id": show_id, "source": "Netflix"})

        conn.commit()

def main():
    engine = connect()
    print("After engine function")
    create_mysql_tables(engine)
    print("After table creation")
    insert_sample_data(engine, "data_files/cleaned_netflix_movies.csv")
    # mysql_query_examples(engine)

if __name__ == "__main__":
    main()


