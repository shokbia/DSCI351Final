# Final Project Part 1.1
use ctjangna;
SHOW TABLES;

SELECT * FROM movie;
SELECT * FROM netflix;

# 1
SELECT title, release_year
FROM netflix
ORDER BY release_year DESC
LIMIT 5;

# 2
SELECT COUNT(*) as numMovies, rating
FROM netflix
GROUP BY rating;

# 3
SELECT ROUND(AVG(release_year),0) AS avgReleaseYear, rating
FROM netflix
GROUP BY rating;

# 4 
SELECT n.title, m. year
FROM netflix n INNER JOIN movie m ON m.movie_id_np = n.movie_id_np
WHERE n.release_year = m.year;

# 5
SELECT rating, COUNT(*) as numMovies
FROM netflix
GROUP BY rating
HAVING numMovies > 3;

# 6 
SELECT UPPER(title)
FROM netflix
LIMIT 10;

# 7 
SELECT director, COUNT(*) as numMovies
FROM netflix
GROUP BY director
HAVING numMovies > 1;

# 8 
SELECT title
FROM netflix
WHERE LOWER(description) LIKE '%father%';

# 9 
SELECT rating
FROM netflix
GROUP BY rating
HAVING max(release_year) <= 2000;

# 10 
SELECT n.title, n.release_year, m.year
FROM netflix n INNER JOIN movie m ON m.movie_id_np = n.movie_id_np
WHERE n.director LIKE "%Steven Spielberg%";

# 11
SELECT n.title, n.rating, m.year
FROM netflix n INNER JOIN movie m ON m.movie_id_np = n.movie_id_np
WHERE n.rating IN ('PG','PG-13');

# 12
SELECT n.title
FROM netflix n INNER JOIN movie m ON m.movie_id_np = n.movie_id_np
WHERE m.year > 1995 AND n.release_year > 1995


