CREATE VIEW IF NOT EXTSTS topMovieIDs AS
SELECT movieID, count(movieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY rating Count DESC;

SELECT n.title,ratingCount
FROM topMovieIDs t JOIN names n ON t.movieID=n.movieID;
