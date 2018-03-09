CREATE VIEW IF NOT EXTSTS avgRatings AS
SELECT movieID, AVG(movieID) as avgRating, COUNT(movieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY rating Count DESC;

SELECT n.title,avgRating
FROM avgRatings t JOIN names n ON t.movieID=n.movieID
WHERE ratingCount>10;
