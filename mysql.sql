select movies.title,count(ratings.movie_id) as ratingCount
from movies
inner join ratings
on movies.id=ratings.movie_id
group by movies.title
order by ratingCount;