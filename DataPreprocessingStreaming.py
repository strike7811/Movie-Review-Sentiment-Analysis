from pyspark.sql import SparkSession
from neo4j import GraphDatabase

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkNeo4jStreaming").getOrCreate()

# Neo4j connection details
URI = "neo4j+s://46dbca23.databases.neo4j.io"  # Corrected URI format
AUTH = ("neo4j", "gd0T2M1NgHS-qgGPm3CVaBWDWKvFj7H6qr2nYCeQm0c")  # Corrected auth format


class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


conn = Neo4jConnection(uri=URI, user=AUTH[0], pwd=AUTH[1])
conn.query('')

movies_query = """
UNWIND $movies as movie
MERGE (m:Movie {imdb_id: movie.imdb_id})
ON CREATE SET m.tmdb_id = movie.tmdb_id, m.title = movie.title, m.year = movie.year,
               m.duration = movie.duration, m.rating = movie.rating, m.votes = movie.votes,
               m.overview = movie.overview, m.genre_list = movie.genre_list
ON MATCH SET m.tmdb_id = coalesce(movie.tmdb_id, m.tmdb_id)
"""

reviews_query = """
UNWIND $reviews as review
MERGE (r:Review {review_ID: review.review_ID})
ON CREATE SET r.imdb_id = review.imdb_id, 
               r.tmdb_id = review.tmdb_id, 
               r.author = review.author,
               r.content = review.content, 
               r.rating = review.rating, 
               r.comment_date = review.comment_date,
               r.content_length = review.content_length, 
               r.sentiment = review.sentiment,
               r.expanded_content = review.expanded_content, 
               r.text_no_urls = review.text_no_urls,
               r.lowercase = review.lowercase, 
               r.no_punc = review.no_punc,
               r.no_num_content = review.no_num_content, 
               r.cleaned_content = review.cleaned_content,
               r.tokens = review.tokens, 
               r.bigrams = review.bigrams, 
               r.trigrams = review.trigrams,
               r.filtered_tokens = review.filtered_tokens,
               r.wn_lemmatized_tokens = review.wn_lemmatized_tokens, 
               r.tb_lemmatized_tokens = review.tb_lemmatized_tokens,
               r.porter_stemmed_tokens = review.porter_stemmed_tokens, 
               r.snowball_stemmed_tokens = review.snowball_stemmed_tokens,
               r.pos_ = review.pos
"""

review_movie_query = """
UNWIND $reviews as review
MATCH (r:Review {review_ID: review.review_ID})
OPTIONAL MATCH (m:Movie {imdb_id: review.imdb_id})
OPTIONAL MATCH (m2:Movie {tmdb_id: review.tmdb_id})
WITH r, coalesce(m, m2) as movie
WHERE movie IS NOT NULL
MERGE (r)-[:REVIEW_OF]->(movie)
"""

movie_genre_query = """
UNWIND $movies as movie
MERGE (m:Movie {imdb_id: movie.imdb_id})
ON CREATE SET m = movie
WITH m, movie.genre_list as genres
UNWIND genres as genre
MERGE (g:Genre {name: genre})
MERGE (m)-[:HAS_GENRE]->(g)
"""

author_review_query = """
UNWIND $reviews as review
MERGE (u:User {author: review.author})
MERGE (r:Review {review_ID: review.review_ID})
ON CREATE SET r = review
MERGE (u)-[:WROTE]->(r);
"""


# Execute queries
conn.query(movies_query, parameters={"movies": movies})
conn.query(reviews_query, parameters={"reviews": reviews})
conn.query(review_movie_query, parameters={"reviews": reviews})
conn.query(movie_genre_query, parameters={"movies": movies})
conn.query(author_review_query, parameters={"reviews": reviews})

# Close the connection when done
conn.close()
