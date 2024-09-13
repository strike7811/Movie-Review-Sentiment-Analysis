from neo4j import GraphDatabase

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
            response = session.run(query, parameters)
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


# Initialize connection
conn = Neo4jConnection(uri="neo4j+s://46dbca23.databases.neo4j.io", user="neo4j", pwd="gd0T2M1NgHS-qgGPm3CVaBWDWKvFj7H6qr2nYCeQm0c")

movies_query = """
UNWIND $movies as movie
MERGE (m:Movie {imdb_id: movie.imdb_id})
ON CREATE SET m.tmdb_id = movie.tmdb_id, m.title = movie.title, m.year = movie.year,
               m.duration = movie.duration, m.rating = movie.rating, m.votes = movie.votes,
               m.overview = movie.overview, m.genre_list = movie.genre_list
ON MATCH SET m.tmdb_id = coalesce(movie.tmdb_id, m.tmdb_id)
"""

reviews_query = """
UNWIND $movies as movie
MERGE (m:Movie {title: movie.title, year: movie.year})
ON CREATE SET
    m.imdb_id = movie.imdb_id,
    m.tmdb_id = movie.tmdb_id,
    m.duration = movie.duration,
    m.rating = movie.rating,
    m.votes = movie.votes,
    m.overview = movie.overview,
    m.genre_list = movie.genre_list
ON MATCH SET
    m.imdb_id = COALESCE(movie.imdb_id, m.imdb_id),
    m.tmdb_id = COALESCE(movie.tmdb_id, m.tmdb_id),
    m.duration = movie.duration,
    m.rating = movie.rating,
    m.votes = movie.votes,
    m.overview = movie.overview,
    m.genre_list = movie.genre_list
"""

review_movie_query = """
UNWIND $reviews as review
MATCH (r:Review {review_ID: review.review_ID})
OPTIONAL MATCH (m:Movie {imdb_id: review.imdb_id})
OPTIONAL MATCH (m2:Movie {tmdb_id: review.tmdb_id})
WITH r, COALESCE(m, m2) AS movie
WHERE movie IS NOT NULL
MERGE (r)-[:REVIEW_OF]->(movie)
RETURN r.review_ID, movie.imdb_id AS MovieIMDBID, movie.tmdb_id AS MovieTMDBID
"""

author_review_query= """
UNWIND $reviews as review
MERGE (u:User {author: review.author})
ON CREATE SET u.created = date()  // Example of setting additional attributes
MERGE (r:Review {review_ID: review.review_ID})
ON CREATE SET r = review  // Ensures that the review node is created with all provided properties
MERGE (u)-[:WROTE]->(r);
"""


# # Create constraints with updated syntax
# # conn.query("CREATE CONSTRAINT FOR (r:Review) REQUIRE r.review_ID IS UNIQUE;")
#
# try:
#     movies = []
#     reviews = []
#     conn.query(movies_query, parameters={"movies": movies})
#     conn.query(reviews_query, parameters={"reviews": reviews})
#     conn.query(review_movie_query, parameters={"reviews": reviews})
#     conn.query(author_review_query, parameters={"reviews": reviews})
# except Exception as e:
#     print("Error processing data-related queries:", e)
#
# # Close the connection
# conn.close()
