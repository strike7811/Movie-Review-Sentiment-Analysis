import socket
import json
import re
import contractions
import nltk
from nltk import pos_tag, ngrams
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer, PorterStemmer, SnowballStemmer
from textblob import TextBlob
from nltk.corpus import stopwords
from neo4jsetup import Neo4jConnection

nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

# Setup stopwords
stop_words = set(stopwords.words('english'))


def start_server(host='localhost', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, port))
        sock.listen(1)
        print(f"Server listening on {host}:{port}")

        neo4j_conn = Neo4jConnection(uri="neo4j+s://46dbca23.databases.neo4j.io", user="neo4j",
                                     pwd="gd0T2M1NgHS-qgGPm3CVaBWDWKvFj7H6qr2nYCeQm0c")

        conn, addr = sock.accept()
        with conn:
            print(f"Connected by {addr}")
            try:
                data_buffer = ""
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    data_buffer += data.decode('utf-8')

                    while '\n' in data_buffer:
                        message, data_buffer = data_buffer.split('\n', 1)  # Split on the first newline
                        if message and message is not None:
                            processed_data = preprocess_data(message)
                            if processed_data is not None:
                                processed_data.pop('pos_tags', None)
                                if processed_data:
                                    # Insert review data into Neo4j
                                    insert_query = """
                                                    MERGE (review:Review {review_ID: $review_ID})
                                                    ON CREATE SET
                                                        review.imdb_id = $imdb_id,
                                                        review.tmdb_id = $tmdb_id,
                                                        review.author = $author,
                                                        review.content = $content,
                                                        review.rating = $rating,
                                                        review.date = $date,
                                                        review.sentiment = $sentiment,
                                                        review.tokens = $tokens,
                                                        review.bigrams = $bigrams,
                                                        review.trigrams = $trigrams,
                                                        review.filtered_tokens = $filtered_tokens,
                                                        review.wn_lemmatized_tokens = $wn_lemmatized_tokens,
                                                        review.tb_lemmatized_tokens = $tb_lemmatized_tokens,
                                                        review.porter_stemmed_tokens = $porter_stemmed_tokens,
                                                        review.snowball_stemmed_tokens = $snowball_stemmed_tokens
                                                    """
                                    print("Processed Data:", processed_data)
                                    neo4j_conn.query(insert_query, parameters=processed_data)
            except json.JSONDecodeError as e:
                print("JSON decode error:", e)
            except Exception as e:
                print("Error:", e)
            finally:
                print("Closing connection")
                conn.close()


def preprocess_data(json_data):
    data = json.loads(json_data)

    # 1. Check for null rating and filter out if necessary
    if data['rating'] is None:
        return None

    # 2. Assign sentiment based on the rating
    rating = data['rating']
    if rating <= 2:
        sentiment = "Negative"
    elif rating <= 4:
        sentiment = "Somewhat Negative"
    elif rating == 5:
        sentiment = "Neutral"
    elif rating <= 7:
        sentiment = "Somewhat Positive"
    else:
        sentiment = "Positive"
    data['sentiment'] = sentiment

    # 3. Text processing: Expand contractions, remove HTML, URLs, and lowercase
    content = data['content']
    content = contractions.fix(content)
    content = re.sub(r"<.*?>", "", content)  # Remove HTML tags
    content = re.sub(
        r"https?://(?:www\.)?[a-zA-Z0-9\-_]+(?:\.[a-zA-Z]{2,})+[:0-9]*[/?][^\s]*|www\.[a-zA-Z0-9\-_]+(?:\.[a-zA-Z]{2,"
        r"})+[:0-9]*[/?][^\s]*",
        "", content)  # Remove URLs
    content = content.lower()  # Convert to lowercase
    content = re.sub(r"[^\w\s]", " ", content)  # Remove special characters
    content = re.sub(r"\s+", " ", content)  # Replace multiple spaces with a single space
    content = re.sub(r"\d+", "", content)  # Remove numbers
    content = content.strip()
    data['content'] = content

    # 4. Tokenization
    tokens = word_tokenize(content)
    data['tokens'] = tokens

    # 5. Create Bigrams and Trigrams
    bigrams = [" ".join(bg) for bg in ngrams(tokens, 2)]
    trigrams = [" ".join(tg) for tg in ngrams(tokens, 3)]
    data['bigrams'] = bigrams
    data['trigrams'] = trigrams

    # 6. Pos Tagging
    data['pos_tags'] = pos_tag(tokens)
    data['pos'] = [tup[1] for tup in data['pos_tags']]

    # 7. Stopword Removal
    filtered_tokens = [token for token in tokens if token not in stop_words]
    data['filtered_tokens'] = filtered_tokens

    # 8. Lemmatization and Stemming
    wn_lemmatizer = WordNetLemmatizer()
    pt_stemmer = PorterStemmer()

    tb_lemmatizer = TextBlob(content)
    sb_stemmer = SnowballStemmer('english')

    data['wn_lemmatized_tokens'] = [wn_lemmatizer.lemmatize(token) for token in filtered_tokens]
    data['porter_stemmed_tokens'] = [pt_stemmer.stem(token) for token in filtered_tokens]
    data['snowball_stemmed_tokens'] = [sb_stemmer.stem(token) for token in filtered_tokens]
    data['tb_lemmatized_tokens'] = [word.lemmatize() for word in tb_lemmatizer.words]

    return data


if __name__ == "__main__":
    start_server()
