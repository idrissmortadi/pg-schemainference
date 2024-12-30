import joblib
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase, exceptions
from itertools import combinations
import numpy as np
from tqdm import tqdm


def get_keys_from_database(uri, user, password):
    """
    Returns a list of unique keys from the Neo4j database.
    :param uri: Neo4j URI (e.g., bolt://localhost:7687)
    :param user: Username for Neo4j authentication
    :param password: Password for Neo4j authentication
    :return: List of unique keys from the database
    """
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        keys = set()
        with driver.session() as session:
            result = session.run("MATCH (n) UNWIND keys(n) AS key RETURN DISTINCT key")
            keys = {record["key"] for record in result}
        return list(keys)
    except exceptions.ServiceUnavailable as e:
        print("Could not connect to Neo4j:", e)
        return []
    except Exception as e:
        print("An error occurred:", e)
        return []
    finally:
        driver.close()


if __name__ == "__main__":
    # Neo4j connection details
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"

    # Get keys from the database
    keys = get_keys_from_database(uri, user, password)
    if not keys:
        print("No keys found in the database.")
        exit()

    # Load sentence transformer model
    print("Loading model...")
    model = SentenceTransformer("multi-qa-mpnet-base-dot-v1")

    # Calculate embeddings
    print("Calculating embeddings...")
    keys_embedding = model.encode(keys, show_progress_bar=True)

    # Compute dot products for all pairs
    print("Calculating similarities...")
    similarity_dict = {}
    for key1, key2 in tqdm(combinations(range(len(keys)), 2)):
        key_pair = (keys[key1], keys[key2])
        dot_product = np.dot(keys_embedding[key1], keys_embedding[key2])
        similarity_dict[key_pair] = dot_product

    # Save similarity dictionary to a file
    similarity_file = "key_similarity.pkl"
    joblib.dump(similarity_dict, similarity_file)
    print(f"Similarity dictionary saved to {similarity_file}")
