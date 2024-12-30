import time

from neo4j import GraphDatabase
from termcolor import colored

from preprocessing_step import preprocessing
from sampling import sampling


def test():
    DBname = "DBP-15k"
    uri = "bolt://localhost:7687"
    user = "neo4j"
    passwd = "password"

    print("Connecting to Neo4j Database...")
    driver = GraphDatabase.driver(
        uri, auth=(user, passwd), encrypted=False
    )  # set encrypted to False to avoid possible errors

    print(
        colored("Starting to query on ", "red"),
        colored(DBname, "red"),
        colored(":", "red"),
    )
    t1 = time.perf_counter()
    amount_dict, list_of_distinct_nodes, distinct_labels, labs_sets = preprocessing(
        driver
    )
    t1f = time.perf_counter()

    step1 = t1f - t1  # time to complete step 1
    print(colored("Queries are done.", "green"))
    print("Step 1: Preprocessing was completed in ", step1, "s")

    print("---------------")

    print("amount_dict", len(amount_dict))
    print("list_of_distinct_nodes", len(list_of_distinct_nodes))
    print("distinct_labels", len(distinct_labels))
    print("labs_sets", len(labs_sets))

    print("Node example: ", list_of_distinct_nodes[0])
    print("amount_dict: ", amount_dict)

    print(colored("Data sampling : ", "blue"))
    ts = time.perf_counter()
    amount_dict, list_of_distinct_nodes, validate, test = sampling(
        amount_dict, list_of_distinct_nodes, 80
    )
    tsf = time.perf_counter()
    steps = tsf - ts  # time to complete the sampling step
    print(colored("Separating done.", "green"))
    print("The sampling step was processed in ", steps, "s")

    print("---------------")


if __name__ == "__main__":
    test()
