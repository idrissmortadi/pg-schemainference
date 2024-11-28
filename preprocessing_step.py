""" Step 1 : Preprocessing data """

##### Imports
from termcolor import colored

def preprocessing(driver):
    """  Queries a property graph using the driver to get all needed labels',properties' and nodes' information

    Parameters
    ----------
    driver : GraphDatabase.driver object
        Driver used to access the PG stored in a Neo4j database.

    Returns
    -------
    amount_dict : Python dict
        A dictionary with node strings as keys and the number of occurrences of the node as a value
        Its format is : {'Label1 Label2 Label3 prop1 prop2 prop3 ...': int, ...}
    list_of_distinct_nodes : Python list
        A list of node strings
        Its format is : ['Label1 Label2 prop1', 'Label1 Label3 prop2', 'prop4 prop5', ...]
    distinct_labels : Python list
        A list of labels
        Its format is : ['Label1', 'Label2', 'Label3', ...]
    labs_sets : Python list of list
        A list of all labels sets
        Its format is : [['Label 1','Label2'],['Label1'],['Label3'],...]
    """
    
    print(colored("Querying neo4j to get all distinct labels:", "yellow"))
    with driver.session() as session:
        all_labels = session.run(
            "MATCH(n) WITH LABELS(n) AS labs \
            UNWIND labs AS lab \
            RETURN DISTINCT lab"
            )

        distinct_labels = []
        for labs in all_labels:
            distinct_labels.append(labs["lab"])
        print(colored("Done.", "green"))
        print(colored("Querying neo4j to get all distinct sets of labels:", "yellow"))
        labels_sets = session.run(
            "MATCH(n) \
            RETURN DISTINCT LABELS(n)"
            )

        labs_sets = []
        for labels_set in labels_sets:
            labs_sets.append(labels_set["LABELS(n)"])
        print(colored("Done.", "green"))

        print(colored("Querying neo4j to get all distinct sets of labels and props:", "yellow"))
        #get all nodes' labels and properties' names
        distinct_nodes = session.run(
            "MATCH(n) \
            RETURN ID(n), labels(n), keys(n)\
            LIMIT 1000"
            )

        # Storing the number of repetitions of the node
        amount_dict = {}

        # transform neo4j dict to python list of string
        list_of_distinct_nodes=[]

        for node in distinct_nodes:
            #get a list of labels
            labels = node["labels(n)"]

            #get a list of properties
            properties = node["keys(n)"]

            node_id = node["ID(n)"]

            labels_properties = [str(node_id)] + labels + properties
            labels_properties_str = ' '.join(labels_properties)

            if labels_properties_str in list_of_distinct_nodes:
                amount_dict[labels_properties_str] += 1
            else:
                list_of_distinct_nodes.append(labels_properties_str) 
                amount_dict[labels_properties_str] = 1
    print(colored("Done.", "green"))

    return amount_dict,list_of_distinct_nodes,distinct_labels,labs_sets