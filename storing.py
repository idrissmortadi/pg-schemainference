"""Write clusters into a file"""

##### Imports
import csv


def storing(distinct_labels, labs_sets, hierarchy_tree):
    """Write clusters into a file

    Parameters
    ----------
    distinct_labels : Python list
        A list of labels
        Its format is : ['Label1', 'Label2', 'Label3', ...]
    labs_sets : Python list of list
        A list of all labels sets
        Its format is : [['Label 1','Label2'],['Label1'],['Label3'],...]
    hierarchy_tree : Python list of sets
        Each set of this list represents a different cluster,
        they may contain one element or more,
        an element is a string node that was clustered in this cluster
        Its format is : [{'Label1 prop1', 'Label1', 'Label1 prop1 prop2'}, {'Label3', 'Label3 prop1 prop4'}, ...]

    Returns
    -------
    file : String
        Name of the file clusters were written into.

    """

    header = [
        "id",
        "labels",
        "properties",
        "subtypeof",
        "type",
        "is_basetype",
    ]

    # Associate each neo4j node id to a cluster id
    neo4j_node_cluster_mapping = {"neo4j_id": [], "cluster_id": []}

    run_clusters = []

    cluster_id = 1

    with open("data.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        # iterate through each basic type clusters
        for basic_type in hierarchy_tree:
            parent_id = cluster_id

            # line id
            data_line = [str(parent_id)]

            labels = list(basic_type[0])

            # labels
            data_line.append(":".join(labels))

            cluster_id += 1

            lcluster = basic_type[1]
            rcluster = basic_type[2]

            k = 2
            properties = ""

            if lcluster is not None and rcluster is not None:
                lset = set(list(lcluster[0])[0].split())
                rset = set(list(rcluster[0])[0].split())
                inter = lset.intersection(rset)
                inter_list_props = []
                for elt in inter:
                    if elt not in distinct_labels:
                        inter_list_props.append(elt)

                properties = ":".join(sorted(inter_list_props))

            if properties != "":
                # intersection of properties
                data_line.append(properties)

                # no supertypes for base types
                data_line.append("")

                # the name of the infered type
                data_line.append("T1")

                # is a base type
                data_line.append("yes")

                writer.writerow(data_line)

            # search for subtypes
            if lcluster is not None:
                cluster_id, k = rec_storing(
                    distinct_labels,
                    labs_sets,
                    writer,
                    lcluster,
                    cluster_id,
                    parent_id,
                    run_clusters,
                    k,
                    neo4j_node_cluster_mapping,
                )
            if rcluster is not None:
                cluster_id, k = rec_storing(
                    distinct_labels,
                    labs_sets,
                    writer,
                    rcluster,
                    cluster_id,
                    parent_id,
                    run_clusters,
                    k,
                    neo4j_node_cluster_mapping,
                )

    with open('neo4j_id_cluster_mapping.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write headers
        writer.writerow(neo4j_node_cluster_mapping.keys())
        # Write rows
        writer.writerows(zip(*neo4j_node_cluster_mapping.values()))
    return "data.csv"


def rec_storing(
    distinct_labels,
    labs_sets,
    writer,
    hierarchy_tree,
    cluster_id,
    parent_id,
    run_clusters,
    k,
    neo4j_node_cluster_mapping,
):
    """Write clusters into a file

    Parameters
    ----------
    distinct_labels : Python list
        A list of labels
        Its format is : ['Label1', 'Label2', 'Label3', ...]
    labs_sets : Python list of list
        A list of all labels sets
        Its format is : [['Label 1','Label2'],['Label1'],['Label3'],...]
    all_clusters : Python list of sets
        Each set of this list represents a different cluster,
        they may contain one element or more,
        an element is a string node that was clustered in this cluster
        Its format is : [{'Label1 prop1', 'Label1', 'Label1 prop1 prop2'}, {'Label3', 'Label3 prop1 prop4'}, ...]

    Returns
    -------
    file : String
        Name of the file clusters were written into.

    """
    all_labels = set()
    all_properties = set()
    always_labels = set()
    always_properties = set()

    j = 0

    # iterate through each node that forms the cluster
    for node in hierarchy_tree[0]:
        # Map node neo4j_id to cluster id
        neo4j_node_cluster_mapping["neo4j_id"].append(node.split(" ")[0])
        neo4j_node_cluster_mapping["cluster_id"].append(cluster_id)

        props_labs = node.split(" ")[1:]

        cur_labels = set()
        cur_properties = set()

        # search for labels and properties in this subtype
        if j == 0:
            for prop_lab in props_labs:
                if prop_lab in distinct_labels:
                    all_labels.add(prop_lab)
                    always_labels.add(prop_lab)
                    cur_labels.add(prop_lab)
                else:
                    all_properties.add(prop_lab)
                    always_properties.add(prop_lab)
                    cur_properties.add(prop_lab)
            j += 1
        else:
            for prop_lab in props_labs:
                if prop_lab in distinct_labels:
                    all_labels.add(prop_lab)
                    cur_labels.add(prop_lab)
                else:
                    all_properties.add(prop_lab)
                    cur_properties.add(prop_lab)

        always_labels = always_labels.intersection(cur_labels)
        always_properties = always_properties.intersection(cur_properties)

    # identify optionnal labels and properties with the always_labels et and always_properties
    optional_labels = all_labels - always_labels
    optional_properties = all_properties - always_properties

    # add a question mark for optionnal labels and properties
    if optional_labels != set():
        labels = (
            ":".join(sorted(list(always_labels)))
            + ":?"
            + ":?".join(sorted(list(optional_labels)))
        )
    else:
        labels = ":".join(sorted(list(always_labels)))

    if optional_properties != set():
        properties = (
            ":".join(sorted(list(always_properties)))
            + ":?"
            + ":?".join(sorted(list(optional_properties)))
        )
    else:
        properties = ":".join(sorted(list(always_properties)))

    # if the formed cluster does not exist
    if labels + properties not in run_clusters:
        # line id
        data_line = [str(cluster_id)]

        data_line.append(labels)

        data_line.append(properties)

        # parent line id
        data_line.append(str(parent_id))

        # type name
        data_line.append("T" + str(k))

        # is not a base type
        data_line.append("no")

        writer.writerow(data_line)

        run_clusters.append(labels + properties)

        new_parent_id = cluster_id

        # take neo4j_id
        data_line.append(node.split()[0])

        k += 1

        cluster_id += 1

        # search for more subtypes
        if hierarchy_tree[1] is not None:
            cluster_id, k = rec_storing(
                distinct_labels,
                labs_sets,
                writer,
                hierarchy_tree[1],
                cluster_id,
                new_parent_id,
                run_clusters,
                k,
                neo4j_node_cluster_mapping
            )
        if hierarchy_tree[2] is not None:
            cluster_id, k = rec_storing(
                distinct_labels,
                labs_sets,
                writer,
                hierarchy_tree[2],
                cluster_id,
                new_parent_id,
                run_clusters,
                k,
                neo4j_node_cluster_mapping
            )

    return cluster_id, k
