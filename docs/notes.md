- Use GMM on embeddings
    - i.e. clustering embedding bectors directly.

- Create b_ref -> calculate similarity with **embeddings** -> continue
    - b_ref's embedding will be calculated as well.
    - This requires a look up table of embeddings of all possible keys (~10k keys).
    - Embedding of node is obtained by averaging embeddings of its keys and labels.

    - We might even ditch b_ref (use 0, median...)
        - With this, we can pre-calculate the embeddings.
        - That is because with the previous method, b_ref changes in every cluster (fictitious node).


Notes: 29-11-2024
DBP-15K isn't a "property graph".