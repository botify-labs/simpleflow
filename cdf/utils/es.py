def bulk(client, docs, chunk_size=500, bulk_type='index', **kwargs):
    bulk_actions = []
    for d in docs:
        action = {bulk_type: {}}
        for key in ('_id', '_index', '_parent', '_percolate', '_routing',
                    '_timestamp', '_ttl', '_type', '_version',):
            if key in d:
                action[bulk_type][key] = d.pop(key)

        bulk_actions.append(action)
        bulk_actions.append(d.get('_source', d))

    if not bulk_actions:
        return {}

    return client.bulk(bulk_actions, **kwargs)
