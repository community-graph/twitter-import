import os

from lib.utils import import_links, decrypt_value


def lambda_handler(event, context):
    print("Event:", event)
    version_updated = "Default (Updating public graph)"
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', "test")
    NEO4J_URL = os.environ.get('NEO4J_URL', "bolt://localhost")

    TWITTER_BEARER = decrypt_value(os.environ['TWITTER_BEARER'])

    if event and event.get("resources"):
        if "CommunityGraphTwitterImportPublic" in event["resources"][0]:
            version_updated = "Updating public graph"
            NEO4J_PASSWORD = decrypt_value(os.environ['NEO4J_PASSWORD'])
            NEO4J_URL = os.environ.get('NEO4J_PUBLIC_URL')
        elif "CommunityGraphTwitterImportPrivate" in event["resources"][0]:
            version_updated = "Updating private graph"
            NEO4J_PASSWORD = decrypt_value(os.environ['NEO4J_PRIVATE_PASSWORD'])
            NEO4J_URL = os.environ.get('NEO4J_PRIVATE_URL')

    neo4jPass = NEO4J_PASSWORD
    bearerToken = TWITTER_BEARER
    neo4jUrl = NEO4J_URL
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    search = os.environ.get("TWITTER_SEARCH")

    print(version_updated)
    import_links(neo4jUrl=neo4jUrl, neo4jUser=neo4jUser, neo4jPass=neo4jPass, bearerToken=bearerToken, search=search)


if __name__ == "__main__":
    neo4jPass = os.environ.get('NEO4J_PASSWORD', "test")
    bearerToken = os.environ.get('TWITTER_BEARER', "")
    neo4jUrl = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    search = os.environ.get("TWITTER_SEARCH",
                            'neo4j OR "graph database" OR "graph databases" OR graphdb OR graphconnect OR @neoquestions OR @Neo4jDE OR @Neo4jFr OR neotechnology')

    import_links(neo4jUrl=neo4jUrl, neo4jUser=neo4jUser, neo4jPass=neo4jPass, bearerToken=bearerToken, search=search)
