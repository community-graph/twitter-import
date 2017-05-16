import os

from lib.utils import import_links, decrypt_value


def lambda_handler(event, context):
    print("Event:", event)
    version_updated = "Default (Updating GraphQL graph)"
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', "test")
    NEO4J_URL = os.environ.get('NEO4J_URL', "bolt://localhost")
    TWITTER_BEARER = os.environ['TWITTER_BEARER']

    if event and event.get("resources"):
        if "GraphQLCommunityGraphTwitterImport" in event["resources"][0]:
            NEO4J_PASSWORD = decrypt_value(os.environ['NEO4J_PASSWORD'])
            TWITTER_BEARER = decrypt_value(os.environ['TWITTER_BEARER'])

    neo4jPass = NEO4J_PASSWORD
    bearerToken = TWITTER_BEARER
    neo4jUrl = NEO4J_URL
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    search = os.environ.get("TWITTER_SEARCH")

    print(version_updated)
    import_links(neo4jUrl=neo4jUrl, neo4jUser=neo4jUser, neo4jPass=neo4jPass, bearerToken=bearerToken, search=search)

if __name__ == "__main__":
    neo4jPass = os.environ.get('NEO4J_PASSWORD', "neo")
    bearerToken = os.environ.get('TWITTER_BEARER', "")
    neo4jUrl = os.environ.get('NEO4J_URL', "bolt://127.0.0.1")
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    search = os.environ.get("TWITTER_SEARCH",
                            'graphql OR @apollo OR @graphcool OR graphql-js OR @leeb OR #graphql OR @graphql')

    import_links(neo4jUrl=neo4jUrl, neo4jUser=neo4jUser, neo4jPass=neo4jPass, bearerToken=bearerToken, search=search)
