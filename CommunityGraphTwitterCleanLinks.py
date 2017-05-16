from urllib import urlencode
from urlparse import urlparse, urlunparse, parse_qs
from neo4j.v1 import GraphDatabase, basic_auth
import os

import boto3

from base64 import b64decode

def lambda_handler(event, context):
    print("Event:", event)
    version_updated = "Default (Updating public graph)"
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', "test")
    NEO4J_URL = os.environ.get('NEO4J_URL', "bolt://localhost")

    if event and event.get("resources"):
        if "CommunityGraphTwitterCleanLinksPublic" in event["resources"][0]:
            version_updated = "Updating public graph"
            ENCRYPTED_NEO4J_PASSWORD = os.environ['NEO4J_PASSWORD']
            NEO4J_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_NEO4J_PASSWORD))['Plaintext']
            NEO4J_URL = os.environ.get('NEO4J_PUBLIC_URL')
        elif "CommunityGraphTwitterCleanLinksPrivate" in event["resources"][0]:
            version_updated = "Updating private graph"
            ENCRYPTED_NEO4J_PASSWORD = os.environ['NEO4J_PRIVATE_PASSWORD']
            NEO4J_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_NEO4J_PASSWORD))['Plaintext']
            NEO4J_URL = os.environ.get('NEO4J_PRIVATE_URL')

    neo4jUrl = NEO4J_URL
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    neo4jPass = NEO4J_PASSWORD

    print(version_updated)
    clean_links(neo4jUrl = neo4jUrl, neo4jUser = neo4jUser, neo4jPass = neo4jPass)


def clean_links(neo4jUrl, neo4jUser, neo4jPass):
    driver = GraphDatabase.driver(neo4jUrl, auth=basic_auth(neo4jUser, neo4jPass))

    query = "MATCH (l:Link) WHERE NOT EXISTS(l.cleanUrl) RETURN l, ID(l) AS internalId"

    session = driver.session()
    result = session.run(query)

    updates = []
    for row in result:
        uri = row["l"]["url"]
        if uri:
            uri = uri.encode('utf-8')
            updates.append({"id": row["internalId"], "clean": clean_uri(uri)})

    print("Updates to apply", updates)

    updateQuery = """\
    UNWIND {updates} AS update
    MATCH (l:Link) WHERE ID(l) = update.id
    SET l.cleanUrl = update.clean
    """

    update_result = session.run(updateQuery, {"updates": updates})

    print(update_result)

    session.close()


def clean_uri(url):
    u = urlparse(url)
    query = parse_qs(u.query)

    for param in ["utm_content", "utm_source", "utm_medium", "utm_campaign", "utm_term"]:
        query.pop(param, None)

    u = u._replace(query=urlencode(query, True))
    return urlunparse(u)

if __name__ == "__main__":
    neo4jPass = os.environ.get('NEO4J_PASSWORD', "test")
    neo4jUrl = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    clean_links(neo4jUrl=neo4jUrl, neo4jUser=neo4jUser, neo4jPass=neo4jPass)