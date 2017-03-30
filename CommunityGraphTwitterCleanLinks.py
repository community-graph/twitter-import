import socket

import httplib
import urlparse
import boto3
import os

from neo4j.v1 import GraphDatabase, basic_auth
from base64 import b64decode

if bool(os.environ.get('CREDENTIALS_ENCRYPTED', "")):
    ENCRYPTED_NEO4J_PASSWORD = os.environ['NEO4J_PASSWORD']
    NEO4J_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_NEO4J_PASSWORD))['Plaintext']

    ENCRYPTED_TWITTER_BEARER = os.environ['TWITTER_BEARER']
    TWITTER_BEARER = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_TWITTER_BEARER))['Plaintext']

def lambda_handler(event, context):
    neo4jUrl = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    neo4jPass = NEO4J_PASSWORD

    driver = GraphDatabase.driver(neo4jUrl, auth=basic_auth(neo4jUser, neo4jPass))

    session = driver.session()
    result = session.run(
        "MATCH (link:Link) WHERE exists(link.short) RETURN id(link) as id, link.url as url LIMIT {limit}",
        {"limit": 100})
    update = []
    rows = 0
    for record in result:
        try:
            resolved = unshorten_url(record["url"])
            rows += 1
            if resolved != record["url"]:
                update += [{"id": record["id"], "url": resolved}]
        except socket.gaierror:
            print("Failed to resolve {0}. Ignoring for now".format(record["url"]))
        except socket.error:
            print("Failed to connect to {0}. Ignoring for now".format(record["url"]))

    print("urls", len(update), "records", rows)
    result = session.run(
        "UNWIND {data} AS row MATCH (link) WHERE id(link) = row.id SET link.url = row.url REMOVE link.short",
        {"data": update})
    print(result.consume().counters)
    session.close()

def unshorten_url(url):
    print(url)
    if url == None or len(url) < 11:
        return url
    parsed = urlparse.urlparse(url)
    h = httplib.HTTPConnection(parsed.netloc, timeout=5)
    h.request('HEAD', parsed.path)
    response = h.getresponse()
    if response.status/100 == 3 and response.getheader('Location'):
        loc = str(response.getheader('Location'))
        print(url,parsed.netloc,loc,response.status)
        if loc <> url and len(loc) <= 22:
            return unshorten_url(loc)
        else:
            return loc
    else:
        return url


if __name__ == "__main__":
    lambda_handler(None, None)