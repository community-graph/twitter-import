import socket

import requests
import boto3
import os

from neo4j.v1 import GraphDatabase, basic_auth
from base64 import b64decode
from bs4 import BeautifulSoup, Tag

def lambda_handler(event, context):
    print("Event:", event)
    version_updated = "Default (Updating public graph)"
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', "test")
    NEO4J_URL = os.environ.get('NEO4J_URL', "bolt://localhost")

    if event and event.get("resources"):
        if "CommunityGraphTwitterHydrateLinksPublic" in event["resources"][0]:
            version_updated = "Updating public graph"
            ENCRYPTED_NEO4J_PASSWORD = os.environ['NEO4J_PASSWORD']
            NEO4J_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_NEO4J_PASSWORD))['Plaintext']
            NEO4J_URL = os.environ.get('NEO4J_PUBLIC_URL')
        elif "CommunityGraphTwitterHydrateLinksPrivate" in event["resources"][0]:
            version_updated = "Updating private graph"
            ENCRYPTED_NEO4J_PASSWORD = os.environ['NEO4J_PRIVATE_PASSWORD']
            NEO4J_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_NEO4J_PASSWORD))['Plaintext']
            NEO4J_URL = os.environ.get('NEO4J_PRIVATE_URL')

    neo4jUrl = NEO4J_URL
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    neo4jPass = NEO4J_PASSWORD

    print(version_updated)
    hydrate_links(neo4jUrl = neo4jUrl, neo4jUser = neo4jUser, neo4jPass = neo4jPass)


def hydrate_links(neo4jUrl, neo4jUser, neo4jPass):
    driver = GraphDatabase.driver(neo4jUrl, auth=basic_auth(neo4jUser, neo4jPass))
    session = driver.session()
    result = session.run(
        "MATCH (link:Link) WHERE not exists(link.title) RETURN id(link) as id, link.url as url ORDER BY ID(link) DESC LIMIT {limit}",
        {"limit": 100})
    update = []
    rows = 0
    for record in result:
        try:
            print(record["url"])
            title = hydrate_url(record["url"])
            rows += 1
            update += [{"id": record["id"], "title": title}]
        except socket.gaierror:
            print("Failed to resolve {0}. Ignoring for now".format(record["url"]))
        except socket.error:
            print("Failed to connect to {0}. Ignoring for now".format(record["url"]))

    print("urls", len(update), "records", rows)
    result = session.run(
        "UNWIND {data} AS row MATCH (link) WHERE id(link) = row.id SET link.title = row.title",
        {"data": update})
    print(result.consume().counters)
    session.close()


def hydrate_url(url):
    user_agent = {'User-agent': 'Mozilla/5.0'}
    potential_title = []
    try:
        if url:
            r = requests.get(url, headers=user_agent)
            response = r.text
            page = BeautifulSoup(response, "html.parser")
            potential_title = page.find_all("title")
    except requests.exceptions.ConnectionError:
        print("Failed to connect: ", url)

    if len(potential_title) == 0:
        print("Skipping: ", url)
        return "N/A"
    else:
        return potential_title[0].text

if __name__ == "__main__":
    neo4jPass = os.environ.get('NEO4J_PASSWORD', "test")
    neo4jUrl = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4jUser = os.environ.get('NEO4J_USER', "neo4j")
    hydrate_links(neo4jUrl=neo4jUrl, neo4jUser=neo4jUser, neo4jPass=neo4jPass)