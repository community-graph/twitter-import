from urllib import urlencode
from urlparse import urlparse, urlunparse, parse_qs
from neo4j.v1 import GraphDatabase, basic_auth
import os

def clean_uri(url):
    u = urlparse(url)
    query = parse_qs(u.query)

    for param in ["utm_content", "utm_source", "utm_medium", "utm_campaign", "utm_term"]:
        query.pop(param, None)

    u = u._replace(query=urlencode(query, True))
    return urlunparse(u)


neo4jPass = os.environ.get('NEO4J_PASSWORD', "test")
neo4jUrl = os.environ.get('NEO4J_URL', "bolt://localhost")
neo4jUser = os.environ.get('NEO4J_USER', "neo4j")

driver = GraphDatabase.driver(neo4jUrl, auth=basic_auth(neo4jUser, neo4jPass))

query = "MATCH (l:Link) WHERE NOT EXISTS(l.cleanUrl) RETURN l, ID(l) AS internalId"

session = driver.session()
result = session.run(query)

updates = []
for row in result:
    uri = row["l"]["url"]
    if uri:
        uri = uri.encode('utf-8')
        # print(uri, clean_uri(uri), row["internalId"])
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
