import datetime
import sys
import boto
import flask
import time

from ago import human
from flask import render_template
from neo4j.v1 import GraphDatabase
from datetime import tzinfo, timedelta, datetime

ZERO = timedelta(0)

class UTC(tzinfo):
    def utcoffset(self, dt):
        return ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return ZERO

products = {
    "neo4j": {
        "url": "138.197.15.1",
        "user": "all",
        "password": "readonly",
        "title": "Neo4j",
        "summary": "twin4j"
    },
    "graphql": {
        "url": "107.170.69.23",
        "user": "graphql",
        "password": "graphql",
        "title": "GraphQL",
        "summary": "twigraphql"
    }
}


def github_links(tx):
    records = []
    for record in tx.run("""\
        MATCH (n:Repository) WHERE EXISTS(n.created) AND n.updated > timestamp() - 7 * 60 * 60 * 24 * 1000
        WITH n
        ORDER BY n.updated desc
        MATCH (n)<-[:CREATED]-(user) WHERE NOT (user.name IN ["neo4j", "neo4j-contrib"])
        RETURN n.title, n.url, n.created, n.favorites, n.updated, user.name, n.created_at, n.updated_at
        ORDER BY n.updated desc
        """):
        records.append(record)
    return records


def twitter_links(tx):
    records = []
    for record in tx.run("""\
        WITH ((timestamp() / 1000) - (7 * 24 * 60 * 60)) AS oneWeekAgo
        MATCH (l:Link)<--(t:Tweet:Content)
        WHERE not(t:Retweet)
        WITH oneWeekAgo, l, t
        ORDER BY l.cleanUrl, toInteger(t.created)
        WITH oneWeekAgo, l.cleanUrl AS url, l.title AS title, collect(t) AS tweets WHERE toInteger(tweets[0].created) is not null AND tweets[0].created > oneWeekAgo AND not url contains "neo4j.com"
        RETURN url, title, REDUCE(acc = 0, tweet IN tweets | acc + tweet.favorites + size((tweet)<-[:RETWEETED]-())) AS score, tweets[0].created * 1000 AS dateCreated, [ tweet IN tweets | head([ (tweet)<-[:POSTED]-(user) | user.screen_name]) ] AS users
        ORDER BY score DESC
        """):
        records.append(record)
    return records


def meetup_events(tx):
    records = []
    for record in tx.run("""\
    MATCH (event:Event)<-[:CONTAINED]-(group)
    WHERE timestamp() + 7 * 60 * 60 * 24 * 1000 > event.time > timestamp() - 7 * 60 * 60 * 24 * 1000
    RETURN event, group
    ORDER BY event.time
    """):
        records.append(record)
    return records

app = flask.Flask('my app')


@app.template_filter('humanise')
def humanise_filter(value):
    return human(datetime.fromtimestamp(value / 1000), precision=1)


@app.template_filter("shorten")
def shorten_filter(value):
    return (value[:75] + '..') if len(value) > 75 else value


def generate_page(product):
    driver = GraphDatabase.driver("bolt://{0}:7687".format(product["url"]), auth=(product["user"], product["password"]))
    with driver.session() as session:
        github_records = session.read_transaction(github_links)
        twitter_records = session.read_transaction(twitter_links)
        meetup_records = session.read_transaction(meetup_events)

    with app.app_context():
        utc = UTC()
        time_now = str(datetime.now(utc))

        rendered = render_template('index.html',
                                   github_records=github_records,
                                   twitter_records=twitter_records,
                                   meetup_records=meetup_records,
                                   title=product["title"],
                                   time_now=time_now)

        local_file_name = "/tmp/{0}.html".format(product["summary"])
        with open(local_file_name, "w") as file:
            file.write(rendered.encode('utf-8'))

        s3_connection = boto.connect_s3()
        bucket = s3_connection.get_bucket(product["summary"])
        key = boto.s3.key.Key(bucket, "{0}.html".format(product["summary"]))
        key.set_contents_from_filename(local_file_name)


def lambda_handler(event, context):
    print("Event:", event)

    product_name = "neo4j"
    if event and event.get("resources"):
        if "GraphQLGenerateSummaryPage" in event["resources"][0]:
            product_name = "graphql"
        if "Neo4jGenerateSummaryPage" in event["resources"][0]:
            product_name = "neo4js"

    generate_page(products[product_name])


if __name__ == "__main__":
    args = sys.argv[1:]
    product_name = args[0] if args[0:] else "neo4j"
    generate_page(products[product_name])
