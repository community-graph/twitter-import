import os
import time
import requests
import urllib
from neo4j.v1 import GraphDatabase, basic_auth

def import_twitter(event, context):

    neo4jUrl = os.environ.get('NEO4J_URL',"bolt://localhost")
    neo4jUser = os.environ.get('NEO4J_USER',"neo4j")
    neo4jPass = os.environ.get('NEO4J_PASSWORD',"test")
    bearerToken = os.environ.get('TWITTER_BEARER',"")

    if len(bearerToken) == 0 :
        raise("No Twitter Bearer token configured")

    driver = GraphDatabase.driver(neo4jUrl, auth=basic_auth(neo4jUser, neo4jPass))

    session = driver.session()

    # Add uniqueness constraints.
    session.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
    session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
    session.run("CREATE INDEX ON :Tag(name);")
    session.run("CREATE INDEX ON :Link(url);")

    # Build query.
    importQuery = """
    UNWIND {tweets} AS t

    WITH t
    ORDER BY t.id

    WITH t,
         t.entities AS e,
         t.user AS u,
         t.retweeted_status AS retweet

    MERGE (tweet:Tweet {id:t.id})
    SET tweet:Content, tweet.text = t.text,
        tweet.created = t.created_at,
        tweet.favorites = t.favorite_count

    MERGE (user:User {screen_name:u.screen_name})
    SET user.name = u.name,
        user.location = u.location,
        user.followers = u.followers_count,
        user.following = u.friends_count,
        user.statuses = u.statuses_count,
        user.profile_image_url = u.profile_image_url

    MERGE (user)-[:POSTED]->(tweet)

    FOREACH (h IN e.hashtags |
      MERGE (tag:Tag {name:LOWER(h.text)})
      MERGE (tag)<-[:TAGGED]-(tweet)
    )

    FOREACH (u IN e.urls |
      MERGE (url:Link {url:u.expanded_url})
      MERGE (tweet)-[:LINKED]->(url)
    )

    FOREACH (m IN e.user_mentions |
      MERGE (mentioned:User {screen_name:m.screen_name})
      ON CREATE SET mentioned.name = m.name
      MERGE (tweet)-[:MENTIONED]->(mentioned)
    )

    FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
      MERGE (reply_tweet:Tweet {id:r})
      MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
    )

    FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
        MERGE (retweet_tweet:Tweet {id:retweet_id})
        MERGE (tweet)-[:RETWEETED]->(retweet_tweet)
    )
    """

    # todo as params
    q = urllib.quote(os.environ.get("TWITTER_SEARCH",'neo4j OR "graph database" OR "graph databases" OR graphdb OR graphconnect OR @neoquestions OR @Neo4jDE OR @Neo4jFr OR neotechnology'))
    maxPages = 20
    catch_up = True
    count = 100
    result_type = "recent"
    lang = "en"

    since_id = -1
    max_id = -1
    page = 1

    hasMore = True
    while hasMore and page <= maxPages:
        if catch_up:
            result = session.run("MATCH (t:Tweet) RETURN max(t.id) as sinceId")
            for record in result:
                since_id = record["sinceId"]
        else:
            result = session.run("MATCH (t:Tweet) RETURN min(t.id) as maxId")
            for record in result:
                max_id = record["maxId"]

        # Build URL.
        apiUrl = "https://api.twitter.com/1.1/search/tweets.json?q=%s&count=%s&result_type=%s&lang=%s" % (q, count, result_type, lang)
        if since_id != -1 :
            apiUrl += "&since_id=%s" % (since_id)
        if max_id != -1 :
            apiUrl += "&max_id=%s" % (max_id)

        print(apiUrl)

        response = requests.get(apiUrl, headers = {"accept":"application/json","Authorization":"Bearer " + bearerToken})
        if response.status_code != 200:
            print(response)
            # raise("%s : %s" % (response.status_code, response.text))

        json = response.json()
        meta = json["search_metadata"]
        # print(meta)

        tweets = json.get("statuses",[])
        print(len(tweets))
        if len(tweets) > 0:
            result = session.run(importQuery,{"tweets":tweets})
            print(result.consume().counters)
            page = page + 1
        else:
            hasMore = False

        print("page {page} max_id {max_id}".format(page=page,max_id=max_id))
        time.sleep(1)
        if json.get('backoff',None) != None:
            print("backoff",json['backoff'])
            time.sleep(json['backoff']+5)

if __name__ == "__main__":
    import_twitter(None, None)