import time
import requests
import urllib
import boto3

from neo4j.v1 import GraphDatabase, basic_auth


def import_links(neo4jUrl, neo4jUser, neo4jPass, bearerToken, search):
    if len(bearerToken) == 0:
        raise ("No Twitter Bearer token configured")

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

    MERGE (tweet:Tweet:Twitter {id:t.id})
    SET tweet:Content, tweet.text = t.text,
        tweet.created_at = t.created_at,
        tweet.created = apoc.date.parse(t.created_at,'s','E MMM dd HH:mm:ss Z yyyy'),
        tweet.favorites = t.favorite_count

    MERGE (user:User {screen_name:u.screen_name})
    SET user.name = u.name, user.id = u.id,
        user.location = u.location,
        user.followers = u.followers_count,
        user.following = u.friends_count,
        user.statuses = u.statuses_count,
        user.profile_image_url = u.profile_image_url,
        user:Twitter

    MERGE (user)-[:POSTED]->(tweet)

    FOREACH (h IN e.hashtags |
      MERGE (tag:Tag {name:LOWER(h.text)}) SET tag:Twitter
      MERGE (tag)<-[:TAGGED]-(tweet)
    )

    FOREACH (u IN e.urls |
      MERGE (url:Link {url:u.expanded_url})
      ON CREATE SET url.short = case when length(u.expanded_url) < 25 then true else null end
      SET url:Twitter
      MERGE (tweet)-[:LINKED]->(url)
    )

    FOREACH (m IN e.user_mentions |
      MERGE (mentioned:User {screen_name:m.screen_name})
      ON CREATE SET mentioned.name = m.name, mentioned.id = m.id
      SET mentioned:Twitter
      MERGE (tweet)-[:MENTIONED]->(mentioned)
    )

    FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
      MERGE (reply_tweet:Tweet:Twitter {id:r})
      MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
      SET tweet:Reply
    )

    FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
        MERGE (retweet_tweet:Tweet:Twitter {id:retweet_id})
        MERGE (tweet)-[:RETWEETED]->(retweet_tweet)
        SET tweet:Retweet
    )
    """

    # todo as params

    # """
    # dumpdevos OR #rejectrex OR #resist OR #nodapl OR #theresistance OR #resistance OR #factsmatter OR #nobannowall OR
    # presson OR #notmypresident OR #alternativefacts OR
    # maga OR president OR @realdonaldtrump OR @GOP OR @POTUS OR devos OR tillerson OR #scotus"""

    q = urllib.quote_plus(search)
    maxPages = 100
    # False for retrieving history, True for catchup forward
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
            result = session.run("MATCH (t:Tweet:Content) RETURN max(t.id) as sinceId")
            for record in result:
                print(record)
                if record["sinceId"] != None:
                    since_id = record["sinceId"]
                    #    else:
                    #        result = session.run("MATCH (t:Tweet:Content) RETURN min(t.id) as maxId")
                    #        for record in result:
                    #            if record["maxId"] != None:
                    #                max_id = record["maxId"]

                    # Build URL.
        apiUrl = "https://api.twitter.com/1.1/search/tweets.json?q=%s&count=%s&result_type=%s&lang=%s" % (
            q, count, result_type, lang)
        if since_id != -1:
            apiUrl += "&since_id=%s" % (since_id)
        if max_id != -1:
            apiUrl += "&max_id=%s" % (max_id)
        # print(apiUrl)
        response = requests.get(apiUrl,
                                headers={"accept": "application/json", "Authorization": "Bearer " + bearerToken})
        if response.status_code <> 200:
            raise (Exception(response.status_code, response.text))

        json = response.json()
        meta = json["search_metadata"]
        #    print(meta)
        if not catch_up and meta.get('next_results', None) != None:
            max_id = meta["next_results"].split("=")[1][0:-2]
        tweets = json.get("statuses", [])
        # print(len(tweets))
        if len(tweets) > 0:
            result = session.run(importQuery, {"tweets": tweets})
            print(result.consume().counters)
            page = page + 1

        hasMore = len(tweets) == count

        print("catch_up", catch_up, "more", hasMore, "page", page, "max_id", max_id, "since_id", since_id, "tweets",
              len(tweets))
        time.sleep(1)
        #    if json.get('quota_remaining',0) <= 0:
        #        time.sleep(10)
        if json.get('backoff', None) != None:
            print("backoff", json['backoff'])
            time.sleep(json['backoff'] + 5)

    session.close()

def decrypt_value(encrypted):
    return boto3.client('kms').decrypt(CiphertextBlob=b64decode(encrypted))['Plaintext']
