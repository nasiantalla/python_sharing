from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import psycopg2
import time
import json
import tweepy



#        replace mysql.server with "localhost" if you are running via your own server!
#                        server       MySQL username	MySQL pass  Database name.
conn = psycopg2.connect("dbname=marketing user=ba password=n8Ght_54aU34Dc_V19 host=bi-live-db1 port=5432")
c = conn.cursor()


#consumer key, consumer secret, access token, access secret.
ckey="oSYJ3bZEgpqqusNDYvaZUYGWh"
csecret="PT3iZ6dgBkgxwWb4EQUps6v3TAL9qao4FcLowVR12qUsxwSD2m"
atoken="97784671-DtEJsuP2bhzA5RevVhGvO5BS6l8gsCmx12IPfOIY5"
asecret="N6wQEDTfqE700hOt5vYNHoyo36R5FOSqgE5wTjyROEU6i"

class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)
        
        tweet = all_data["text"]
        
        username = all_data["user"]["screen_name"]

        created_at = all_data["created_at"]
        favourite_count = all_data["favorite_count"]
        retweet_count = all_data["retweet_count"]
        lang = all_data["lang"]
        place = all_data["place"]
        user_id = all_data["user"]["id"]

        user_location = all_data["user"]["location"]
        statuses_count = all_data["user"]["statuses_count"]
        hhashtags = all_data["entities"]["hashtags"]
        count_hash = 0
        hash_text = ''
        for j in hhashtags:
            hash_text = j['text']
            count_hash += 1

        count_media = 0
        display_url = ''
        hasmedia = all_data["entities"]["symbols"]
        for k in hasmedia:
            display_url = k["media_url"]
            count_media += 1

        in_reply_to_status_id = all_data["in_reply_to_status_id"]
        if in_reply_to_status_id is None:
            in_reply_to_status_id = 0

        in_reply_to_user_id = all_data["in_reply_to_user_id"]
        if in_reply_to_user_id is None:
            in_reply_to_user_id = 0

        retweeted = all_data["retweeted"]
        usermentions = all_data["entities"]["user_mentions"]
        count_mentions = 0
        for l in usermentions:
            usermentions_id = l["id_str"]
            men_name = l['name']
            men_screen_name = l['screen_name']
            count_mentions += 1            
        
        c.execute("INSERT INTO performance_marketing_data.twitter_competition (username, tweet, created_at, favourite_count, retweet_count, lang, user_id, user_location, statuses_count, count_hash, count_media, in_reply_to_status_id, in_reply_to_user_id, retweeted, count_mentions) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (username, tweet, created_at, favourite_count, retweet_count, lang, user_id, user_location, statuses_count, count_hash, count_media, in_reply_to_status_id, in_reply_to_user_id, retweeted, count_mentions))

        conn.commit()

        print((username,tweet))
        
        return True

    def on_error(self, status):
        print(status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)


# api = tweepy.API(auth)

# query = 'Beirut'
# max_tweets = 1000
# searched_tweets = [status for status in tweepy.Cursor(api.search, q=query).items(max_tweets)]

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["lieferando", "deliveroo"])