from mastodon import Mastodon, StreamListener, MastodonNetworkError
import os
from dotenv import load_dotenv
from pprint import pprint
from kafka import KafkaProducer
import json
import time

load_dotenv()
access_token = os.getenv('access_token')



mastodon = Mastodon(access_token = access_token,
                    api_base_url="https://mastodon.social")
me = mastodon.account_verify_credentials()
print(me["username"])




#produceur kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')

)

AI = mastodon.timeline_hashtag(hashtag="AI", limit=1)
premier_toot = AI[0]                            # 1er niveau : un toot dans la liste
pprint(premier_toot)

dernier_id = None

while True:
    try:
        toots = mastodon.timeline_hashtag("AI", since_id=dernier_id)

        for toot in toots:
                    producer.send('mastodon_stream', {'account': toot["account"]["username"], "status": toot["id"], "content": toot["content"], 
                                        "language": toot["language"],"favourites_count": toot["favourites_count"],
                                        "reblogs_count": toot["reblogs_count"],"created_at": toot["created_at"].isoformat(),
                                        "hashtags": [tag["name"] for tag in toot["tags"]],})


        if toots:
            dernier_id = toots[0]["id"]

    except MastodonNetworkError as e:
        print(f"Erreur Mastodon : {e}")

    time.sleep(5)