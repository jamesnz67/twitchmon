import time
import json
import csv
import logging
import sys
from pprint import pformat

import requests


def get_bearer(client_id, client_secret, auth_url):
	params = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
	r = requests.post(url=auth_url, params=params, timeout=20)
	if r.status_code == 200:
		return r.json()['access_token']	
	logging.error(f"failed to get bearer token: return code {r.status_code}. response text: {r.text}")

		
def get_category_id(category_name, config, bearer):
	url = f"{config['TWITCH_API_BASE']}helix/games"
	headers = {"Authorization": f"Bearer {bearer}", "Client-Id": config['TWITCH_CLIENT_ID']}
	params = {"name": category_name}
	r = requests.get(url, headers=headers, params=params, timeout=20)
	if r.status_code == 200:
		return r.json()['data'][0]['id']
	logging.error(f"failed to get category_id for {category_name}: return code {r.status_code}. response text: {r.text}")	


def get_results(pagination_cursor, config, streams_seen, usernames_seen, bearer, category_id):
	try:
		headers = {
			"Authorization": f"Bearer {bearer}",
			"Client-Id": config['TWITCH_CLIENT_ID'],
		}
		params = {
			"game_id": category_id,
			"first": '100',
		}
		if pagination_cursor:
			params['after'] = pagination_cursor
		url = f"{config['TWITCH_API_BASE']}helix/streams"
		r = requests.get(url, headers=headers, params=params, timeout=20)
		if r.status_code == 200:
			with open('searches.txt', 'r', encoding='utf-8') as searches, \
				open('usernames.txt', 'r', encoding='utf-8') as usernames:
					searches = {search.strip().lower() for search in searches}
					usernames = {username.strip().lower() for username in usernames}
			data = r.json().get('data')
			pagination = r.json()['pagination']
			pagination_cursor = pagination['cursor'] if pagination else None
			if data:
				logging.debug(f"retrieved {len(data)} streams")
				for stream in data:
					streamer_data = {
						'game_name': stream['game_name'],
						'language': stream['language'],
						'thumbnail_url': stream['thumbnail_url'],
						'title': stream['title'].lower(),
						'user_login': stream['user_login'],
						'user_name': stream['user_name'],
						'viewer_count': stream['viewer_count'],
					}
					if not streamer_data['title'] in streams_seen:
						if any(search in streamer_data['title'] for search in searches) or \
						(streamer_data['user_name'] in usernames) or \
						(streamer_data['user_login'] in usernames):
								logging.debug(pformat(streamer_data))
								streams_seen.add(streamer_data['title'])

					if not streamer_data['user_name'] in usernames_seen:
						usernames_seen.add(streamer_data['user_name'])
						with open('streamer_data.csv', 'a+', newline='', encoding='utf-8') as streamer_data_:
							csvwriter = csv.writer(streamer_data_, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
							csvwriter.writerow(streamer_data.values())
				return get_results(pagination_cursor, config, streams_seen, usernames_seen, bearer, category_id)
		else:
			logging.warning(f'Error: non-200 status code: {r.status_code}, retry in {config["TIMEOUT"]} secs')

	except Exception as e:
		raise e
		logging.error(f'Got Exception: {e}')

	logging.debug(f"sleeping {config['TIMEOUT']}s")
	return config['TIMEOUT']    


if __name__ == '__main__':
	with open('config.json', 'r') as config, open('streamer_data.csv', 'r', newline='', encoding='utf-8') as streamer_data:
		logging.basicConfig(level=logging.DEBUG)
		config = json.load(config)
		usernames_seen = {line[5] for line in csv.reader(streamer_data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)}
	streams_seen=set()
	bearer = get_bearer(config['TWITCH_CLIENT_ID'], config['TWITCH_CLIENT_SECRET'], config['OAUTH_URL']) 
	if bearer:
		category_id = get_category_id(config['TWITCH_CATEGORY'], config, bearer)
		if not category_id:
			sys.exit(f"failed to obtain category_id for game {config['category_name']}")
		while True:
			time.sleep(get_results(None, config, streams_seen, usernames_seen, bearer, category_id))
	else:
		sys.exit("failed to obtain bearer token.")