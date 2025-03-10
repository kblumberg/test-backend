import json
import requests
import pandas as pd
from utils.utils import get_base_path, read_csv
from utils.db import TWITTER_BEARER_TOKEN, TWITTER_KOL_COLS, PROJECT_COLS, load_data_from_pg, upload_data_to_pg, execute_pg_query

def get_user_by_username(row):
	url = f"https://api.twitter.com/2/users/by/username/{row['username']}"

	headers = {
		"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}",
	}

	params = {
		"user.fields": "id,username,name,description,public_metrics"
	}

	response = requests.get(url, headers=headers, params=params)

	if response.status_code == 200:
		user_data = response.json().get("data", {})
		d = {
			'id': user_data.get('id'),
			'name': user_data.get('name'),
			'username': user_data.get('username'),
			'description': user_data.get('description'),
			'followers_count': user_data['public_metrics']['followers_count'],
			'associated_project_id': row['associated_project_id'],
			'account_type': row['account_type'],
			'tracking': row['tracking']
		}
		return d
	else:
		print(f"Error: {response.status_code} - {response.text}")

def update_users(df = None):
	cols = ['associated_project_id', 'username', 'tracking', 'ecosystem', 'account_type']
	if df is None:
		# we need: associated_project_id | username | tracking | ecosystem | account_type
		fname = get_base_path() + 'data/update_users.csv'
		update_users = read_csv(fname)[cols]
	else:
		update_users = df[cols]

	update = []
	for _, row in update_users.iterrows():
		user = get_user_by_username(row)
		update.append(user)

	update_df = pd.DataFrame(update)[TWITTER_KOL_COLS]
	upload_data_to_pg(update_df, 'twitter_kols')
	return update_df


def update_projects():
	# we need: associated_project_id | username | tracking | ecosystem | account_type
	cols = ['username', 'tracking', 'ecosystem', 'tags', 'parent_project_id']
	fname = get_base_path() + 'data/update_projects.csv'
	update_projects = read_csv(fname)[cols]
	print(update_projects)
	update_projects['account_type'] = 'project'
	update_projects['tags'] = update_projects['tags'].apply(lambda x: x.split(',') if x == x else [])
	update_projects['tags'] = update_projects['tags'].apply(json.dumps)
	# update_users['parent_project_id'] = None

	update_projects['associated_project_id'] = None
	update_users_df = update_users(update_projects)

	update_df = pd.merge(update_users_df[['username','name','description']], update_projects, on='username', how='left')
	update_df['name'] = update_df['name'].apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	update_users_df['name'] = update_users_df['name'].apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	d = {
		'elizaOS': 'eliza',
	}
	update_df['name'] = update_df['name'].apply(lambda x: d.get(x, x))
	update_users_df['name'] = update_users_df['name'].apply(lambda x: d.get(x, x))
	update_df = update_df[PROJECT_COLS]

	upload_data_to_pg(update_df, 'projects')
	
	query = 'select * from projects'
	new_projects = load_data_from_pg(query)
	new_projects = new_projects[new_projects.name.isin(update_df.name)]
	new_users = pd.merge(new_projects, update_users_df[['name','id']], on='name', how='inner')

	for _, row in new_users.iterrows():
		query = f"update twitter_kols set associated_project_id = {row['id_x']} where id = {row['id_y']}"
		print(query)
		execute_pg_query(query)

def get_users_in_list(list_id='1896993199560032554'):
	headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
	url = f"https://api.twitter.com/2/lists/{list_id}/members"

	response = requests.get(url, headers=headers)
	users = []
	if response.status_code == 200:
		users = response.json().get("data", [])
		next_token = response.json().get("meta", {}).get("next_token")
		while next_token:
			url = f"https://api.twitter.com/2/lists/{list_id}/members?pagination_token={next_token}"
			response = requests.get(url, headers=headers)
			users += response.json().get("data", [])
			next_token = response.json().get("meta", {}).get("next_token")
	else:
		print("Error:", response.json())
	return users

def update_tracking():
	users = get_users_in_list()
	query = 'select * from twitter_kols'
	df = load_data_from_pg(query)
	ids = [int(user['id']) for user in users]
	df['tracking_x'] = df.id.isin(ids)
	g = df[df.tracking != df.tracking_x]
	for _, row in g.iterrows():
		query = f"update twitter_kols set tracking = {row['tracking_x']} where id = {row['id']}"
		print(query)
		execute_pg_query(query)
	new_ids = g[g.tracking_x == True].id.unique()

def update_projects_direct():
	cols = ['name', 'parent_project_id', 'description', 'ecosystem', 'tags']
	fname = get_base_path() + 'data/update_projects.csv'
	update_projects = read_csv(fname)[cols]

def check_projects():
	query = 'select p.name as project_name, p.id as project_id, t.* from projects p left join twitter_kols t on p.id = t.associated_project_id and t.account_type = \'project\' order by p.id'
	df = load_data_from_pg(query)
	df.to_csv('~/Downloads/projects.csv', index=False)


