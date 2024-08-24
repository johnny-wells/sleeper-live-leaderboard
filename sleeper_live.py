import requests
import json
import pandas as pd
import gspread # Google Sheets module

# Gspread vars (must write df to gsheet to make data live in Tableau Public)
gc = gspread.service_account()
sh = gc.open('Mahomies Live Points Leaderboard')
ws = sh.sheet1

mahomies = 'xxxxxxxxxxxxxxxxxx' # League ID

# Endpoints
league = 'https://api.sleeper.app/v1/league/' # <league_id>
players = 'https://api.sleeper.app/v1/players/nfl' # Only parameter is sport
rosters = 'https://api.sleeper.app/v1/league/' # <league_id>/rosters
matchups = 'https://api.sleeper.app/v1/league/' # <league_id>/matchups/<week>
users = 'https://api.sleeper.app/v1/league/' # <league_id>/users
state = 'https://api.sleeper.app/v1/state/nfl' # Only parameter is sport
projections = ('https://api.sleeper.com/projections/nfl/player/6794?season_typ'
               'e=regular&season=2023&grouping=week')

users_data = requests.get(users + mahomies + '/users')
users_json = users_data.text
users_obj = json.loads(users_json)

rosters_data = requests.get(rosters + mahomies + '/rosters')
rosters_json = rosters_data.text
rosters_obj = json.loads(rosters_json)

# Create list of lists to later make into dataframe. Numbers are roster_IDs
owners_list = [[5, 'Carlos'],
               [1, 'Mike'],
               [8, 'Johnny'],
               [9,'Gavin'],
               [7, 'Justice'],
               [2, 'Aswad'],
               [6,'Cody'],
               [3,'Jake'],
               [10,'Tim'],
               [11,'Rudy'],
               [4,'Matt'],
               [12,'Greg']]

# Column names for dataframe
owners_columns = ['id','Name']

owners_df = pd.DataFrame(owners_list,columns=owners_columns)
owners_df.set_index('id',inplace=True)

# State endpoint gives us the currrent week
    # Need to figure out when it changes
state_data = requests.get(state)
state_json = state_data.text
state_obj = json.loads(state_json)

# Pull matchup data for all weeks including current
completed_weeks = range(1,state_obj['week'] + 1)
weekly_data_dict = {}
for i in completed_weeks:
    matchups_data = requests.get(matchups + mahomies + '/matchups/' + str(i))
    matchups_json = matchups_data.text
    matchups_obj = json.loads(matchups_json)
    weekly_data_dict['week {0}'.format(i)] = matchups_obj

# Create lists for ids and point totals that we'll later combine into a df
roster_ids = []
point_totals = []
for i in weekly_data_dict.values():
    for j in i:
        roster_ids.append(j['roster_id'])
        point_totals.append(j['points'])

# Combine lists into dataframe
points = pd.DataFrame({'id' : roster_ids , 'Points' : point_totals})

# SELECT id, sum(points) FROM points GROUP BY id
points = points.groupby('id').sum()

# Join owners and points dfs on id for final df with names and points
final = pd.merge(owners_df,
                 points,
                 on='id',
                 how='left')

# Drop Jake, Tim, and Greg because they're not in on the side bet, then sort
final = final.drop([3,10,12]).sort_values(by='Points', ascending=False)

# Reset index so I can subtract all point values from first row
final = final.reset_index().drop('id', axis=1)

# Subtract all points values from first row to get Points Behind
final['Points Behind'] = final['Points'] - final.loc[0,'Points']

# Replace first place 0 with blank in Points Behind
final.loc[0,'Points Behind'] = ''

# Write final df to Google Sheet
ws.update([final.columns.values.tolist()] + final.values.tolist())
