import pandas as pd
import sqlite3
from jira import JIRA
from os import getenv
from datetime import datetime, timedelta

jira_connect_config = {
    'user': getenv("JIRA_SCRAPE_API_USER"), # myemail@example.com
    'server': getenv("JIRA_SCRAPE_API_SERVER"), # https://company-name-example.atlassian.net
    'scrape_api_token': getenv("JIRA_SCRAPE_API_TOKEN"), # API token
}

jira_options = {'server': jira_connect_config['server']}
jira = JIRA(options=jira_options, basic_auth=(jira_connect_config['user'], jira_connect_config['scrape_api_token'],))

days_back = 5
date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
print(f"Fetching tickets updated since: {date_str}")

tickets_data = []
users_dict = {}

# get list of ALL projects, available with this api token
projects = [proj.key for proj in jira.projects()]

projects_jql_string = ''
for idx in range(len(projects)):
    proj_id = projects[idx]
    projects_jql_string += f'project={proj_id}'
    if idx < len(proj_id) - 1:
        projects_jql_string += ' OR '

jql = f'({projects_jql_string}) AND updated >= "{date_str}" ORDER BY updated DESC'

print(f"Fetching tickets using this jql:\n\t{jql}")
# maxResults=False means get all results, the library would handle all the pagination
# and api throttling synchronously
issues = jira.search_issues(jql, maxResults=False)

for issue in issues:
    reporter = issue.fields.reporter
    assignee = issue.fields.assignee

    tickets_data.append({
        'ticket_id': issue.key,
        'summary': issue.fields.summary,
        'description': issue.fields.description if issue.fields.description else None,
        'updated': issue.fields.updated,
        'reporter_id': reporter.accountId if reporter else None,
        'assignee_id': assignee.accountId if assignee else None
    })

    for user in [reporter, assignee]:
        if user and user.accountId not in users_dict:
            # Fetch extended user info
            user_detailed = jira.user(user.accountId)
            users_dict[user.accountId] = {
                'user_id': user_detailed.accountId,
                'display_name': user_detailed.displayName,
                'email': getattr(user_detailed, 'emailAddress', None),
                'active': getattr(user_detailed, 'active', None),
                'time_zone': getattr(user_detailed, 'timeZone', None)
            }

users_df = pd.DataFrame(users_dict.values())
tickets_df = pd.DataFrame(tickets_data)

print("\nUsers DataFrame:")
print(users_df)
print("\nTickets DataFrame:")
print(tickets_df)

conn = sqlite3.connect('jira_data.db')
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT,
    email TEXT,
    active BOOLEAN,
    time_zone TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tickets (
    ticket_id TEXT PRIMARY KEY,
    summary TEXT,
    description TEXT,
    updated TEXT,
    reporter_id TEXT,
    assignee_id TEXT,
    FOREIGN KEY (reporter_id) REFERENCES users(user_id),
    FOREIGN KEY (assignee_id) REFERENCES users(user_id)
);
""")

# this would not work for upserts
# users_df.to_sql('users', conn, if_exists='append', index=False)
# tickets_df.to_sql('tickets', conn, if_exists='append', index=False)

# Prepare user rows
user_rows = [tuple(row) for _, row in users_df.iterrows()]

# Prepare ticket rows
ticket_rows = [tuple(row) for _, row in tickets_df.iterrows()]

# Upsert users
cursor.executemany("""
    INSERT INTO users (user_id, display_name, email, active, time_zone)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
        display_name=excluded.display_name,
        email=excluded.email,
        active=excluded.active,
        time_zone=excluded.time_zone;
""", user_rows)

# Upsert tickets
cursor.executemany("""
    INSERT INTO tickets (ticket_id, summary, description, updated, reporter_id, assignee_id)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(ticket_id) DO UPDATE SET
        summary=excluded.summary,
        description=excluded.description,
        updated=excluded.updated,
        reporter_id=excluded.reporter_id,
        assignee_id=excluded.assignee_id;
""", ticket_rows)

conn.commit()
conn.close()
print("\n✅ Data with extended user info inserted into jira_data.db")
