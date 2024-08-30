import pandas as pd
import sys
import os


# Configure the root directory path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

def load_data(db:object, cursor:object, data:pd.DataFrame) -> None:
    try:
        # Create the table
        table_name = 'transformed_premier_league_table'
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            position INT PRIMARY KEY,
            team_name VARCHAR(50),
            games_played INT,
            won INT,
            drawn INT,
            lost INT,
            goals_scored INT,
            goals_conceded INT,
            goal_difference INT,
            points INT
        )
        ''')

        data_to_insert = [tuple(row) for index, row in data.iterrows()]
        
        query = f'''
            INSERT INTO {table_name} 
            (position, team_name, games_played, won, drawn, lost, goals_scored, goals_conceded, goal_difference, points) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            team_name = VALUES(team_name),
            games_played = VALUES(games_played),
            won = VALUES(won),
            drawn = VALUES(drawn),
            lost = VALUES(lost),
            goals_scored = VALUES(goals_scored),
            goals_conceded = VALUES(goals_conceded),
            goal_difference = VALUES(goal_difference),
            points = VALUES(points)
        '''
        
        # Insert data into the table
        cursor.executemany(query, data_to_insert)
        db.commit()

        print('Data inserted successfully')
    except Exception as e:
        print(f'Error: {e}')
        db.rollback()
        db.close()
        cursor.close() 