import airflow
import os
import sys

# Configure the root directory path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(parent_dir)

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.short_circuit_operator import ShortCircuitOperator

from src.extract.extraction import get_column_names, get_teams, get_data_frame, daily_check
from src.transform.transformation import transform_data
from src.load.loading import load_data
from data_quality_tests.data_quality_check import check_data_contract
from utils.database import db_connection, save_to_mysql


db, cursor = db_connection()
database_name = 'epl_2023/24'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Dag details
dag = DAG(
    dag_id = 'epl_table_scraper',
    default_args= default_args,
    description = 'EPL Table Scraper',
    start_date = airflow.utils.dates.days_ago(0),
    # schedule_interval = None 
    schedule_interval = '@daily' 
)

teams = None
def trigger_pipeline() -> bool:  
    teams = get_teams()
    games_played_db = daily_check(db, cursor)
    games_played_table = sum([int(team[2]) for team in teams])
    
    return games_played_db != games_played_table 

# Get functions from scripts directory
def run_function(function_name: str) -> None:
    transformed_data = None 
    
    if function_name == 'extraction':
        column_names = get_column_names()
        # teams = get_teams()
        extracted_data = get_data_frame(column_names, teams)
        save_to_mysql(db, cursor, extracted_data)
        
        print('\nExtracted data:')
        print(extracted_data)
        print('')    
    elif function_name == 'extracted_data_quality':
        contract = 'data_extraction_contract.yaml'
        table_name = 'premier_league_table'
        suite_name = 'premier_extraction_league_table_suite'
        success = check_data_contract(contract, database_name, table_name, suite_name)
        
        print(f"***Data Extraction contract validation {'passed' if success else 'failed'}***")
    elif function_name == 'transformation':
        transformed_data = transform_data(db, cursor)
        
        print('\nTransformed data:')
        print(transformed_data)
    elif function_name == 'loading':
        load_data(db, cursor, transformed_data)
    elif function_name == 'transformed_data_quality':
        contract = 'data_transformation_contract.yaml'
        table_name = 'transformed_premier_league_table'
        suite_name = 'premier_transformation_league_table_suite'
        success = check_data_contract(contract, database_name, table_name, suite_name)
        print(f"***Data Transformation contract validation {'passed' if success else 'failed'}***")
    else:
        raise ValueError(f"Function {function_name} not found")
    
# Dag tasks
# start_task = BashOperator(
#     task_id='start',
#     bash_command='echo "Pipeline started"',
#     dag=dag
# )   

# ShortCircuit operator to check if update is needed
check_update_needed = ShortCircuitOperator(
    task_id='check_update_needed',
    python_callable=trigger_pipeline,
    dag=dag,
)

data_extraction = PythonOperator(
    task_id='extraction',
    python_callable= run_function,
    op_args=['extraction'],
    dag=dag,
)

extracted_data_quality = PythonOperator(
    task_id='extracted_data_quality',
    python_callable=run_function,
    op_args=['extracted_data_quality'],
    dag=dag,
)

data_transformation = PythonOperator(
    task_id='transformation',
    python_callable=run_function,
    op_args=['transformation'],
    dag=dag,
)

data_loading = PythonOperator(
    task_id='loading',
    python_callable=run_function,
    op_args=['loading'],
    dag=dag,
    # on_failure_callback=failure_alert,
)

transformed_data_quality = PythonOperator(
    task_id='transformed_data_quality',
    python_callable=run_function,
    op_args=['transformed_data_quality'],
    dag=dag,
)

# end_task = BashOperator(
#     task_id='end',
#     bash_command='echo "Pipeline completed"',
#     dag=dag,
#     # on_success_callback=success_alert
# )  

# Data pipeline workflow
check_update_needed >> data_extraction >> extracted_data_quality >> data_transformation >> data_loading >> transformed_data_quality 
