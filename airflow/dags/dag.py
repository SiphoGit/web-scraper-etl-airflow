import os
import sys
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import timedelta


# Configure the root directory path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(parent_dir)

from src.extract.extraction import get_column_names, get_teams, get_data_frame, daily_check
from src.transform.transformation import transform_data
from src.load.loading import load_data
from data_quality_tests.data_quality_check import check_data_contract
from utils.database import db_connection, save_to_mysql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='epl_table_scraper',
    default_args=default_args,
    description='EPL Table Scraper',
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval='@daily'
)

# Check if scraping is needed.
def trigger_pipeline(**context: object) -> bool:
    db, cursor = db_connection()
    
    try:
        teams = get_teams()
        games_played_db = daily_check(db, cursor)
        games_played_table = sum([int(team[2]) for team in teams])
        
        print(f'Database total number of games played: {games_played_db}')
        print(f'Source total number of games played: {games_played_table}')
        
        update_needed = games_played_db != games_played_table
        
        if not update_needed:
            print('Table up to date. Not updating needed!')
            return False
        
        context['ti'].xcom_push(key='teams', value=teams)
        
        return update_needed
    finally:
        cursor.close()
        db.close()
        

# Call functions from other folders in the root directory
def run_function(function_name: str, **context: object) -> None:
    db, cursor = db_connection()
    
    try:
        if function_name == 'extraction':
            column_names = get_column_names()
            teams = context['ti'].xcom_pull(task_ids='check_update_needed', key='teams')
            extracted_data = get_data_frame(column_names, teams)
            save_to_mysql(db, cursor, extracted_data)
            
            print('\nExtracted data:')
            print(extracted_data)
        elif function_name == 'extracted_data_quality':
            contract = 'data_extraction_contract.yaml'
            table_name = 'premier_league_table'
            suite_name = 'premier_extraction_league_table_suite'
            
            success = check_data_contract(contract, 'epl_2023/24', table_name, suite_name)
            
            print(f"***Data Extraction contract validation {'passed' if success else 'failed'}***")
        elif function_name == 'transformation':
            transformed_data = transform_data(db, cursor)
            
            print('\nTransformed data:')
            print(transformed_data)
            
            context['ti'].xcom_push(key='transformed_data', value=transformed_data)
        elif function_name == 'loading':
            transformed_data = context['ti'].xcom_pull(task_ids='transformation', key='transformed_data')
            load_data(db, cursor, transformed_data)
        elif function_name == 'transformed_data_quality':
            contract = 'data_transformation_contract.yaml'
            table_name = 'transformed_premier_league_table'
            suite_name = 'premier_transformation_league_table_suite'
            
            success = check_data_contract(contract, 'epl_2023/24', table_name, suite_name)
            
            print(f'***Data Transformation contract validation {'passed' if success else 'failed'}***')
        else:
            raise ValueError(f'Function {function_name} not found')
    finally:
        cursor.close()
        db.close()

# Dag tasks
check_update_needed = ShortCircuitOperator(
    task_id='check_update_needed',
    python_callable=trigger_pipeline,
    provide_context=True,
    dag=dag,
)

data_extraction = PythonOperator(
    task_id='extraction',
    python_callable=run_function,
    op_kwargs={'function_name': 'extraction'},
    provide_context=True,
    dag=dag,
)

extracted_data_quality = PythonOperator(
    task_id='extracted_data_quality',
    python_callable=run_function,
    op_kwargs={'function_name': 'extracted_data_quality'},
    provide_context=True,
    dag=dag,
)

data_transformation = PythonOperator(
    task_id='transformation',
    python_callable=run_function,
    op_kwargs={'function_name': 'transformation'},
    provide_context=True,
    dag=dag,
)

data_loading = PythonOperator(
    task_id='loading',
    python_callable=run_function,
    op_kwargs={'function_name': 'loading'},
    provide_context=True,
    dag=dag,
)

transformed_data_quality = PythonOperator(
    task_id='transformed_data_quality',
    python_callable=run_function,
    op_kwargs={'function_name': 'transformed_data_quality'},
    provide_context=True,
    dag=dag,
)

# Data pipeline workflow
check_update_needed >> data_extraction >> extracted_data_quality >> data_transformation >> data_loading >> transformed_data_quality