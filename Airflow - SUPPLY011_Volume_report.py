from airflow import DAG
import pendulum
from datetime import timedelta
from utils.storage import TempStorage
from utils.sharepoint import SharePoint
from utils.samba import SambaServer
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
tz_local = pendulum.timezone('Europe/Brussels')


default_args = {
    'owner': 'SUPPLY',
    'depends_on_past': False,
    'email': 'iven.vandertaelen@pfizer.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 1,
    'start_date': pendulum.datetime(2024, 1, 18, tz=tz_local),
    'on_failure_callback': TempStorage.clean_temps
}


class DagConfig(object):
    CONFIG = {
        "snowflake_conn_id": "snowflake_udh_prod",
        'email_subject': 'Airflow test',
        "Client_id": "f1cf8d33-556c-4269-b500-2a04efff6882",
        "Client_secret": "pvy/2k4AvJ88h7BqmZr1xov/1cQoHGpH98FhfBTAdGE=",
        "sharepoint_url": "https://pfizer.sharepoint.com/sites/PuursSupplyChain",
        "sharepoint_file" : "https://pfizer.sharepoint.com/sites/PuursSupplyChain/Weekly%20SAP%20Volumes/",
        "sharepoint_folder":"sites/PuursSupplyChain/Weekly%20SAP%20Volumes/",
        #"sharepoint_path": '/sites/PuursSupplyChain/Labeling%20Operations/',
        "sharepoint_filename": 'Volume_report',
        'EDCNAS_folder_prod': r'\\edcnas001\puu-app\Tableau_Sources\BusEx-Supply\Supply - SAP Orders History',
        'EDCNAS_filename_export': 'Volume_report ',
        'fp_EDCNAS_BudgetItems_file' : r'\\edcnas001\puu-app\Tableau_Sources\BusEx-Supply\Supply - SAP Orders History\BusEx - Suppy - Volume Budget - items.xlsx',
        'EDCNAS_BudgetItems_file_sheet' : 'Budget',
        'fp_EDCNAS_workcenter_allocation' : r'\\edcnas001\puu-app\Tableau_Sources\BusEx-Supply\Supply - SAP Orders History\BusEx - Workcenter_allocation.xlsx',
        'EDCNAS_workcenter_allocation_sheet' : 'Workcenter_allocation'
    }
    

def setup_temps(**context):
    dag_run_dir = TempStorage.create_temps(**context)
    return dag_run_dir
    
    
def _udh_query(sql: str, params=None):
    import pandas as pd
    hook = SnowflakeHook(DagConfig.CONFIG['snowflake_conn_id'])
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        df = pd.read_sql(sql=sql, con=conn, params=params)
    return df
    
        
def get_snowflake_process_planned_orders(**context):
    import importlib.resources as resources
    from datetime import datetime
    from utils.sql import SUPPLY011_SAP_Process_Planned_Orders
    with resources.open_text(SUPPLY011_SAP_Process_Planned_Orders, 'SAP Planned and process orders.sql') as file:
        sql_utils = file.read()
    df = _udh_query(sql=sql_utils)
    df.columns = [c.upper() for c in df.columns]
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)
    return fp_of_df


def get_master_data(**context):
    import importlib.resources as resources
    from utils.sql import SUPPLY011_SAP_Process_Planned_Orders
    with resources.open_text(SUPPLY011_SAP_Process_Planned_Orders, 'Material_master.sql') as file:
        sql_utils = file.read()
    df = _udh_query(sql=sql_utils)
    df.columns = [c.upper() for c in df.columns]
    #Add data upload label
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)
    return fp_of_df


def get_budget_file(**context):
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    SambaServer.pull_excel(
        conn_id='edcnas',
        adress=DagConfig.CONFIG['fp_EDCNAS_BudgetItems_file'],
        destination=fp_of_df,
        sheet_name=DagConfig.CONFIG['EDCNAS_BudgetItems_file_sheet']
    )
    return fp_of_df


def get_workcenter_allocation(**context):
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    SambaServer.pull_excel(
        conn_id='edcnas',
        adress=DagConfig.CONFIG['fp_EDCNAS_workcenter_allocation'],
        destination=fp_of_df,
        sheet_name=DagConfig.CONFIG['EDCNAS_workcenter_allocation_sheet']
    )
    return fp_of_df    


def process_concat_sources(**context):
    from datetime import datetime
    import pandas as pd
    #get order data
    df_orders = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_snowflake_process_planned_orders'))
    #get master data
    df_budget_items = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_budget_file'))
    df = pd.concat([df_orders, df_budget_items], axis=0).reset_index(drop=True)
    #Add data upload label
    df["EXTRACTION_DATE"] = datetime.now().strftime("%Y%m%d")
    fp_of_df = TempStorage.unique_name(tz_local,**context)
    df.to_pickle(fp_of_df)
    return fp_of_df


def add_master_data(**context):
    import pandas as pd
    df_volumes = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_concat_sources'))
    df_master = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_master_data'))    
    df = df_volumes.merge(df_master,
                                how='left',
                                left_on = 'MATERIAL_KEY',
                                right_on = 'MATERIAL_KEY')
    fp_of_df = TempStorage.unique_name(tz_local,**context)
    df.to_pickle(fp_of_df)
    return fp_of_df


def add_missing_mpg_desc(**context):
    #SOME MPG CODES do not have a MPG in UDH
    #create dataframe with mpg_desc and mpg_code. To have a list that contains a value for mpg_desc. From some items in UDH masterdata table this mpg_desc value is null
    import pandas as pd
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='add_master_data'))
    #create new mpg dataframe
    df_mpg = df[["MPG_CODE", "MPG_DESC"]].drop_duplicates()
    df_mpg = df_mpg.loc[df["MPG_DESC"].notna()]
    df_mpg.columns = [c + "_NEW" for c in df_mpg.columns]
    #merge with main dataframe and fill missing null values
    df_merge = df.merge(df_mpg,
                    how='left',
                    left_on='MPG_CODE',
                    right_on='MPG_CODE_NEW')
    df_merge.loc[df_merge["MPG_DESC"].isnull(),
       "MPG_DESC"] = df_merge["MPG_DESC_NEW"]
    df_merge.drop(columns=["MPG_CODE_NEW","MPG_DESC_NEW"], inplace=True)
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df_merge.to_pickle(fp_of_df)
    return fp_of_df


def add_workcenter_info(**context):
    #Add information about department, sub-department,... from general file instead of programming logic in code or sql
    import pandas as pd
    df_volumes = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='add_missing_mpg_desc'))
    df_workcenter = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_workcenter_allocation'))
    df = df_volumes.merge(df_workcenter,
                          how='left',
                          left_on = df_volumes['WORKCENTER'].str[:7],
                          right_on = 'WORKCENTER',
                          suffixes=['','_DROP']).filter(regex='^(?!.*_DROP)')
    fp_of_df = TempStorage.unique_name(tz_local,**context)
    df.to_pickle(fp_of_df)
    return fp_of_df


def process_missing_workcenter_info(**context):
    #workcenter information that is not in general file, to compose in excel to be send via mail
    import pandas as pd
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='add_workcenter_info'))
    df = df.loc[df['DEPARTMENT'].isnull]
    df = df[['WORKCENTER', 'WORKCENTER_NAME']].drop_duplicates()
    fp_of_xlsx = f'{TempStorage.unique_name(tz_local, **context)}.xlsx'
    df.to_excel(fp_of_xlsx,
                index=False,
                sheet_name='Data_missing')
    return fp_of_xlsx


def convert_to_excel(**context):
    import pandas as pd
    fp_of_df = context.get('ti').xcom_pull(
        task_ids='add_workcenter_info')
    df = pd.read_pickle(fp_of_df)
    fp_of_xlsx = f'{TempStorage.unique_name(tz_local, **context)}.xlsx'
    df.to_excel(
        fp_of_xlsx,
        index=False,
        sheet_name='Data')
    return fp_of_xlsx

    
def push_to_sharepoint(**context):
    fp_of_xlsx = context.get('ti').xcom_pull(task_ids="convert_to_excel")
    dt = pendulum.now().strftime("%Y%m%d")
    file_name = f"SAP Process and Planned Orders {dt}.xlsx"
    sp = SharePoint(
    client_id=DagConfig.CONFIG['Client_id'],
    client_secret=DagConfig.CONFIG['Client_secret'],
    site_url=DagConfig.CONFIG['sharepoint_url']
    )
    sp.upload(
        local_location=fp_of_xlsx,
        sharepoint_location=DagConfig.CONFIG['sharepoint_folder'],
        file_name=file_name
    )
    

def push_to_EDCNAS(**context):
    import os
    fp_of_xlsx = context.get('ti').xcom_pull(
        task_ids='convert_to_excel'
        )
    dt = pendulum.now().strftime("%Y%m%d")
    output_path = os.path.join(
        DagConfig.CONFIG['EDCNAS_folder_prod'],
        DagConfig.CONFIG['EDCNAS_filename_export'] + str(dt) + '.xlsx'
    )
    SambaServer.copy_file(
        conn_id='edcnas',
        destination=output_path,
        adress=fp_of_xlsx
    )
    
    
with DAG(
    dag_id='SUPPLY011_Volume_report.py',
    #schedule weekly on monday
    schedule_interval=None,#'0 7 * * MON',
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),
    tags=['iUDH', 'oSHAREPOINT','oEDCNAS', 'GMP_NON'],
    description= """
        This DAG will query UDH to load process and planned orders and push them to Supply Chain Sharepoint"""
) as dag:

    setup_temps = PythonOperator(
        task_id='setup_temps',
        python_callable=setup_temps
    )
    
    get_snowflake_process_planned_orders = PythonOperator(
        task_id='get_snowflake_process_planned_orders',
        python_callable=get_snowflake_process_planned_orders
    )

    get_master_data = PythonOperator(
        task_id='get_master_data',
        python_callable=get_master_data
    )

    get_budget_file = PythonOperator(
        task_id='get_budget_file',
        python_callable=get_budget_file
    )

    process_concat_sources = PythonOperator(
        task_id='process_concat_sources',
        python_callable=process_concat_sources
    )

    add_master_data = PythonOperator(
        task_id='add_master_data',
        python_callable=add_master_data
    )
    
    add_missing_mpg_desc = PythonOperator(
        task_id='add_missing_mpg_desc',
        python_callable=add_missing_mpg_desc
    )
    
    process_missing_workcenter_info = PythonOperator(
        task_id='process_missing_workcenter_info',
        python_callable=process_missing_workcenter_info
    )

    send_email_unmatched = EmailOperator(
        task_id='send_email_unmatched',
        to=["iven.vandertaelen@pfizer.com"],
        #cc=["iven.vandertaelen@pfizer.com"],
        subject='SUPPLY011 - Missing Work center info',
        html_content='In bijlage workcenter die niet in algemene file toegewezen worden aan een departement.',
        files=[
            "{{ task_instance.xcom_pull(task_ids='process_missing_workcenter_info') }}"]
    )
        
    convert_to_excel = PythonOperator(
        task_id='convert_to_excel',
        python_callable=convert_to_excel
    )
        
    # push_to_sharepoint = PythonOperator(
    #     task_id='push_to_sharepoint',
    #     python_callable=push_to_sharepoint
    # )
    
    push_to_EDCNAS = PythonOperator(
        task_id='push_to_EDCNAS',
        python_callable=push_to_EDCNAS,
        on_success_callback=TempStorage.clean_temps
    )
            
setup_temps >> [
    get_snowflake_process_planned_orders, 
    get_master_data,
    get_budget_file
] >> process_concat_sources >> add_master_data >> add_missing_mpg_desc  
add_missing_mpg_desc >> process_missing_workcenter_info >>send_email_unmatched >> convert_to_excel >> push_to_EDCNAS
