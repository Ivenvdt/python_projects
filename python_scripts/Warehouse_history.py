from airflow import DAG, XComArg
from datetime import timedelta
import pendulum
import logging
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from utils.sharepoint import SharePoint
from utils.storage import TempStorage
from utils.samba import SambaServer
tz_local = pendulum.timezone('Europe/Brussels')


default_args = {
    'owner': 'OPEX',
    'depends_on_past': False,
    'email': 'Iven.vandertaelen@pfizer.com',  # 'DL-PUU_AIRFLOW_OPEX@pfizer.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'max_active_runs': 1,
    'start_date': pendulum.datetime(2022, 10, 24, tz=tz_local),
    'on_failure_callback': TempStorage.clean_temps
}


class DagConfig(object):
    CONFIG = {
        'database_folder': (
            'https://pfizer.sharepoint.com/:f:/r/sites/'
            'WarehouseCapacityPuurs/Shared%20Documents/'
            'IPL Archive'
        ),
        'processed_log_dir': (
            'https://pfizer.sharepoint.com/:f:/r/sites/'
            'WarehouseCapacityPuurs/Shared%20Documents/'
            'IPL Archive/execution_log'
        ),
        'processed_log_file': (
            'processed_log.xlsx'
        ),
        'export_folder_prod': (
            r'\\edcnas001\puu-app\Tableau_Sources\Productie Dashboards'
        ),
        'export_folder_dev': (
            r'\\edcnas001\puu-app\Tableau_Sources\Airflow DEV'
        ),
        'export_file_name': 'Productie Dashboard - Warehouse history',
        'fp_profit_center': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\BusEx - Supply - Profit Centers.xlsx'
        ),
        'fp_work_zones': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\BusEx - Supply - Work zones.xlsx'
        ),
        'snowflake_conn_id': 'snowflake_udh_prod',
        'site_url': (
            'https://pfizer.sharepoint.com/sites/WarehouseCapacityPuurs'
        ),
        'client_id': '028dd842-2584-4a1b-a257-9b7658254bde',
        'client_secret': 'aa7OJ8m+YtF7T8nimBqJ4AI08rA2goFugXtZWZGH7Z8=',
    }


def _udh_query(sql: str, params=None):
    import pandas as pd
    hook = SnowflakeHook(DagConfig.CONFIG['snowflake_conn_id'])
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        df = pd.read_sql(sql=sql, con=conn, params=params)
    return df


def setup_temps(**context):
    dag_run_dir = TempStorage.create_temps(**context)
    return dag_run_dir


def get_snowflake_data(**context):
    sql_snow = """
        SELECT
            MM.Material_key as "Material key",
            MM.Material_desc as "Item Description",
            MM.material_type_key "Material type",
            MP.purchasing_group "purchasing_group",
            MP.material_sourcing_flag "material_sourcing_flag",
            MP.MARKET_CODE "market_code",
            MP.MRP_CONTROLLER "MRP_CONTROLLER",
            MM.material_group,
        CASE WHEN MP.mpg_desc IS NULL THEN 'puurs' ELSE LOWER(MP.mpg_desc) END "Profit Center",
        CASE
            WHEN
            (
                MM.Material_desc like 'MF-%' OR
                MM.Material_desc like 'CLIN-%' OR
                MM.Material_desc like 'CLIN %'  OR
                MM.Material_desc like 'ET-%'  OR
                MM.Material_desc like 'ET %' OR
                MM.Material_desc like 'BUFFER %'
            ) THEN  'MF/CLIN/ET/BUFFER'
            WHEN MM.material_type_key = 'VERP' THEN 'VERP'
            WHEN MM.material_type_key = 'ZMPS' THEN 'ZMPS'
            WHEN MM.material_type_key = 'FERT' THEN 'Saleable Goods'
            WHEN MM.material_type_key = 'HALB' AND MP.material_sourcing_flag = '7' THEN 'Saleable Goods'
            WHEN MM.material_type_key = 'ZRAW' THEN 'Inact RM'
            WHEN MM.material_group IN (
                'DP_P055I','DP_P022D', 'DP_P055A', 'DP_P055G','DP_P055C',
                'DP_P055F', 'DP_P044C', 'DP_P044E', 'DP_P022A', 'DP_P022E',
                'DP_P055H', 'DP_P055E', 'XXXX'
                ) THEN 'PPM'
            WHEN MM.material_type_key = 'ZPAK' THEN 'NON PPM'
            WHEN MM.material_group = 'DP_P011F' THEN 'NON PPM'
            WHEN MM.base_unit_of_measure != 'EA' AND MM.base_unit_of_measure != 'VL' THEN 'API Finished'
            WHEN MP.plant_procurement_type IN ('F','E','X') THEN 'Semi Finished'
            ELSE 'NOT VALID'
        END  "ITEM TYPE"

        FROM
        PGSUDH_DL_PROD.PGSUDH_CDM.ecc_material_master MM
        INNER JOIN
        PGSUDH_DL_PROD.PGSUDH_CDM.ecc_material_plant MP
        ON
        MP.material_key = MM.material_key
        AND MP.plant_key = 'BE37'
    """
    df = _udh_query(sql=sql_snow)
    df.drop_duplicates(inplace=True)
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)
    return fp_of_df


def get_profit_centers(**context):
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    SambaServer.pull_excel(
        conn_id='edcnas',
        adress=DagConfig.CONFIG['fp_profit_center'],
        destination=fp_of_df,
        sheet_name='Profit_centers'
    )
    return fp_of_df


def get_metadata_db_folder(**context):
    sp = SharePoint(
        client_id=DagConfig.CONFIG['client_id'],
        client_secret=DagConfig.CONFIG['client_secret'],
        site_url=DagConfig.CONFIG['site_url']
    )
    folder = sp.get_folder(DagConfig.CONFIG['database_folder'])
    df_files = sp.get_files_in_folder(folder)
    current_year = pendulum.today().year
    df_monday = df_files[
        (df_files['TimeCreated'].dt.dayofweek == 0) &
        (df_files['TimeCreated'].dt.year == current_year)
    ]
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df_monday.to_pickle(fp_of_df)
    return fp_of_df


def get_processed_log(**context):
    from requests.exceptions import HTTPError
    import pandas as pd
    sp = SharePoint(
        client_id=DagConfig.CONFIG['client_id'],
        client_secret=DagConfig.CONFIG['client_secret'],
        site_url=DagConfig.CONFIG['site_url']
    )
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    try:
        sp.download(
            sharepoint_location=(
                DagConfig.CONFIG['processed_log_dir'] + '/' + DagConfig.CONFIG['processed_log_file']
                ),
            local_location=fp_of_df,
            as_dataframe=True
        )
        return fp_of_df
    except HTTPError as err:
        if err.response.status_code == 404:
            # no log file exists, we will create it from scratch.
            df = pd.DataFrame(columns=['ServerRelativeUrl', 'Processed', 'Exists'])
            df.to_pickle(fp_of_df)
            return fp_of_df
        else:
            raise


def get_output_file(**context):
    import os
    from smbprotocol.exceptions import SMBOSError
    import pandas as pd
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    current_year = pendulum.today().year
    output_path = os.path.join(
        DagConfig.CONFIG['export_folder_prod'],
        DagConfig.CONFIG['export_file_name'] + str(current_year) + '.xlsx'
    )
    try:
        fp_of_df = SambaServer.pull_excel(
            conn_id='edcnas',
            adress=output_path,
            destination=fp_of_df
        )
    except SMBOSError as err:  # TODO : catch exception is file does not yet exist?
        logging.warning(err.strerror)
        logging.warning("constructing df from scratch.")
        df = pd.DataFrame(columns=[
            'Reporting data',
            'Profit Center Group', 'Category owner',
            'LIC_NO'])
        fp_of_df = TempStorage.unique_name(tz_local, **context)
        df.to_pickle(fp_of_df)
    return fp_of_df


def determine_unprocessed(**context):
    import pandas as pd
    df_processed_log = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_processed_log'))
    df_monday_files = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_metadata_db_folder'))
    df_processed = pd.merge(
        left=df_monday_files,
        right=df_processed_log,
        left_on='ServerRelativeUrl',
        right_on='ServerRelativeUrl',
        indicator=True,
        how='left'
    )
    df_processed.loc[
        (df_processed['_merge'] == 'left_only'),
        'Processed'
        ] = False
    df_processed.loc[
        (df_processed['_merge'] != 'left_only'),
        'Processed'
        ] = True
    df_processed.loc[
        (df_processed['_merge'] == 'right_only'),
        'Exists'
        ] = False
    df_processed.loc[
        (df_processed['_merge'] != 'right_only'),
        'Exists'
        ] = True
    df_unprocessed = df_processed[
        (~df_processed['Processed']) &
        (df_processed['Exists'])
    ]
    files_to_process = df_unprocessed[['ServerRelativeUrl', 'Name']].to_dict('records')
    return files_to_process


def get_files_to_process(ServerRelativeUrl: str, Name: str, **context):
    import os
    sp = SharePoint(
        client_id=DagConfig.CONFIG['client_id'],
        client_secret=DagConfig.CONFIG['client_secret'],
        site_url=DagConfig.CONFIG['site_url']
    )
    filename = os.path.splitext(Name)[0]
    fp_of_df = f'{TempStorage.unique_name(tz_local, **context)}_{filename}'
    sp.download(
        sharepoint_location=ServerRelativeUrl,
        local_location=fp_of_df,
        url_is_absolute=False,
        as_dataframe=True
    )
    return {'fp_of_df': fp_of_df}


def process_files(fp_of_df: str, **context):
    import pandas as pd
    import numpy as np
    df = pd.read_pickle(fp_of_df)
    df_master_data = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_snowflake_data'))
    df_profit_center = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_profit_centers'))
    # ensure propper dtype
    df['Reporting data'] = pd.to_datetime(df["Reporting data"], dayfirst=True)
    df = df.merge(
        df_master_data,
        left_on='ITEM',
        right_on='MATERIAL KEY',
        how='left',
        suffixes=('', '_DROP')
    ).filter(regex='^(?!.*_DROP)')
    # merge profit centers
    df_profit_center['PROFIT_CENTER_DESC'] = df_profit_center['PROFIT_CENTER_DESC'].str.lower()
    df = df.merge(
        df_profit_center,
        left_on='PROFIT CENTER',
        right_on='PROFIT_CENTER_DESC',
        how='left',
        suffixes=('_DROP', '')
    ).filter(regex='^(?!.*_DROP)')
    # fill in blank values in column Profit Center Group
    df['Profit Center Group'] = df['Profit Center Group'].replace(
        "", np.NaN).fillna(df['PROFIT CENTER'])
    # add general warehouse description
    df['Warehouse'] = 'Other'
    # PLOG HB ambient
    df.loc[
        df['WORK_ZONE'].isin(
            [
                "WAIS32", "WAIS33",
                "WAIS34", "WAIS35", "WAIS36"
            ]),
        "Warehouse"
    ] = "PLOG HB AMB"
    # PLOG HB cold
    df.loc[
        df['WORK_ZONE'] == "WAIS31",
        "Warehouse"
    ] = "PLOG HB FRI"
    # CSP HB Ambient
    df.loc[
        df['WORK_ZONE'].isin(
            [
                "WAIS3", "WAIS4", "WAIS5",
                "WAIS6", "WAIS7", "WAIS8", "WAIS9",
                "WAIS10", "WAIS11", "WAIS12"
            ]),
        "Warehouse"
    ] = "CSP HB AMB"
    # CSP HB Cold
    df.loc[
        df['WORK_ZONE'].isin(
            ["WAIS1", "WAIS2", "WAIS13"]
        ),
        "Warehouse"
    ] = "CPS HB FRI"
    # ODTH AMB
    df.loc[
        (df['WORK_ZONE'].isin(["WODTH", "REJODTH"])) |
        (df["LOCATION"] == "!CODTH"),
        "Warehouse"
    ] = "ODTH AMB"
    # ODTH Cold
    df.loc[
        (df['WORK_ZONE'] == "WFRODTH") |
        (df["LOCATION"].isin(["!CFRODTH","!FRODTH"])),
        "Warehouse"
    ] = "ODTH FRI"
    # CSP ABRS
    df.loc[
        df["WORK_ZONE"] == "WCABR",
        "Warehouse"
    ] = "CSP ABRS"
    # Indaver plant
    df.loc[
        df["WORK_ZONE"] == "WINDA",
        "Warehouse"
    ] = "INDAVER PLANT"
    # PLOG AUTOSTORE
    df.loc[
        df["WORK_ZONE"] == "WPAS1",
        "Warehouse"
    ] = "PLOG AUTOSTORE"
    # VERZELE
    df.loc[
        df["LOCATION"] == "!VERZELE",
        "Warehouse"
    ] = "VERZELE"
    # Freezer 20 st
    df.loc[
        df["WORK_ZONE"] == "WIF2S",
        'Warehouse'
    ] = 'Freezer 20 st'
    # Freezer 40 Bag
    df.loc[
        (df['WORK_ZONE'] == 'WIF40'),
        'Warehouse'
    ] = 'Freezer 40 Bag'
    # Freezer 75
    df.loc[
        (df['WORK_ZONE'] == 'WIF75'),
        'Warehouse'
    ] = 'Freezer 75'
    # Fr 20 Karren Retour
    df.loc[
        (df['WORK_ZONE'] == 'WIF20') &
        (
            (df['LOCATION'].str.startswith('!214')) |
            (df['LOCATION'].str.startswith('!215'))
        ),
        'Warehouse'
    ] = 'Fr 20 Karren Retour'
    # Fr 20 Fles
    df.loc[
        (df['WORK_ZONE'] == 'WIF20') &
        (df['LOCATION'].str.startswith('!VR')),
        'Warehouse'
    ] = 'Fr 20 Fles'
    # Fr 20 Pal
    df.loc[
        (
            (df['WORK_ZONE'] == 'WIF20') &
            (df['LOCATION'].str.len() == 7) &
            (df['LOCATION'].str.startswith('!2')) &
            (df['LOCATION'].str.endswith('2'))
        ) |
        (
            (df['WORK_ZONE'] == 'WIF20') &
            (df['LOCATION'].str.len() == 8) &
            (df['LOCATION'].str.startswith('!2')) &
            (df['LOCATION'].str[6:7] == '2')
        ),
        'Warehouse'
    ] = 'Fr 20 Pal'
    # Fr 20 Tank
    df.loc[
        (
            (df['WORK_ZONE'] == 'WIF20') &
            (df['LOCATION'].str.len() == 7) &
            (df['LOCATION'].str.startswith('!2')) &
            (df['LOCATION'].str.endswith('1'))
        ) |
        (
            (df['WORK_ZONE'] == 'WIF20') &
            (df['LOCATION'].str.len() == 8) &
            (df['LOCATION'].str.startswith('!2')) &
            (df['LOCATION'].str[6:7] == '1')
        ),
        'Warehouse'
    ] = 'Fr 20 Tank'
    # Fr 20 container
    df.loc[
        (df['WORK_ZONE'] == 'WIF20C'),
        'Warehouse'
    ] = 'Fr 20 container'
    # --------add catergory owner--------
    df['Category owner'] = 'Other'
    # NONERP
    df.loc[
        (df['CATEGORY'] == 'NONERP'),
        'Category owner'
    ] = 'NONERP'
    # INTR
    df.loc[
        (df['CATEGORY'] == 'INTR'),
        'Category owner'
    ] = 'INTR'
    # HALB
    df.loc[
        (df['CATEGORY'] == 'HALB'),
        'Category owner'
    ] = 'HALB'
    # ZMPS
    df.loc[
        (df['CATEGORY'] == 'ZMPS'),
        'Category owner'
    ] = 'ZMPS'
    # FERT
    df.loc[
        (df['CATEGORY'] == 'FERT'),
        'Category owner'
    ] = 'FERT'
    # ZCPA
    df.loc[
        (df['CATEGORY'] == 'ZCPA'),
        'Category owner'
    ] = 'ZCPA'
    # EAMS ITEM
    df.loc[
        (df['DESCR'].str.startswith('(EAMS)')),
        'Category owner'
    ] = 'EAMS ITEM'
    # ZPAK-PLANT
    df.loc[
        (df['CATEGORY'] == "ZPAK") &
        (df['purchasing_group'].isin(
            [
                'BDB', 'BDD', 'BDE', 'BDI',
                'BDI', 'BDJ', 'BDH', 'BDK',
                'BDL', 'BDM'
            ])),
        'Category owner'
    ] = 'ZPAK-PLANT'
    # ZPAK-CSP-PPM
    df.loc[
        (df['CATEGORY'].isin(["ZPAK", "HALB"])) &
        (df['purchasing_group'].isin([
            'BDA', 'BDC', 'BDG', 'BDN', 'BDO'])),
        'Category owner'
    ] = 'ZPAK-CSP-PPM'
    # ZPAK-CSP-NPPM
    df.loc[
        (df['CATEGORY'] == "ZPAK") &
        (df['purchasing_group'].isin([
            'BDA', 'BDC', 'BDG', 'BDN', 'BDO'])) &
        (df['material_group'].isin([
            'DP_R6010C', 'DP_P033K', 'DP_P033N', 'DP_P022K', 'DP_P088L',
            'DP_P022J', 'DP_P022I', 'DP_P099A', 'DP_P033U', 'DP_P088E',
            'DP_P011C', 'DP_P077C', 'DP_P100A', 'DP_P088G', 'DP_P077F',
            'DP_P088I', 'DP_P088H', 'DP_P088I', 'DP_P066B', 'DP_P066A',
            'DP_P066D', 'DP_P066C', 'DP_P033W', 'DP_P033E', 'DP_P033R',
            'DP_P033X', 'DP_P033P', 'DP_P077H', 'DP_P033I', 'DP_P033F',
            'DP_R544F', 'DP_P100B', 'DP_P088D', 'DP_P099B', 'DP_P077I'])),
        'Category owner'
    ] = 'ZPAK-CSP-NPPM'
    #Labeling operation
    df.loc[
        (df['CATEGORY'] == "ZPAK") &
        (df['purchasing_group'].isin([
            'BDA', 'BDC', 'BDG', 'BDN', 'BDO'])) &
        (df['mrp_controller'] == "DSS"),
        'Category owner'
    ] = 'Labeling operation'    
    # ZRAW-PLANT
    df.loc[
        (
            (df['CATEGORY'] == "ZRAW") &
            (df['purchasing_group'] == "BDK")
        ),
        'Category owner'
    ] = 'ZRAW-PLANT'
    # ZRAW-SUPPLY
    df.loc[(
        (df['CATEGORY'] == "ZRAW") &
        (df['purchasing_group'] == "BDF")),
        'Category owner'
    ] = 'ZRAW-SUPPLY'
    # DONOR-SUPPLY
    df.loc[
        (
            ((df['CATEGORY'] == "FERT") | (df['CATEGORY'] == "HALB")) &
            (df['market_code'] == "15")
        ),
        'Category owner'
    ] = 'DONOR-SUPPLY'
    # SF4S
    df.loc[(
        (df['CATEGORY'] == "HALB") &
        (df['material_sourcing_flag'] == "7")
        ),
        'Category owner'
    ] = 'SF4S'
    # LEEGGOED
    df.loc[
        (df['DESCR'].str.contains('[LG]', regex=False)) |
        (df['CATEGORY'] == "EMPT"),
        'Category owner'
    ] = 'LEEGGOED'
    # LABO MATERIAAL
    df.loc[
        (df['ITEM'].str.contains('LABO', case=False)) |
        (df["DESCR"].str.contains("LABO", case=False)) |
        (df['ITEM'].str.contains('STALEN', case=False)) |
        (df["DESCR"].str.contains("STALEN", case=False)),
        'Category owner'
    ] = 'LABO MATERIAAL'
    # VALIDATIELOTEN
    df.loc[
        (
            (
                (df['CATEGORY'] == "HALB") |
                (df['CATEGORY'] == "INTR")
            ) &
            (
                (df['DESCR'].str.startswith("CLIN ")) |
                (df['DESCR'].str.startswith("ET-")) |
                (df['DESCR'].str.startswith("DEV")) |
                (df['DESCR'].str.startswith("PQ")) |
                (df['DESCR'].str.startswith("PV"))
            )
        ),
        'Category owner'
    ] = 'VALIDATIELOTEN'
    # TEST COMPONENT
    df.loc[
        (df['DESCR'].str.contains('TEST')),
        'Category owner'
    ] = 'TEST COMPONENT'
    # T-MATERIAAL
    df.loc[
        (df['ITEM'].str.startswith('TEST')),
        'Category owner'
    ] = 'T-MATERIAAL'
    # aggregation on Profit Center
    df_grouped = df.groupby([
        'Reporting data', 'Warehouse', 'Profit Center Group', 'Category owner'
    ], dropna=False)['LIC_NO'].nunique().reset_index()
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df_grouped.to_pickle(fp_of_df)
    return fp_of_df


def join_data(**context):
    import pandas as pd
    df_output = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_output_file'))
    fps_of_grouped_dfs = context.get('ti').xcom_pull(
        task_ids='process_files'
    )
    for fp in fps_of_grouped_dfs:
        df_grouped = pd.read_pickle(fp)
        df_output = pd.concat([df_output, df_grouped], axis=0)
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df_output.to_pickle(fp_of_df)
    return fp_of_df


def convert_to_excel(**context):
    import pandas as pd
    fp_of_df = context.get('ti').xcom_pull(
        task_ids='join_data')
    df_raw = pd.read_pickle(fp_of_df)
    fp_of_xlsx = f'{TempStorage.unique_name(tz_local, **context)}.xlsx'
    df_raw.to_excel(
        fp_of_xlsx,
        index=False,
        sheet_name='Data')
    return fp_of_xlsx


def push_files(**context):
    import os
    fp_of_xlsx = context.get('ti').xcom_pull(
        task_ids='convert_to_excel'
        )
    current_year = pendulum.today().year
    output_path = os.path.join(
        DagConfig.CONFIG['export_folder_prod'],
        DagConfig.CONFIG['export_file_name'] + str(current_year) + '.xlsx'
    )
    SambaServer.copy_file(
        conn_id='edcnas',
        destination=output_path,
        adress=fp_of_xlsx
    )


def update_log(**context):
    import pandas as pd
    df_processed_new = pd.DataFrame.from_records(
        (context.get('ti').xcom_pull(task_ids='determine_unprocessed')),
        exclude=['Name'])
    df_processed_new['Exists'] = True
    df_processed_new['Processed'] = True
    df_processed_log = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_processed_log'))
    new_log = pd.concat(
        [df_processed_log, df_processed_new], ignore_index=True)
    fp_of_xlsx = f'{TempStorage.unique_name(tz_local, **context)}.xlsx'
    new_log.to_excel(fp_of_xlsx, index=False)
    sp = SharePoint(
        client_id=DagConfig.CONFIG['client_id'],
        client_secret=DagConfig.CONFIG['client_secret'],
        site_url=DagConfig.CONFIG['site_url']
    )
    sp.upload(
        local_location=fp_of_xlsx,
        sharepoint_location=DagConfig.CONFIG['processed_log_dir'],
        file_name=DagConfig.CONFIG['processed_log_file']
    )


with DAG(
    dag_id='OPX057_warehouse_history',
    schedule_interval='0 6 * * MON',
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    on_success_callback=TempStorage.clean_temps,
    tags=['iUDH', 'oEDCNAS', 'oSHAREPOINT', 'GMP_NON'],
    description="""
        This DAG will query IPL and produce a snapshot which it stores on sharepoint
    """
) as dag:

    setup_temps = PythonOperator(
        task_id='setup_temps',
        python_callable=setup_temps
    )

    get_snowflake_data = PythonOperator(
        task_id='get_snowflake_data',
        python_callable=get_snowflake_data
    )

    get_profit_centers = PythonOperator(
        task_id='get_profit_centers',
        python_callable=get_profit_centers
    )

    get_metadata_db_folder = PythonOperator(
        task_id='get_metadata_db_folder',
        python_callable=get_metadata_db_folder
    )

    get_processed_log = PythonOperator(
        task_id='get_processed_log',
        python_callable=get_processed_log
    )

    get_output_file = PythonOperator(
        task_id='get_output_file',
        python_callable=get_output_file
    )

    determine_unprocessed = PythonOperator(
        task_id='determine_unprocessed',
        python_callable=determine_unprocessed
    )

    get_files_to_process = PythonOperator.partial(
        task_id='get_files_to_process',
        python_callable=get_files_to_process
    ).expand(op_kwargs=XComArg(determine_unprocessed))

    process_files = PythonOperator.partial(
        task_id='process_files',
        python_callable=process_files
    ).expand(op_kwargs=XComArg(get_files_to_process))

    join_data = PythonOperator(
        task_id='join_data',
        python_callable=join_data
    )

    convert_to_excel = PythonOperator(
        task_id='convert_to_excel',
        python_callable=convert_to_excel
    )

    push_files = PythonOperator(
        task_id='push_files',
        python_callable=push_files
    )

    update_log = PythonOperator(
        task_id='update_log',
        python_callable=update_log,
        on_success_callback=TempStorage.clean_temps
    )

setup_temps >> [
    get_snowflake_data,
    get_profit_centers,
    get_processed_log,
    get_metadata_db_folder,
    get_output_file
] >> determine_unprocessed >> get_files_to_process >> process_files
process_files >> join_data >> convert_to_excel >> push_files >> update_log
