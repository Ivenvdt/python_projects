from airflow import DAG, XComArg
from datetime import timedelta
import pendulum
import logging
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from utils.sharepoint import SharePoint
from utils.storage import TempStorage
from utils.samba import SambaServer
tz_local = pendulum.timezone('Europe/Brussels')


default_args = {
    'owner': 'SUPPLY',
    'depends_on_past': False,
    'email': 'iven.vandertaelen@pfizer.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 1,
    'start_date': pendulum.datetime(2024, 5, 22, tz=tz_local),
    'on_failure_callback': TempStorage.clean_temps
}

class DagConfig(object):
    CONFIG = {
        'export_folder_prod': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply'
        ),
        'fp_output1': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\Inv_item_projection.txt'
        ),
        'fp_output2': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\Inv_item_clustering_projection.txt'
        ),
        'fp_output3': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\Inv_aggregation_projection.txt'
        ),
        'fp_profit_center': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\BusEx - Supply - Profit Centers.xlsx'
        ),
        'fp_pallet_information': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\BusEx - Supply - Max per pallet.xlsx'
        ),
        'fp_ZCON': (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\BusEx - Supply - ZCON_Cost.xlsx'
        ),
        'fp_Item_clustering' : (
            r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\BusEx - Supply - Hcode grouping data driven SS.xlsm'
        ),
        'snowflake_conn_id': 'snowflake_udh_prod',
        'weeks_forward_MOH': 35,
        'weeks_forward_projection': 78
    }

def get_year_week_current_date():
    #get todays week
    import datetime
    today = datetime.datetime.now()
    iso_year, iso_week_number, _ = today.isocalendar()
    year_week = f"{iso_year}{iso_week_number:02d}"
    return int(year_week)

def get_year_week_forward_looking(weeks_to_look_forward):
    #get todays week + 35 weeks forward
    import datetime
    today = datetime.datetime.now()
    today_weeks_to_look_forward = today + datetime.timedelta(weeks=weeks_to_look_forward)
    today_weeks_to_look_forward.isocalendar()
    iso_year, iso_week_number, _ = today_weeks_to_look_forward.isocalendar()
    year_week = f"{iso_year}{iso_week_number:02d}"
    return int(year_week)    

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

def get_snowflake_data_inventory(**context):
    sql_snow_inventory = """
    WITH 
    MRP_BOOK_STOCK_H_F AS(
    SELECT DISTINCT
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    SUM("TOTAL_STOCK") "TOTAL_STOCK",
    SUM("TOTAL_DEMAND") "TOTAL_DEMAND",
    SUM("TOTAL_RECEIPTS") "TOTAL_RECEIPTS",
    SUM("TOTAL_QUANTITY") "TOTAL_QUANTITY",
    "UOM"
    FROM(
        SELECT
        MB.material_key "MATERIAL_KEY",
        MB.mrp_element_ind, 
        --SETTING THE DATE FOR STOCK (WB) AND QM STOCK (QM) EQUAL TO CURRENT DATE
        CURRENT_DATE "Date",
        CASE WHEN LENGTH (CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date"))) = 6 
        THEN CAST(CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date")) AS INTEGER)
        ELSE CAST(CONCAT(YEAROFWEEKISO("Date"),0,WEEKISO("Date")) AS INTEGER) 
        END AS "CALENDAR_YEAR_WEEK",
        MB.MRP_actual_req_qty  "TOTAL_STOCK",
        TO_NUMBER(0) "TOTAL_DEMAND",
        TO_NUMBER(0) "TOTAL_RECEIPTS",
        MB.MRP_actual_req_qty AS "TOTAL_QUANTITY",
        MB.BUOM AS "UOM"
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.ecc_mrp_book MB
        WHERE
        MB.plant_key in ('BE37','BE94', 'BE95')
        --WB= STOCK, QM= Quality batches
        AND MB.mrp_element_ind IN ('QM','WB')
        AND (MB.MATERIAL_KEY LIKE 'H%' OR MB.MATERIAL_KEY LIKE 'F%')
        HAVING YEAROFWEEKISO("Date") < YEAROFWEEKISO(CURRENT_DATE())+2
        ORDER BY 
        MATERIAL_KEY,
        "Date")
    GROUP BY 
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    "UOM"
    ORDER BY 
    "CALENDAR_YEAR_WEEK" asc)
    ,
    MRP_BOOK_API_PO AS(
    SELECT DISTINCT
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    SUM("TOTAL_STOCK") "TOTAL_STOCK",
    SUM("TOTAL_DEMAND") "TOTAL_DEMAND",
    SUM("TOTAL_RECEIPTS") "TOTAL_RECEIPTS",
    SUM("TOTAL_QUANTITY") "TOTAL_QUANTITY",
    "UOM"
    FROM(
        SELECT
        MB.material_key "MATERIAL_KEY",
        MB.mrp_element_ind, 
        CASE WHEN MB.RESCH_DATE IS NOT NULL THEN MB.RESCH_DATE 
        ELSE MB.Order_finish_date END as "Date",	 
        CASE WHEN LENGTH (CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date"))) = 6 
        THEN CAST(CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date")) AS INTEGER)
        ELSE CAST(CONCAT(YEAROFWEEKISO("Date"),0,WEEKISO("Date")) AS INTEGER) 
        END AS "CALENDAR_YEAR_WEEK",
        TO_NUMBER(0) "TOTAL_STOCK",
        TO_NUMBER(0) "TOTAL_DEMAND",
        CASE WHEN MB.MRP_actual_req_qty >= 0 THEN MRP_actual_req_qty ELSE 0 END "TOTAL_RECEIPTS",
        MB.MRP_actual_req_qty AS "TOTAL_QUANTITY",
        MB.BUOM AS "UOM"
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.ecc_mrp_book MB
        WHERE
        MB.plant_key = 'BE37'
        --LA= ShipNt/OrdCnf, BA=PurRqs, BE= Purchase orders 
        AND MB.mrp_element_ind IN ('LA','BE','BA')
        AND (MB.MATERIAL_KEY LIKE 'H%' OR MB.MATERIAL_KEY LIKE 'F%')
        HAVING YEAROFWEEKISO("Date") < YEAROFWEEKISO(CURRENT_DATE())+2
        ORDER BY 
        MATERIAL_KEY,
        "Date")
    GROUP BY 
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    "UOM"
    ORDER BY 
    "CALENDAR_YEAR_WEEK" asc)
    ,
    MRP_BOOK_NPPM_PPM AS(
    SELECT DISTINCT
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    SUM("TOTAL_STOCK") "TOTAL_STOCK",
    SUM("TOTAL_DEMAND") "TOTAL_DEMAND",
    SUM("TOTAL_RECEIPTS") "TOTAL_RECEIPTS",
    SUM("TOTAL_QUANTITY") "TOTAL_QUANTITY",
    "UOM"
    FROM(
        SELECT
        MB.material_key "MATERIAL_KEY",
        MB.mrp_element_ind, 
        CASE 
        --SETTING THE DATE FOR STOCK (WB) AND QM STOCK (QM) EQUAL TO CURRENT DATE
        WHEN MB.mrp_element_ind = 'WB' OR MB.mrp_element_ind = 'QM' THEN CURRENT_DATE
        --SOME PLANNED ORDERS EXIST IN MRP BOOK AND NOT IN PLANNED ORDER TABLE
        WHEN MB.mrp_element_ind = 'AR' AND PO.start_date_process_order IS NULL THEN MB.Order_finish_date
        --SETTING DATE OF PROCESS ORDERS (AR) TO START DATE OF PROCESS ORDERS
        WHEN MB.mrp_element_ind = 'AR' THEN PO.start_date_process_order 
        --SOME PROCESS ORDERS EXIST IN MRP BOOK AND NOT IN PROCESS ORDER TABLE
        WHEN MB.mrp_element_ind = 'SB' AND PL.start_date_planned_order IS NULL THEN MB.Order_finish_date
        --SETTING DATE OF PLANNED ORDERS (AR) TO START DATE OF PLANNED ORDERS
        WHEN MB.mrp_element_ind = 'SB' THEN PL.start_date_planned_order
        WHEN MB.RESCH_DATE IS NOT NULL THEN MB.RESCH_DATE 
        ELSE MB.Order_finish_date END as "Date",
        CASE WHEN LENGTH (CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date"))) = 6 THEN CAST(CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date")) AS INTEGER)
        ELSE CAST(CONCAT(YEAROFWEEKISO("Date"),0,WEEKISO("Date")) AS INTEGER) END AS "CALENDAR_YEAR_WEEK",
        CASE WHEN MB.mrp_element_ind IN ('WB', 'QM') THEN MRP_actual_req_qty ELSE 0 END as "TOTAL_STOCK",
        CASE WHEN MB.MRP_actual_req_qty < 0 THEN MRP_actual_req_qty ELSE 0 END AS "TOTAL_DEMAND",
        CASE WHEN (MB.MRP_actual_req_qty >= 0 AND MB.mrp_element_ind NOT IN ('WB','QM'))  THEN MRP_actual_req_qty ELSE 0 END AS "TOTAL_RECEIPTS",
        MB.MRP_actual_req_qty AS "TOTAL_QUANTITY",
        MB.BUOM AS "UOM"
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.ecc_mrp_book MB
        LEFT JOIN 
        (SELECT DISTINCT
        process_order.PROCESS_ORDER, 
        CASE WHEN process_order.actual_start_date IS NOT NULL THEN process_order.actual_start_date ELSE process_order.scheduled_release_date END AS start_date_process_order
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.ecc_process_order process_order
        WHERE 
        process_order.plant_key ='BE37'
        )PO
        ON MB.manufacturing_order = PO.PROCESS_ORDER
        LEFT JOIN 
        (SELECT DISTINCT
        planned_orders.planned_order, 
        planned_orders.scheduled_start_date as start_date_planned_order
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.ecc_planned_orders planned_orders
        WHERE 
        planned_orders.plant_key ='BE37'
        )PL
        ON MB.source_number = PL.planned_order 
        WHERE
        MB.plant_key in ('BE37','BE94', 'BE95')
        AND (MB.MATERIAL_KEY LIKE 'P%' OR MB.MATERIAL_KEY LIKE 'R%')
        HAVING YEAROFWEEKISO("Date") < YEAROFWEEKISO(CURRENT_DATE())+2
        ORDER BY 
        MATERIAL_KEY,
        "Date")
    GROUP BY 
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    "UOM"
    ORDER BY 
    "CALENDAR_YEAR_WEEK" asc)
    ,
    APO_RECEIPTS_VIEW_LOC_KEY_BE37 AS (
    SELECT DISTINCT
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    SUM("TOTAL_STOCK") "TOTAL_STOCK",
    SUM("TOTAL_DEMAND") "TOTAL_DEMAND",
    SUM("TOTAL_RECEIPTS") "TOTAL_RECEIPTS",
    SUM("TOTAL_QUANTITY") "TOTAL_QUANTITY",
    "UOM"
    FROM(
        SELECT 
        APO_RECEIPTS_VIEW.MATERIAL_KEY "MATERIAL_KEY",
        APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE,
        CASE WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('BI','EG','EI') AND APO_RECEIPTS_VIEW.START_DATE < CURRENT_DATE() THEN CURRENT_DATE()
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('BI','EG','EI') THEN APO_RECEIPTS_VIEW.START_DATE
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('AU','AV','AY') AND APO_RECEIPTS_VIEW.START_DATE < CURRENT_DATE() THEN APO_RECEIPTS_VIEW.AVAILABLE_WO_GR_DATETIME
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('AU','AV','AY') THEN APO_RECEIPTS_VIEW.START_DATE
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('AA','AB','AI','AJ') AND APO_RECEIPTS_VIEW.AVAILABLE_WO_GR_DATETIME IS NULL AND APO_RECEIPTS_VIEW.START_DATE < CURRENT_DATE() THEN CURRENT_DATE()
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('AA','AB','AI','AJ') AND APO_RECEIPTS_VIEW.AVAILABLE_WO_GR_DATETIME IS NULL THEN APO_RECEIPTS_VIEW.START_DATE 
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('AA','AB','AI','AJ') AND APO_RECEIPTS_VIEW.AVAILABLE_WO_GR_DATETIME < CURRENT_DATE() THEN CURRENT_DATE() 
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('AA','AB','AI','AJ') THEN APO_RECEIPTS_VIEW.AVAILABLE_WO_GR_DATETIME
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('EQ') THEN APO_RECEIPTS_VIEW.END_DATE
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('EE','EL','ZT') THEN APO_RECEIPTS_VIEW.START_DATE 
        ELSE APO_RECEIPTS_VIEW.START_DATE END "Date",
        CASE WHEN LENGTH (CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date"))) = 6 THEN CAST(CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date")) AS INTEGER)
        ELSE CAST(CONCAT(YEAROFWEEKISO("Date"),0,WEEKISO("Date")) AS INTEGER) END AS "CALENDAR_YEAR_WEEK",
        TO_NUMBER(0) "TOTAL_STOCK",
        CASE WHEN SCHED_RECEIPTS_TYPE IN ('AU', 'AY', 'EK', 'AV','ZT','EQ', 'EG', 'AW','BI','EL')
        --CASE WHEN SCHED_RECEIPTS_TYPE IN ('AU', 'AY', 'EK', 'AG','EF', 'EL', 'AV', 'BF','ZT','EQ','EA','BI')
        THEN -APO_RECEIPTS_VIEW.QUANTITY ELSE 0 END "TOTAL_DEMAND",
        CASE WHEN SCHED_RECEIPTS_TYPE IN ('AA', 'AJ', 'EI','AI', 'EE','AB','ZS')
        --CASE WHEN SCHED_RECEIPTS_TYPE IN ('AA', 'AJ', 'EI', 'EB','EG', 'AI', 'EH', 'EE','AB','BH')
        THEN APO_RECEIPTS_VIEW.QUANTITY ELSE 0 END "TOTAL_RECEIPTS",
        CASE WHEN "TOTAL_DEMAND" = 0 THEN "TOTAL_RECEIPTS" ELSE "TOTAL_DEMAND" END "TOTAL_QUANTITY",
        APO_RECEIPTS_VIEW.UNIT_OF_MEASURE "UOM"
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.APO_RECEIPTS_VIEW APO_RECEIPTS_VIEW
        WHERE 
        LOCATION_KEY = 'BE37'
        AND (APO_RECEIPTS_VIEW.MATERIAL_KEY LIKE 'F%' OR APO_RECEIPTS_VIEW.MATERIAL_KEY LIKE 'H%')
        AND to_number(CALENDAR_YEAR) < YEAR(CURRENT_DATE())+2
        ORDER BY 
        APO_MATERIAL_LOCATION_KEY,
        CALENDAR_YEAR_WEEK ASC)
    GROUP BY 
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    "UOM"
    ORDER BY 
    "CALENDAR_YEAR_WEEK" ASC)
    ,
    APO_RECEIPTS_VIEW_SOURCE_KEY_BE37 AS (
    SELECT DISTINCT
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    SUM("TOTAL_STOCK") "TOTAL_STOCK",
    SUM("TOTAL_DEMAND") "TOTAL_DEMAND",
    SUM("TOTAL_RECEIPTS") "TOTAL_RECEIPTS",
    SUM("TOTAL_QUANTITY") "TOTAL_QUANTITY",
    "UOM"
    FROM(
        SELECT 
        APO_RECEIPTS_VIEW.MATERIAL_KEY "MATERIAL_KEY",
        APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE,
        CASE WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('BC','BH','EB','ED','EF','EH','EA', 'EE', 'AI', 'SR','ZN') AND APO_RECEIPTS_VIEW.START_DATE < CURRENT_DATE() THEN CURRENT_DATE()
        WHEN APO_RECEIPTS_VIEW.SCHED_RECEIPTS_TYPE IN ('BC','BH','EB','ED','EF','EH','EA', 'EE', 'AI', 'SR','ZN') THEN APO_RECEIPTS_VIEW.START_DATE
        ELSE APO_RECEIPTS_VIEW.START_DATE END "Date",
        CASE WHEN LENGTH (CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date"))) = 6 THEN CAST(CONCAT(YEAROFWEEKISO("Date"),WEEKISO("Date")) AS INTEGER)
        ELSE CAST(CONCAT(YEAROFWEEKISO("Date"),0,WEEKISO("Date")) AS INTEGER) END AS "CALENDAR_YEAR_WEEK",
        TO_NUMBER(0) "TOTAL_STOCK",
        CASE WHEN SCHED_RECEIPTS_TYPE IN ('ED','EB','EH','BC','BH') --'AY'
        THEN -APO_RECEIPTS_VIEW.QUANTITY ELSE 0 END "TOTAL_DEMAND",
        CASE WHEN SCHED_RECEIPTS_TYPE IN ('EF','EA','ET')
        THEN APO_RECEIPTS_VIEW.QUANTITY ELSE 0 END "TOTAL_RECEIPTS",
        CASE WHEN "TOTAL_DEMAND" = 0 THEN "TOTAL_RECEIPTS" ELSE "TOTAL_DEMAND" END "TOTAL_QUANTITY",
        APO_RECEIPTS_VIEW.UNIT_OF_MEASURE "UOM"
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.APO_RECEIPTS_VIEW APO_RECEIPTS_VIEW
        WHERE 
        SOURCE_LOCATION = 'BE37'
        AND (LOCATION_KEY != 'BE37' OR LOCATION_KEY IS NULL)
        AND (APO_RECEIPTS_VIEW.MATERIAL_KEY LIKE 'F%' OR APO_RECEIPTS_VIEW.MATERIAL_KEY LIKE 'H%')
        AND to_number(CALENDAR_YEAR) < YEAR(CURRENT_DATE())+2
        ORDER BY 
        APO_MATERIAL_LOCATION_KEY,
        CALENDAR_YEAR_WEEK ASC)
    GROUP BY 
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    "UOM"
    ORDER BY 
    "CALENDAR_YEAR_WEEK" ASC)
    SELECT 
    "MATERIAL_KEY",
    "CALENDAR_YEAR_WEEK",
    "TOTAL_STOCK",
    "TOTAL_DEMAND",
    "TOTAL_RECEIPTS",
    "TOTAL_QUANTITY"
    --SUM(TOTAL_QUANTITY) OVER (PARTITION BY "MATERIAL_KEY" ORDER BY "CALENDAR_YEAR_WEEK") "PROJECTED_INVENTORY"
    FROM
        (SELECT
        "MATERIAL_KEY",
        "CALENDAR_YEAR_WEEK",
        SUM(TOTAL_STOCK) "TOTAL_STOCK",
        SUM(TOTAL_DEMAND) "TOTAL_DEMAND",
        SUM(TOTAL_RECEIPTS) "TOTAL_RECEIPTS",
        SUM(TOTAL_QUANTITY) "TOTAL_QUANTITY"
        FROM(
            SELECT *
            FROM MRP_BOOK_STOCK_H_F
            UNION ALL
            SELECT *
            FROM MRP_BOOK_API_PO
            UNION ALL
            SELECT *
            FROM MRP_BOOK_NPPM_PPM
            UNION ALL
            SELECT*
            FROM APO_RECEIPTS_VIEW_LOC_KEY_BE37
            UNION ALL
            SELECT*
            FROM APO_RECEIPTS_VIEW_SOURCE_KEY_BE37
            )
        GROUP BY
        "MATERIAL_KEY",
        "CALENDAR_YEAR_WEEK")
    ORDER BY
    MATERIAL_KEY,
    CALENDAR_YEAR_WEEK
    ;
    """
    df = _udh_query(sql=sql_snow_inventory)
    df.drop_duplicates(inplace=True)
    df.columns = [column.upper() for column in df.columns]  
    df.fillna({"CALENDAR_YEAR_WEEK": 299999}, inplace=True)
    dict_format = {"CALENDAR_YEAR_WEEK":int,
                   "TOTAL_STOCK" : float,
                   "TOTAL_DEMAND" : float,
                   "TOTAL_RECEIPTS" : float,
                   "TOTAL_QUANTITY" : float
                    }
    df = df.astype(dict_format)
    #filter inventory projection to only show as of this week + 106 weeks forward
    df = df.loc[(df["CALENDAR_YEAR_WEEK"]>=get_year_week_current_date()) & 
                (df["CALENDAR_YEAR_WEEK"]<get_year_week_forward_looking(int(DagConfig.CONFIG['weeks_forward_projection'])))]
    #Filter out material keys that do not have movement (demand/receipts) in the filtered time period 
    df_item_sum = df.groupby("MATERIAL_KEY").agg({"TOTAL_STOCK":"sum",
                                                  "TOTAL_DEMAND":"sum",
                                                  "TOTAL_RECEIPTS":"sum"}).reset_index()
    df_item_zero = df_item_sum.loc[(df_item_sum["TOTAL_STOCK"]==0)&
                                   (df_item_sum["TOTAL_DEMAND"]==0)&
                                   (df_item_sum["TOTAL_RECEIPTS"]==0)]
    items_Zero = df_item_zero["MATERIAL_KEY"].unique().tolist()
    df = df.loc[~df["MATERIAL_KEY"].isin(items_Zero)]
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)  
    return fp_of_df

def create_year_week_combination(**context):
    # Create dataframe that combines all possible hits with Calendar year week combination (with item level, profit center group level and item type level)
    import pandas as pd
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_snowflake_data_inventory'))
    df = df.loc[(df["CALENDAR_YEAR_WEEK"]>=get_year_week_current_date()) &
                                                          (df["CALENDAR_YEAR_WEEK"]<get_year_week_forward_looking(int(DagConfig.CONFIG['weeks_forward_projection'])))]
    df_items = pd.DataFrame(df["MATERIAL_KEY"].unique(), columns=["MATERIAL_KEY"])    
    df_calendar_year_week = pd.DataFrame(df["CALENDAR_YEAR_WEEK"].unique(), columns=["CALENDAR_YEAR_WEEK"])
    df_calendar_year_week["CALENDAR_YEAR_WEEK"] = df_calendar_year_week["CALENDAR_YEAR_WEEK"].astype(int)
    # create fictive column to merge
    df_items["Key"] = 0
    df_calendar_year_week["Key"] = 0
    df = pd.merge(df_items, df_calendar_year_week, on='Key', how='outer')
    df.drop(columns=["Key"],inplace = True)
    df.sort_values(by=["MATERIAL_KEY","CALENDAR_YEAR_WEEK"], ascending=True, inplace=True)
    df = df.reset_index(drop=True)
    df.drop_duplicates(inplace=True)
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)
    return fp_of_df

def get_snowflake_data_master_data(**context):
    sql_snow_master_data = """
    SELECT
    MM.Material_key,
    MM.Material_desc "ITEM_DESCRIPTION_MATERIAL_MASTER",
    MM.material_type_key "MATERIAL_TYPE",
    MP.PURCHASING_GROUP "PURCHASING_GROUP",
    MP.material_sourcing_flag "Material_sourcing_flag",
    MP.MARKET_CODE "Market_code",
    MM.material_group,
    MP.GR_PROC_TIME_DAYS,
    MP.plant_procurement_type,
    MP.MRP_TYPE,
    CASE WHEN MP.mpg_desc IS NULL THEN 'PUURS' ELSE UPPER(MP.mpg_desc) END "PROFIT_CENTER",
    CASE WHEN MP.mpg_desc IS NULL THEN '2479110' ELSE MP.FDM_MPG END "MPG_CODE",
    CASE   
    WHEN 
    (MM.Material_desc like 'MF-%' or MM.Material_desc like 'CLIN-%' or MM.Material_desc like 'CLIN %'  or 
    MM.Material_desc like 'ET-%'  or MM.Material_desc like 'ET %' or MM.Material_desc like 'BUFFER %') THEN  'MF/CLIN/ET/BUFFER' 
    WHEN MM.material_type_key = 'VERP' THEN 'VERP'
    WHEN MM.material_type_key = 'ZMPS' THEN 'ZMPS'
    WHEN MM.material_type_key = 'FERT' THEN 'Saleable Goods'
    WHEN MM.material_type_key = 'HALB' AND MP.material_sourcing_flag = '7' THEN 'SF4S'
    WHEN MM.material_type_key = 'ZRAW' THEN 'Inact RM'
    WHEN MM.material_group IN ('DP_P055I','DP_P022D', 'DP_P055A', 'DP_P055G','DP_P055C', 'DP_P055F', 
    'DP_P044C', 'DP_P044E', 'DP_P022A', 'DP_P022E', 'DP_P055H', 'DP_P055E', 'XXXX') THEN 'PPM'
    WHEN MM.material_type_key = 'ZPAK' THEN 'NON PPM'
    WHEN MM.material_group = 'DP_P011F' THEN 'NON PPM'
    WHEN MM.base_unit_of_measure != 'EA' AND MM.base_unit_of_measure != 'VL' THEN 'API Finished'
    WHEN MP.plant_procurement_type IN ('F','E','X') THEN 'Semi Finished'
    ELSE 'NOT VALID'
    END  "item_type",
    MM.BASE_UNIT_OF_MEASURE
    FROM
    PGSUDH_DL_PROD.PGSUDH_CDM.ecc_material_master MM
    INNER JOIN
    PGSUDH_DL_PROD.PGSUDH_CDM.ecc_material_plant MP
    ON 
    MP.material_key = MM.material_key
    AND MP.plant_key = 'BE37';
    """
    df = _udh_query(sql=sql_snow_master_data)
    df.drop_duplicates(inplace=True)
    df.columns = [column.upper() for column in df.columns] 
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)
    return fp_of_df

def get_fiscal_period(**context):
    sql_fiscal_period = """
    SELECT DISTINCT
    "CALENDAR_YEAR_WEEK",
    "FISCAL_PERIOD",
    "FISCAL_YEAR"
    FROM
        (SELECT 
        CASE WHEN LENGTH(CONCAT(YEAROFWEEKISO(DATE_SQL),WEEKISO(DATE_SQL))) = 6
        THEN CAST(CONCAT(YEAROFWEEKISO(DATE_SQL), WEEKISO(DATE_SQL)) AS INTEGER)
        ELSE CAST(CONCAT(YEAROFWEEKISO(DATE_SQL),0, WEEKISO(DATE_SQL)) AS INTEGER)
        END "CALENDAR_YEAR_WEEK",
        FISC_CURR_PER "FISCAL_PERIOD",
        FISC_CURR_YEAR "FISCAL_YEAR",
        RANK () OVER (PARTITION BY "CALENDAR_YEAR_WEEK" ORDER BY FISC_CURR_PER ASC) "RANK"
        FROM PGSUDH_DL_PROD.PGSUDH_CDM.DATE_DIM DATE_DIM
        WHERE 
        CALENDAR_VARIANT = 'Z2'
        AND YEAROFWEEKISO(DATE_SQL) >= YEAROFWEEKISO(CURRENT_DATE())
        )
    WHERE "RANK" = 1
    ORDER BY 
    "CALENDAR_YEAR_WEEK" asc"""
    df = _udh_query(sql=sql_fiscal_period)
    df.drop_duplicates(inplace=True)
    df.columns = [column.upper() for column in df.columns] 
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)
    return fp_of_df

def get_pallet_information(**context):
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    SambaServer.pull_excel(
        conn_id='edcnas',
        adress=DagConfig.CONFIG['fp_pallet_information'],
        destination=fp_of_df,
        sheet_name='Sheet 1'
    )
    return fp_of_df

def get_ZCON(**context):
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    SambaServer.pull_excel(
        conn_id='edcnas',
        adress=DagConfig.CONFIG['fp_ZCON'],
        destination=fp_of_df,
        sheet_name='ZCON'
    )
    return fp_of_df

def process_item_explosion(**context):
    #Create first an explosion on item level and merge with master data and data to calculate measures (PALLET/VALUE calculations)
    import pandas as pd
    df_item_calendar_WeekYear = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='create_year_week_combination'))
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_snowflake_data_inventory'))
    df_max_per_pallet = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_pallet_information'))
    df_ZCON = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_ZCON'))
    df_Material_master= pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_snowflake_data_master_data'))
    df_fiscal_period = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_fiscal_period'))    
    df = df_item_calendar_WeekYear.merge(df,
                                        how="left",
                                        left_on = ["MATERIAL_KEY", "CALENDAR_YEAR_WEEK"],
                                        right_on = ["MATERIAL_KEY","CALENDAR_YEAR_WEEK"],
                                        suffixes = ["","_DROP"]).filter(regex='^(?!.*_DROP)')
    df.sort_values(by=["MATERIAL_KEY","CALENDAR_YEAR_WEEK"],inplace=True)
    df.reset_index(drop=True,inplace=True)
    df.fillna({"TOTAL_STOCK":0,
                "TOTAL_DEMAND":0,
                "TOTAL_RECEIPTS":0,
                "TOTAL_QUANTITY":0},inplace=True)
    #merge with max per pallet master data
    df = df.merge(df_max_per_pallet,
                    how="left",
                    left_on=["MATERIAL_KEY"],
                    right_on=["ITEM"],
                    suffixes=["","_DROP"]).filter(regex='^(?!.*_DROP)')
    #merge with UDH material master
    df = df.merge(df_Material_master,
                    how="left",
                    left_on=["MATERIAL_KEY"],
                    right_on=["MATERIAL_KEY"],
                    suffixes=["","_DROP"]).filter(regex='^(?!.*_DROP)')
    #FILTER OUT MF/CLIN/ET/BUFFER
    df = df.loc[df["ITEM_TYPE"]!="MF/CLIN/ET/BUFFER"]
    #merge ZCON price
    df = df.merge(df_ZCON,
                    how='left',
                    left_on='MATERIAL_KEY',
                    right_on='Material',
                    suffixes=["","_DROP"]).filter(regex='^(?!.*_DROP)')
    #merge fiscal period
    df = df.merge(df_fiscal_period,
                  how='left',
                  left_on='CALENDAR_YEAR_WEEK',
                  right_on='CALENDAR_YEAR_WEEK',
                  suffixes=["","_DROP"]).filter(regex='^(?!.*_DROP)')
    df = df[["MATERIAL_KEY","CALENDAR_YEAR_WEEK","FISCAL_PERIOD","FISCAL_YEAR","TOTAL_STOCK","TOTAL_DEMAND",
            "TOTAL_RECEIPTS", "TOTAL_QUANTITY", "BASE_UNIT_OF_MEASURE", "MAX(QUANTITY_PER_PALLET)",
            "MEDIAN(QUANTITY_PER_PALLET)", "ITEM_DESCRIPTION_MATERIAL_MASTER", "ITEM_TYPE","PROFIT_CENTER", "MPG_CODE","ZCON"
            ]]
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)    
    return fp_of_df

def process_first_output(**context):
    #Create calculation on lowest item level
    import pandas as pd
    import numpy as np
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_item_explosion'))
    df.sort_values(by=["MATERIAL_KEY", "CALENDAR_YEAR_WEEK"],inplace=True)
    df.reset_index(drop=True,inplace=True)
    df["PROJECTED_INVENTORY"] = df.groupby(["MATERIAL_KEY"])["TOTAL_QUANTITY"].transform('cumsum')
    #MAX per Pallet
    df["MAX_PER_PALLET"] = df["PROJECTED_INVENTORY"]/df["MAX(QUANTITY_PER_PALLET)"]
    #MEDIAN per Pallet
    df["MEDIAN_PER_PALLET"] = df["PROJECTED_INVENTORY"]/df["MEDIAN(QUANTITY_PER_PALLET)"]
    #value
    df["VALUE"] = np.ceil(df["PROJECTED_INVENTORY"]*df["ZCON"])
    #set NA and inf value to 0
    df.replace([np.nan,-np.inf,np.inf], 0, inplace=True)
    #merge with item cluster file to also have item clustering on this level link to tableau to have details underneath the item cluster 
    df_item_clustering = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='get_item_clustering'))
    df_item_clustering.rename(columns={"PROFIT_CENTER": "PROFIT_CENTER_CLUSTER",
                                        "MPG_CODE":"MPG_CODE_CLUSTER"},inplace=True) 
    df = df.merge(df_item_clustering, 
                how="left",
                left_on="MATERIAL_KEY",
                right_on="MATERIAL_KEY",
                suffixes=["","_DROP"]).filter(regex="^(?!.*_DROP)")
    #drop unwanted columns
    df.drop(columns=["MAX(QUANTITY_PER_PALLET)", "MEDIAN(QUANTITY_PER_PALLET)","ZCON"], inplace=True)
    df = df[["MATERIAL_KEY","ITEM_DESCRIPTION_MATERIAL_MASTER","ITEM_TYPE","PROFIT_CENTER","MPG_CODE","CALENDAR_YEAR_WEEK",
             "FISCAL_PERIOD","FISCAL_YEAR", "TOTAL_STOCK", "TOTAL_DEMAND", "TOTAL_RECEIPTS", "TOTAL_QUANTITY","PROJECTED_INVENTORY", 
             "BASE_UNIT_OF_MEASURE", "MAX_PER_PALLET", "MEDIAN_PER_PALLET", "VALUE", "PROFIT_CENTER_CLUSTER","MPG_CODE_CLUSTER", "MATERIAL_KEY_GROUPED", "ITEMS_IN_CLUSTER"]]
    fp_of_df = TempStorage.unique_name(tz_local,**context) 
    df.to_pickle(fp_of_df)
    return fp_of_df  

def get_item_clustering(**context):
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    SambaServer.pull_excel(
        conn_id='edcnas',
        adress=DagConfig.CONFIG['fp_Item_clustering'],
        destination=fp_of_df,
        sheet_name='Output'
    )
    return fp_of_df

def process_second_output(**context):
    #Create calculation on item cluster level, first combine item who are defined to be clustered in order to calculate desired measures
    import pandas as pd
    import numpy as np
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_first_output'))
    #replace original material key and item description with the grouped material key from the item grouping file
    df.loc[~df["MATERIAL_KEY_GROUPED"].isna(),
            "MATERIAL_KEY"] = df["MATERIAL_KEY_GROUPED"]
    df.loc[~df["MATERIAL_KEY_GROUPED"].isna(),
            "ITEM_DESCRIPTION_MATERIAL_MASTER"] = df["ITEMS_IN_CLUSTER"]
    df.loc[~df["MATERIAL_KEY_GROUPED"].isna(),
            "PROFIT_CENTER"] = df["PROFIT_CENTER_CLUSTER"]
    df.loc[~df["MATERIAL_KEY_GROUPED"].isna(),
            "MPG_CODE"] = df["MPG_CODE_CLUSTER"]
    df.drop(columns=["PROFIT_CENTER_CLUSTER","MPG_CODE_CLUSTER","MATERIAL_KEY_GROUPED", "PROJECTED_INVENTORY"], inplace=True)
    #perform aggregations
    df = df.groupby(["MATERIAL_KEY", "CALENDAR_YEAR_WEEK","FISCAL_PERIOD","FISCAL_YEAR","BASE_UNIT_OF_MEASURE",
                    "ITEM_DESCRIPTION_MATERIAL_MASTER", "ITEM_TYPE", 
                    "PROFIT_CENTER","MPG_CODE"]).agg({"TOTAL_STOCK":"sum",
                                                                 "TOTAL_DEMAND":"sum",
                                                                 "TOTAL_RECEIPTS":"sum",
                                                                 "TOTAL_QUANTITY":"sum",
                                                                 "MAX_PER_PALLET":"sum",
                                                                 "MEDIAN_PER_PALLET":"sum",
                                                                 "VALUE":"sum"}).reset_index()
    df.sort_values(by=["MATERIAL_KEY", "CALENDAR_YEAR_WEEK"],inplace=True)
    df.reset_index(drop=True,inplace=True)
    #create a new projected inventory
    df["PROJECTED_INVENTORY"] = df.groupby(["MATERIAL_KEY"])["TOTAL_QUANTITY"].transform('cumsum')
    #----------MOH CALCULATIONS---------
    #---------MOH PREDEFINED DATE RANGE FIXED DEMAND CALCULATION---------
    #select needed columns
    df_year_volume = df[["MATERIAL_KEY","CALENDAR_YEAR_WEEK","TOTAL_DEMAND"]]
    #filter dataframe
    df_year_volume = df_year_volume.loc[(df_year_volume["CALENDAR_YEAR_WEEK"]>=get_year_week_current_date())&
                                        (df_year_volume["CALENDAR_YEAR_WEEK"]<get_year_week_forward_looking(DagConfig.CONFIG['weeks_forward_MOH']))]
    #make demand postive
    df_year_volume["TOTAL_DEMAND"] = df_year_volume["TOTAL_DEMAND"]
    df_year_volume.loc[df_year_volume["TOTAL_DEMAND"]<0,
                    "TOTAL_DEMAND"] = -df_year_volume["TOTAL_DEMAND"]
    #Get sum of total demand for each material and calendar year
    df_year_volume = df_year_volume.groupby(["MATERIAL_KEY"])["TOTAL_DEMAND"].sum().reset_index()
    #rename columns
    df_year_volume.rename(columns={"TOTAL_DEMAND":"8_MONTH_VOLUME"},inplace=True)
    #Merge with previous created year volume dataframe to create MOH
    df = df.merge(df_year_volume,how="left",
                    left_on="MATERIAL_KEY",
                    right_on="MATERIAL_KEY",
                    suffixes=["","_DROP"]).filter(regex="^(?!.*_DROP)")
    #Calculate MOH FIXED Demand
    df["MOH_FIXED"] = df["PROJECTED_INVENTORY"]/(df["8_MONTH_VOLUME"]/8)
    #---------MOH ROLLING DEMAND CALCULATION---------
    #function to calculate rolling_sum of demand
    df["TOTAL_DEMAND_POSTIVE"] = df["TOTAL_DEMAND"]
    df.loc[df["TOTAL_DEMAND_POSTIVE"]<0,
                    "TOTAL_DEMAND_POSTIVE"] = -df["TOTAL_DEMAND_POSTIVE"]
    #Function to calculate rolling demand for 52 weeks
    def rolling_sum_with_constraints(group, window=52):
        return group[::-1]['TOTAL_DEMAND_POSTIVE'].rolling(window=window, min_periods=1).sum()[::-1]
    df["TOTAL_DEMAND_ROLLING_YEAR"] = df.groupby("MATERIAL_KEY").apply(lambda x: rolling_sum_with_constraints(x)).reset_index(drop=True)
    df["MOH_ROLLING"] = df["PROJECTED_INVENTORY"]/(df["TOTAL_DEMAND_ROLLING_YEAR"]/12)
    #make sure everything defined by 0 and N/A is set to 0
    df.replace([np.nan,-np.inf,np.inf], 0, inplace=True)
    # df["TOTAL_VALUE_PER_WEEK"] = df.groupby(["PROFIT_CENTER","ITEM_TYPE","CALENDAR_YEAR_WEEK"])["VALUE"].transform('sum')
    # df["WEIGHTED_MOH"] = (df["MOH"]*df["VALUE"])/(df["TOTAL_VALUE_PER_WEEK"])
    # df.replace([np.nan,-np.inf,np.inf], 0, inplace=True)
    df = df[["MATERIAL_KEY","ITEM_DESCRIPTION_MATERIAL_MASTER", "CALENDAR_YEAR_WEEK", "FISCAL_PERIOD",
             "FISCAL_YEAR", "BASE_UNIT_OF_MEASURE", "ITEM_TYPE", "PROFIT_CENTER", "MPG_CODE",
             "VALUE", "MEDIAN_PER_PALLET", "MOH_FIXED","MOH_ROLLING", "8_MONTH_VOLUME","TOTAL_DEMAND_ROLLING_YEAR", "PROJECTED_INVENTORY", "TOTAL_STOCK", "TOTAL_DEMAND", "TOTAL_RECEIPTS"]]
    #set all float columns to 2 decimals.
    df[df.select_dtypes(include=['float']).columns] = df.select_dtypes(include=['float']).round(2)    
    fp_of_df = TempStorage.unique_name(tz_local,**context) 
    df.to_pickle(fp_of_df)    
    return fp_of_df

def process_third_output(**context):
    #aggregate on profit center and item type to further calculate desired measures
    import pandas as pd
    import numpy as np
    df = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_second_output'))
    df["TOTAL_VALUE_PER_WEEK"] = df.groupby(["PROFIT_CENTER","ITEM_TYPE","CALENDAR_YEAR_WEEK"])["VALUE"].transform('sum')
    #---------CALCULATED WEIGHTED FIXED MOH---------
    df["WEIGHTED_MOH_FIXED"] = (df["MOH_FIXED"]*df["VALUE"])/(df["TOTAL_VALUE_PER_WEEK"])
    #---------CALCULATED WEIGHTED ROLLING MOH---------
    df["WEIGHTED_MOH_ROLLING"] = (df["MOH_ROLLING"]*df["VALUE"])/(df["TOTAL_VALUE_PER_WEEK"])
    df.replace([np.nan,-np.inf,np.inf], 0, inplace=True)
    df = df.groupby(["PROFIT_CENTER","ITEM_TYPE","CALENDAR_YEAR_WEEK","FISCAL_PERIOD","FISCAL_YEAR"]).agg({"WEIGHTED_MOH_FIXED":"sum",
                                                                                                           "WEIGHTED_MOH_ROLLING":"sum",
                                                                                                           "MEDIAN_PER_PALLET":"sum",
                                                                                                           "VALUE":"sum",}).reset_index()
    #set all float columns to 2 decimals.
    df[df.select_dtypes(include=['float']).columns] = df.select_dtypes(include=['float']).round(2)   
    fp_of_df = TempStorage.unique_name(tz_local, **context)
    df.to_pickle(fp_of_df)   
    return fp_of_df

def convert_to_txt_items(**context):
    import pandas as pd
    df_item = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_first_output'))
    df_item_cluster = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_second_output'))    
    df_profit_center_item_type = pd.read_pickle(context.get('ti').xcom_pull(
        task_ids='process_third_output'))
    root_fp = f'{TempStorage.unique_name(tz_local=tz_local, **context)}'
    outputs = ["df_item" , "df_item_cluster" , "df_profit_center_item_type"]
    lst_of_location = []
    for output in outputs:
        if output == "df_item":
            df = df_item
        elif output == "df_item_cluster":
            df = df_item_cluster
        elif output == "df_profit_center_item_type":
            df = df_profit_center_item_type
        fp_of_txt = f'{root_fp}_{output}.txt'
        df.to_csv(fp_of_txt, index=False, sep='|')
        lst_of_location.append({
            'ouput': output,
            'fp_of_txt' : fp_of_txt
        })
    return lst_of_location

def push_source_data(
        ouput: str, fp_of_txt: str, **context):
    root_folder = DagConfig.CONFIG["export_folder_prod"]
    SambaServer.copy_file(
        'edcnas',
        destination=f'{root_folder}\BusEx - Supply - {ouput}.txt',
        adress=fp_of_txt
    )

with DAG(
    dag_id='SUPPLY006_Inventory_projection',
    schedule_interval='0 7 * * *',
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    on_success_callback=TempStorage.clean_temps,
    tags=['iUDH', 'oEDCNAS', 'GMP_NON'],
    description="""
        This DAG will query UDH SAP APO + ECC data for an inventory simulation and will write ouput to EDCNAS
    """
) as dag:

    setup_temps = PythonOperator(
        task_id='setup_temps',
        python_callable=setup_temps
    )
    get_snowflake_data_inventory = PythonOperator(
        task_id='get_snowflake_data_inventory',
        python_callable=get_snowflake_data_inventory
    )
    create_year_week_combination = PythonOperator(
        task_id='create_year_week_combination',
        python_callable=create_year_week_combination
    )

    get_snowflake_data_master_data = PythonOperator(
        task_id='get_snowflake_data_master_data',
        python_callable=get_snowflake_data_master_data
    )

    get_fiscal_period = PythonOperator(
        task_id='get_fiscal_period',
        python_callable=get_fiscal_period
    )

    get_pallet_information = PythonOperator(
        task_id='get_pallet_information',
        python_callable=get_pallet_information
    )

    get_ZCON = PythonOperator(
        task_id='get_ZCON',
        python_callable=get_ZCON
    )

    process_item_explosion = PythonOperator(
        task_id='process_item_explosion',
        python_callable=process_item_explosion
    )
    
    get_item_clustering = PythonOperator(
        task_id='get_item_clustering',
        python_callable=get_item_clustering
    )

    process_first_output = PythonOperator(
        task_id='process_first_output',
        python_callable=process_first_output
    )

    process_second_output = PythonOperator(
        task_id='process_second_output',
        python_callable=process_second_output
    )

    process_third_output = PythonOperator(
        task_id='process_third_output',
        python_callable=process_third_output
    )

    convert_to_txt_items = PythonOperator(
        task_id='convert_to_txt_items',
        python_callable=convert_to_txt_items
    ) 
    
    push_source_data = PythonOperator.partial(
        task_id='push_source_data',
        python_callable=push_source_data,
    ).expand(
        op_kwargs=XComArg(convert_to_txt_items)
    )
    
    finishing_task = EmptyOperator(
        task_id='finishing_task',
        on_success_callback=TempStorage.clean_temps
    )

setup_temps >> [
    get_snowflake_data_inventory, 
    get_snowflake_data_master_data,
    get_fiscal_period
] >> create_year_week_combination >> [
    get_ZCON, 
    get_item_clustering
] >> process_item_explosion >> process_first_output
process_first_output >> process_second_output >> process_third_output
process_third_output >> convert_to_txt_items >> push_source_data >> finishing_task
