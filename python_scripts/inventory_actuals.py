import pandas as pd
import xlwings as xw
import sys
import win32api

def get_xlwings_instance():
    app = xw.apps.active
    book = app.books.active
    return book
def read_sheet_as_dataframe(sheet_name:str):
    rng = get_xlwings_instance().sheets(sheet_name).used_range
    df = get_xlwings_instance().sheets(sheet_name).range(rng).options(pd.DataFrame, index=False).value
    return df
def error_message(message):
    win32api.MessageBox(0, message, 'info')
    sys.exit()
def snowflake_connection(sql):
    import snowflake.connector
    conn = snowflake.connector.connect(account='amerprod01.us-east-1.privatelink',
                                host='amerprod01.us-east-1.privatelink.snowflakecomputing.com',
                                authenticator='externalbrowser',
                                database='PGSUDH_DL_PROD',
                                warehouse='PFE_COMMON_WH_L_01',
                                user='iven.vandertaelen@pfizer.com',
                                role='PGSUDH_DL_RO_PROD')
    cur = conn.cursor()
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    return df
#load ZCON prices
def get_zcon_prices():
    df = read_sheet_as_dataframe("ZCON")
    return df
#Inventory actuals from UDH table GLOBAL_INVENTORY_QUANTITY 
#create dataframe from UDH with inventory data
def get_actual_inventory():
    #Get date value from input excel sheet
	from datetime import datetime
	sheet = get_xlwings_instance().sheets("Cockpit")
	date_value = sheet.range("D5").value
	date_value = str(date_value)
	df = snowflake_connection(f"""WITH 
	INVENTORY AS (
		SELECT
		GIQ.PLANT_KEY,
		GIQ.MATERIAL_KEY,
		GIQ.BATCH,
		GIQ.FISCAL_YEAR_PERIOD,
		GIQ.FISCAL_YEAR,
		GIQ.CALENDAR_DAY,
		GIQ.QUANTITY,
		GIQ.UNIT_OF_MEASURE,
		GIQ.STANDARD_PRICE,
		GIQ.STOCK_VALUE,
		GIQ.CURRENCY_KEY
		FROM PGSUDH_DL_PROD.PGSUDH_CDM.GLOBAL_INVENTORY_QUANTITY GIQ
		WHERE GIQ.PLANT_KEY in ('BE37', 'BE94', 'BE95', 'BEB4')
		),
	MAX_DATE_FISCAL_PERIOD AS (
		SELECT
		MAX(CALENDAR_DAY) "MAX_DATE"
		FROM PGSUDH_DL_PROD.PGSUDH_CDM.GLOBAL_INVENTORY_QUANTITY
		WHERE PLANT_KEY in ('BE37', 'BE94', 'BE95', 'BEB4')
		AND FISCAL_YEAR_PERIOD = '{date_value}'
		)		
	SELECT
	'ACTUAL'"INVENTORY_TYPE",
	INVENTORY.MATERIAL_KEY,
	INVENTORY.BATCH,
	INVENTORY.FISCAL_YEAR,
    INVENTORY.FISCAL_YEAR_PERIOD,
    INVENTORY.QUANTITY,
	INVENTORY.STOCK_VALUE,
	FROM INVENTORY INVENTORY 
	WHERE 
	INVENTORY.CALENDAR_DAY = (SELECT MAX_DATE FROM MAX_DATE_FISCAL_PERIOD)
	ORDER BY INVENTORY.FISCAL_YEAR_PERIOD 
	;""")
    #df.drop_duplicates(inplace=True)
	#read ZCON sheet as Dataframe
	df_ZCON = get_zcon_prices()
    #Merge WIP with ZCON to transform standard prices to ZCON(statistical) prices
	df = df.merge(df_ZCON,
               how= "left", 
               left_on= "MATERIAL_KEY",
               right_on= "Material", 
               suffixes= ["", "_DROP"]).filter(regex="^(?!.*_DROP)")
 	#calculate ZCON total price, for rows for which there is no ZCON value, use the original price calculated with the standard price
	df["TOTAL_ZCON"] = df["QUANTITY"]*df["UnitPrice ZCON"]
	df = df[["INVENTORY_TYPE", "FISCAL_YEAR", "FISCAL_YEAR_PERIOD", "MATERIAL_KEY", "BATCH", "TOTAL_ZCON", "QUANTITY"]]
	return df
#Distressed summary --> Create an aggregation on item level with total values in column tmpl ASL estimated 
def get_distressed_inventory():
    df_distressed = read_sheet_as_dataframe("Summary_distressed")
    df = df_distressed.groupby(["tmpl_item_nr","tmpl_lot_nr"])["tmpl_ASL_estimated"].agg("sum").reset_index()
    df.rename(columns={"tmpl_item_nr":"MATERIAL_KEY",
                       "tmpl_lot_nr":"BATCH",
                       "tmpl_ASL_estimated":"TOTAL_ZCON"}, inplace=True)
    #make values in order to deduct this from actual value
    df["TOTAL_ZCON"]= -df["TOTAL_ZCON"]
    #add constant value columns to distinguish Distressed values + match with inventory actuals
    df["INVENTORY_TYPE"] = "DISTRESSED"
    df["QUANTITY"] = 0
    df["FISCAL_YEAR_PERIOD"] = ""
    df["FISCAL_YEAR"] = ""
    df = df[["INVENTORY_TYPE", "FISCAL_YEAR_PERIOD", "MATERIAL_KEY", "BATCH", "TOTAL_ZCON", "QUANTITY"]]
    return df
#WIP --> Use input from Finance (SAP transaction: ZF_PCWIP_KOB1) and 
#translate inventory value to parent item of the process order (Parent item on process order retrieved from UDH table ECC_PROCESS_ORDER)
def get_WIP_inventory():
    #Read WIP sheet as Dataframe
    df_WIP = read_sheet_as_dataframe("WIP")
    #Remove obsolete lines
    df_WIP = df_WIP.loc[~df_WIP["Total Quantity"].isnull()]
    #read ZCON sheet as Dataframe
    df_ZCON = get_zcon_prices()
    #Merge WIP with ZCON to transform standard prices to ZCON(statistical) prices
    df_WIP_ZCON = df_WIP.merge(df_ZCON, 
                            how= "left",
                            left_on= "Material",
                            right_on= "Material",
                            suffixes= ["", "_DROP"]).filter(regex="^(?!.*_DROP)")
    #calculate ZCON total price, for rows for which there is no ZCON value, use the original price calculated with the standard price (e.g. Machine hours, labor hours,...)
    df_WIP_ZCON["TOTAL_ZCON"] = df_WIP_ZCON["Total Quantity"]*df_WIP_ZCON["UnitPrice ZCON"]
    df_WIP_ZCON.loc[(df_WIP_ZCON["TOTAL_ZCON"].isnull())|
                    (df_WIP_ZCON["Total Quantity"]==0),
                    "TOTAL_ZCON"] = df_WIP_ZCON["Value in Obj. Crcy"]
    df_WIP_ZCON = df_WIP_ZCON[["Order", "TOTAL_ZCON"]]
    df_WIP_ZCON.fillna(0, inplace=True)
    #Get the parent item of the process orders
    #first create a tuple with the process orders to downsize query
    orders = tuple(df_WIP_ZCON["Order"])
    df_parent_item = snowflake_connection(f"""SELECT DISTINCT 
    PROCESS_ORDER "BATCH",
    MATERIAL_KEY 
    FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_PROCESS_ORDER
    where PROCESS_ORDER in {orders}""")
    #merge WIP_ZCON with parent items in order to have a final outcome that links WIP value linked to parent item
    df_WIP_final = df_parent_item.merge(df_WIP_ZCON,
                                        how= "left",
                                        left_on= "BATCH",
                                        right_on= "Order",
                                        suffixes= ["", "_DROP"]).filter(regex="^(?!.*_DROP)")
    df = df_WIP_final[["MATERIAL_KEY","BATCH", "TOTAL_ZCON"]]
    df = df_WIP_final.groupby(["MATERIAL_KEY","BATCH"])["TOTAL_ZCON"].sum().reset_index()
    df["INVENTORY_TYPE"] = "WIP"
    df["QUANTITY"] = 0
    df["FISCAL_YEAR_PERIOD"] = ""
    df["FISCAL_YEAR"]= ""
    df = df[["INVENTORY_TYPE", "FISCAL_YEAR", "FISCAL_YEAR_PERIOD", "MATERIAL_KEY", "BATCH", "TOTAL_ZCON", "QUANTITY"]]    
    return df
#Read GENERAL PROVISION TO BE ADDED
def general_provision():
    df = get_xlwings_instance().sheets("Cockpit").range("C7:G13").options(pd.DataFrame, index=False).value
    df["TOTAL_ZCON"] = -df["TOTAL_ZCON"]
    df["QUANTITY"] = 0
    #df["MPG_CODE"] = df["MPG_CODE"].astype(str)
    return df
#Material Master UDH
def get_material_master():
	df = snowflake_connection("""SELECT
			MM.MATERIAL_KEY,
			MM.Material_desc,
			CASE WHEN MP.mpg_desc IS NULL THEN 'PUURS' ELSE UPPER(MP.mpg_desc) END "PROFIT_CENTER",
			CASE WHEN MP.mpg_desc IS NULL THEN '2479110' ELSE MP.FDM_MPG END "MPG_CODE",
			CASE 
			WHEN MM.material_type_key = 'VERP' THEN 'VERP'
			WHEN MM.material_type_key = 'ZMPS' THEN 'ZMPS'
			WHEN MM.material_type_key = 'FERT' THEN 'Saleable Goods'
			WHEN MM.material_type_key = 'HALB' AND MP.material_sourcing_flag = '7' THEN 'Saleable Goods'
			WHEN MM.material_type_key = 'ZRAW' THEN 'Inact RM'
			WHEN MM.material_type_key in ('ZNVH', 'ZNXH') THEN 'Semi Finished'
			WHEN MM.material_group IN ('DP_P055I','DP_P022D', 'DP_P055A', 'DP_P055G','DP_P055C', 'DP_P055F', 'DP_P044C', 'DP_P044E', 'DP_P022A', 'DP_P022E', 'DP_P055H', 'DP_P055E', 'XXXX') THEN 'PPM'
			WHEN MM.material_type_key = 'ZPAK' THEN 'NON PPM'
			WHEN MM.material_group = 'DP_P011F' THEN 'NON PPM'
			WHEN MM.base_unit_of_measure != 'EA' AND MM.base_unit_of_measure != 'VL' THEN 'API Finished'
			WHEN MP.plant_procurement_type IN ('F','E','X') THEN 'Semi Finished'
			ELSE 'NOT VALID'
			END  "ITEM_TYPE"
			FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_MASTER MM
			INNER JOIN PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_PLANT MP 
			ON MP.MATERIAL_KEY = MM.MATERIAL_KEY 
			AND MP.PLANT_KEY = 'BE37'
			"""
	)
	return df


#Merge dataframes
def process_files():
#create a total df that contains actual, distressed, wip and master data info
    df_actuals = get_actual_inventory()
    #Get fiscal year in object to be used further in script
    fiscal_year_actuals = df_actuals["FISCAL_YEAR"].iloc[0]
    df_distressed = get_distressed_inventory()
    df_WIP = get_WIP_inventory() 
    df = pd.concat([df_actuals,df_distressed,df_WIP], axis=0)
    #merge with master data
    df_master_data = get_material_master()
    df = df.merge(df_master_data,
                how= "left",
                left_on= "MATERIAL_KEY",
                right_on= "MATERIAL_KEY",
                suffixes=["","_DROP"]).filter(regex="^(?!.*_DROP)")   
    #ADD General provision --> Afterwards, because profit center&Item type are manually added in excel file and not added on material_key level
    df_general_provision = general_provision()
    
    #----------------------2 ISSUES LINKING ACTUAL WITH BU/LE NUMBERS IN TABLEAU----------------------
    #FIRST CREATE DATAFRAME THAT CONTAINS ALL POSSIBLE MPG CODES
    df_all_mpg_profit_center = df_master_data[["PROFIT_CENTER", "MPG_CODE"]].drop_duplicates().reset_index(drop=True)
    #There is 1 line with blank values - question why?, however to be sure not to have impact on output, removed this line
    df_all_mpg_profit_center = df_all_mpg_profit_center.loc[df_all_mpg_profit_center["MPG_CODE"] != ""]
    # #1. SOME MPG CODES HAVE DISTRESSED VALUE IN BU/LE AND NOT IN ACTUAL REPORTING
    # missing_mpg_distressed = ['DISTRESSED']
    # df_all_mpg_profit_center_distressed = df_all_mpg_profit_center.assign(key=1).merge(pd.DataFrame({'INVENTORY_TYPE': missing_mpg_distressed, 'key': 1}), on='key').drop('key', axis=1)
    # #Get unique dataframe of mpg codes with column inventory type =  Distressed of actual reporting
    # df_mpg_profit_center_distressed = df[["MPG_CODE","INVENTORY_TYPE"]].drop_duplicates()
    # df_mpg_profit_center_distressed.rename(columns={"MPG_CODE":"MPG_CODE_ACTUAL",
    #                                      "INVENTORY_TYPE":"INVENTORY_TYPE_ACTUAL"}, inplace=True)
    # #merge to see which mpg codes are not in the actual reporting and make a unique dataframe of mpg codes that are not in actual reporting
    # df_merge_distressed = df_all_mpg_profit_center_distressed.merge(df_mpg_profit_center_distressed,
    #                                     how='left',
    #                                     left_on=['MPG_CODE',"INVENTORY_TYPE"],
    #                                     right_on=['MPG_CODE_ACTUAL',"INVENTORY_TYPE_ACTUAL"],
    #                                     suffixes=["","_DROP"]
    #                                     ).filter(regex='^(?!.*_DROP)').drop_duplicates()
    # #filter to have mpg codes that are not in actual reporting
    # df_mpg_not_actual_distressed = df_merge_distressed.loc[df_merge_distressed["MPG_CODE_ACTUAL"].isnull()]
    # df_mpg_not_actual_distressed = df_mpg_not_actual_distressed.drop(columns=["MPG_CODE_ACTUAL","INVENTORY_TYPE_ACTUAL"])
    
    #1. TO SET DESTRESSED SEPERATE IN REPORTING OF BU AND LE, COMBINATION OF MPG AND ITEM TYPE DISTRESSED IS ADDED
    missing_mpg_distressed = ['DISTRESSED']
    df_all_mpg_profit_center_distressed = df_all_mpg_profit_center.assign(key=1).merge(pd.DataFrame({'INVENTORY_TYPE': missing_mpg_distressed, 'key': 1}), on='key').drop('key', axis=1)
    # The new values for the additional rows
    df_all_mpg_profit_center_distressed["ITEM_TYPE"]="Distressed"
    df_all_mpg_profit_center_distressed["TOTAL_ZCON"]=0
    df_all_mpg_profit_center_distressed["QUANTITY"]=0   
    #2. MPG CODES THAT ARE NOT IN ACTUAL REPORTING
    #SOME MPG CODES ARE NOT IN ACTUAL REPORTING, THIS IS CAUSING ISSUE WITH LINKING THEM TO BUDGET AND LE INVENTORY NUMBERS,AS SOME MPG CODES MIGHT HAVE NUMBERS LOGGED TO THEM
    #Create list of all possible item types
    missing_mpg_itemtypes = ['PPM', 'NON PPM', 'Semi Finished', 'API Finished', 'Inact RM', 'ZMPS', 'Saleable Goods']
    # Use pandas' `merge` with `cross join` to create all combinations of MPG codes an Item types
    df_all_mpg_profit_center_itemtypes = df_all_mpg_profit_center.assign(key=1).merge(pd.DataFrame({'ITEM_TYPE': missing_mpg_itemtypes, 'key': 1}), on='key').drop('key', axis=1)
    #Get unique dataframe of mpg codes in actual reporting
    df_mpg_profit_center_itemtype = df[["MPG_CODE","ITEM_TYPE"]].drop_duplicates()
    df_mpg_profit_center_itemtype.rename(columns={"MPG_CODE":"MPG_CODE_ACTUAL",
                                         "ITEM_TYPE":"ITEM_TYPE_ACTUAL"}, inplace=True)
    #merge to see which mpg codes are not in the actual reporting and make a unique dataframe of mpg codes that are not in actual reporting
    df_merge_itemtype = df_all_mpg_profit_center_itemtypes.merge(df_mpg_profit_center_itemtype,
                                        how='left',
                                        left_on=['MPG_CODE',"ITEM_TYPE"],
                                        right_on=['MPG_CODE_ACTUAL',"ITEM_TYPE_ACTUAL"],
                                        suffixes=["","_DROP"]
                                        ).filter(regex='^(?!.*_DROP)').drop_duplicates()
    #filter to have mpg codes that are not in actual reporting
    df_mpg_not_actual_itemtype = df_merge_itemtype.loc[df_merge_itemtype["MPG_CODE_ACTUAL"].isnull()]
    df_mpg_not_actual_itemtype = df_mpg_not_actual_itemtype.drop(columns=["MPG_CODE_ACTUAL","ITEM_TYPE_ACTUAL"])
    #make it possible to have all items types available for the missing mpg codes
    # The new values for the additional rows
    df_mpg_not_actual_itemtype["INVENTORY_TYPE"]="ACTUAL"
    df_mpg_not_actual_itemtype["TOTAL_ZCON"]=0
    df_mpg_not_actual_itemtype["QUANTITY"]=0   
    #concat all df's 
    df = pd.concat([df,df_general_provision,df_mpg_not_actual_itemtype,df_all_mpg_profit_center_distressed], axis=0)
    #Fiscal period to be converted and added to all rows
    fiscal_period_value = None
    fiscal_periods = tuple(df["FISCAL_YEAR_PERIOD"].unique().tolist())
    #the tuple will have a value and blanks, make sure to ignore the blanks
    for fiscal_period in fiscal_periods:
        if fiscal_period != "":
            fiscal_period_value = fiscal_period
            break
    fiscal_month = fiscal_period_value[:3]
    fiscal_year = fiscal_period_value[-4:]
    fiscal_period_value = int(fiscal_year+fiscal_month)
    df["FISCAL_YEAR_PERIOD"] = fiscal_period_value
    #Add the fiscal quarter in order to compare with BU and LE number in Tableau
    df["Period"] = df["FISCAL_YEAR_PERIOD"]
    #custom function to transfer a fiscal period to a quarter to eventually link this to BU and LE numbers in Tableau
    def fiscal_period_to_quarter(period):
        period = str(period)
        last_three = period[-3:]
        if last_three in ("001", "002", "003"):
            return "Q1"
        elif last_three in ("004", "005", "006"):
            return "Q2"
        elif last_three in ("007", "008", "009"):
            return "Q3"
        elif last_three in ("010", "011", "012"):
            return "Q4"
        else:
            return None  
    df["Period"] = df["Period"].apply(fiscal_period_to_quarter)
    #set for all rows fiscal year equal to fiscal year value
    df["FISCAL_YEAR"] = fiscal_year_actuals 
    return df

#Export to text file
def export_to_txt():
    # Create a copy of the DataFrame for export purposes
    df_export = process_files()
    #Converting decimal seperator to , instead of .
    # Apply formatting to each specified column
    columns_to_format = ["TOTAL_ZCON", "QUANTITY"]
    for col in columns_to_format:
        df_export[col] = df_export[col].apply(lambda x: f"{x:.2f}".replace('.', ','))
            #get name of dataframe to export
    #create folder path to make dump of output
    Export_folder = r"\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\Supply - Inventory History"
    #get value of first row of column FISCAL_YEAR_PERIOD to identify the export file
    fiscal_period_value = str(df_export["FISCAL_YEAR_PERIOD"].iloc[0])
    return df_export.to_csv(f"{Export_folder}/INV{fiscal_period_value}.csv", sep= ";", index=False)



#CHECK IF THERE IS A COMBO FISCAL YEAR, MPG CODE AND ITEM TYPE IN ACTUAL REPORTING AND NOT IN BUDGET AND LE FILE, IF NOT AVAILABLE MAKE LIST OF MISSING IN CURRENT EXCEL FILE
#VERDER WERKEN!!!!!!
#Get Budget and LE
def process_paste_missing():
    Budget_le_path = r'\\edcnas001\PUU-APP\Tableau_Sources\BusEx-Supply\Supply - Inventory History\BusEx - Supply - Inventaris (Budget + LE).xlsx'
    df_budget_le = pd.read_excel(Budget_le_path)
    df_budget_le = df_budget_le[['FISCAL_YEAR','MPG_CODE','ITEM_TYPE']].drop_duplicates()
    #Get actuals file that was processed
    df_actuals = process_files()
    df_actuals = df_actuals[['FISCAL_YEAR','MPG_CODE','PROFIT_CENTER','ITEM_TYPE']].drop_duplicates()
    #in df actuals, change item type to have the same format as actuals
    df_actuals["ITEM_TYPE_NEW"] = df_actuals['ITEM_TYPE']
    df_actuals.loc[df_actuals['ITEM_TYPE'].isin(['NON PPM', 'PPM', 'Inact RM', 'ZMPS']),
        'ITEM_TYPE_NEW'] = 'ZPAK/ZRAW'
    df_actuals.loc[df_actuals['ITEM_TYPE'].isin(['Semi Finished']),
        'ITEM_TYPE_NEW'] = 'WIP'

    #making sure all columns with dtype int64 are modified to string
    def columns_to_str(df):
        import numpy as np
        for c in df.columns:
            if df[c].dtype == np.int64:
                df[c] = df[c].astype(str)
        return df
    columns_to_str(df_budget_le)
    columns_to_str(df_actuals)      
    #merge Actuals with budget to see which combo of fiscal year, mpg code and item type that has no hit in budget/le file
    df_missing = df_actuals.merge(df_budget_le, 
                                how='left',
                                left_on=['FISCAL_YEAR','MPG_CODE', 'ITEM_TYPE_NEW'],
                                right_on=['FISCAL_YEAR','MPG_CODE','ITEM_TYPE'],
                                suffixes=['', '_budget'])
    #filter on combination that are null (meaning no match)
    df_missing_filter = df_missing.loc[df_missing["ITEM_TYPE_budget"].isnull()]      
    #define active workbook and paste missing in sheet Missing_in_budget_LE
    active_wb = get_xlwings_instance()
    sht = active_wb.sheets("Missing_in_budget_LE")
    sht.clear()
    sht["A1"].value = df_missing_filter
    if not df_missing_filter.empty:
        error_message("There are combinations of fiscal year, mpg code and item type missing in Budget, please add them")


if __name__ == "__main__":
    export_to_txt()
    process_paste_missing()
