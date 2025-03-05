#Doel wat willen we berekenen, we willen op het hoogste niveau (PACK & SF4S) de volumes laten door exploderen naar onderliggende niveaus
#We hebben ook Pack items die we niet willen laten exploderen
#we hebben ook fill items waar we gaan 'stock builden' die willen we ook laten exploderen
#import libraries
import pandas as pd
import xlwings as xw
import numpy as np
import sys
import win32api
import os
import json
import snowflake.connector
import getpass
#define error message
def error_message(message):
    win32api.MessageBox(app.api.Hwnd, message, 'info')
   # sys.exit()

def snowflake_connection(querypath: str, users: dict, datefrom: str = None) -> pd.DataFrame:
    """Run UDH query located under querypath and return pandas DataFrame

    Parameters
    ----------
    querypath : str
        location of the UDH query
    users : dict
        dictionary with keys the username and values the email address
    datefrom : str, optional
        Format: 'YYYY-MM-DD', filter query with start date, query must contain $__YEAR__$ key, by default None

    Returns
    -------
    pd.DataFrame
        result of the query in pd.dataframe
    """

    with open(querypath, 'r') as fp:
        sql = fp.read()

    if datefrom is not None:
        sql = sql.replace("$__YEAR__$", f"'{datefrom}'")

    user = getpass.getuser()

    # Load query from UDH
    conn = snowflake.connector.connect(
        account='amerprod01.us-east-1.privatelink',
        host='amerprod01.us-east-1.privatelink.snowflakecomputing.com',
        authenticator='externalbrowser',
        database='PGSUDH_DL_PROD',
        warehouse='PFE_COMMON_WH_XS_01',
        user='peter.vervaecke@pfizer.com',
        role='PGSUDH_DL_RO_PROD')
    cur = conn.cursor()
    cur.execute(sql)

    df = cur.fetch_pandas_all()
    return df

#define import sheet
def import_sheet(sheet_name: str) -> pd.DataFrame:
    app = xw.apps.active      
    wb = app.books.active
    
    dict={'Material Master':'Material_master.sql'
          ,'Recipes':'Budget recipes query.sql'
          ,'BOM':'Budget BOM query.sql'}
    if sheet_name in dict:
        wb.sheets(sheet_name).cells.clear()
        users = 'peter.vervaecke@pfizer.com'
        querypath = f"//edcnas004/PUU_OPEX/Plant Performance Data/Data/UDH Queries/" + dict[sheet_name]
        df_query=snowflake_connection(querypath,users)
        wb.sheets(sheet_name).range('A1').options(index=False).value = df_query
   
    usedrange = wb.sheets(sheet_name).used_range
    importedsheet = wb.sheets(sheet_name).range(
        usedrange).options(pd.DataFrame, index=False).value
    importedsheet = importedsheet.dropna(how='all')
    return importedsheet

#import sheets
df_volumes_pack=import_sheet('Volumes_pack')
df_volumes_pack['MATERIAL_KEY']=df_volumes_pack['MATERIAL_KEY'].str.strip()
df_volumes_SF4S = import_sheet('Volumes_SF4S')
df_volumes_SF4S['MATERIAL_KEY']=df_volumes_SF4S['MATERIAL_KEY'].str.strip()
df_volumes_stockbuild = import_sheet('Volumes_stockbuild')
df_volumes_stockbuild ['MATERIAL_KEY']=df_volumes_stockbuild ['MATERIAL_KEY'].str.strip()
df_bom=import_sheet('BOM')
df_recipes=import_sheet('Recipes')
df_lotsizes=import_sheet('Lotsizes')[["Material Number", "Lotsize"]]
df_lotsizes.rename(columns={"Material Number":"MATERIAL_KEY"},inplace=True)
df_lotsizes['Lotsize'] = df_lotsizes['Lotsize'].astype(float)
df_master=import_sheet('Material Master')
df_master=df_master[['MATERIAL_KEY','MPG_DESC','PLANT_PROCUREMENT_TYPE','BASE_UNIT_OF_MEASURE']]
#remove volume na and equal to 0 in volume dataframes
volume_dataframes = [df_volumes_pack, df_volumes_SF4S, df_volumes_stockbuild]
for i, df in enumerate(volume_dataframes):
    volume_dataframes[i] = df.loc[(df['Volume']!=0)&
                                  (df['Volume'].notna())] 
    
#--- TE BEKIJKEN OF WE MM03 KUNNEN GEBRUIKEN ALS LOTSIZE (Costing lotsize in Costing 1, SAP transactie ZMDM --> Finance report, BE37)
#Setting lotsizes
#lotsize volume pack OK - ingevuld door SUPPLY
#volume SF4S inladen en lotsize uit tab Lotsize halen (lotsize van OPEX)
df_volumes_SF4S = df_volumes_SF4S.merge(df_lotsizes,
                                        how='left',
                                        left_on='MATERIAL_KEY',
                                        right_on='MATERIAL_KEY')
#missing lotsize in OPEX lotsize files
df_volumes_SF4S_missing_lotsize = df_volumes_SF4S.loc[df_volumes_SF4S["Lotsize"].isna()]
df_volumes_stockbuild = df_volumes_stockbuild.merge(df_lotsizes,
                                        how='left',
                                        left_on='MATERIAL_KEY',
                                        right_on='MATERIAL_KEY')

#Eerst hoogste niveau definiëren = Pack & SF4S &
df_volume_lvl1 = pd.concat([df_volumes_pack, df_volumes_SF4S, df_volumes_stockbuild], axis=0).reset_index(drop=True)

#----ZOU NIET MOGEN --> NOG TE VERWIJDEREN
#df_volume_lvl1 = df_volume_lvl1[df_volume_lvl1['Lotsize'].notna()]
df_volume_lvl1['Lotsize'] = df_volume_lvl1['Lotsize'].fillna(0)

#aantal batchen + afgeronde budget volume bepalen op lvl 1
#batches, H items afronden op 0 decimalen naar boven, F items op 1 decimaal na de komma naar boven
df_volume_lvl1['Nr_of_orders'] = round(df_volume_lvl1['Volume']/df_volume_lvl1['Lotsize'],0)
df_volume_lvl1.loc[df_volume_lvl1['MATERIAL_KEY'].str.startswith('H'),
                   'Nr_of_orders'] = np.ceil((df_volume_lvl1['Volume']/df_volume_lvl1['Lotsize'])*10)/10
#calculate rounded volume
df_volume_lvl1['Volume_mult_ltsz'] = df_volume_lvl1['Lotsize']*df_volume_lvl1['Nr_of_orders']
#add lvl 1 indicator
df_volume_lvl1["Level"] = "lvl1"

#-------BOM EXPLOSIE-------
#Create copy of volume lvl1 to concat further BOM explosion
df_volume_total = df_volume_lvl1.copy()
df_volume_total = df_volume_total[['MATERIAL_KEY', 'ITEM DESCRIPTION', 'Volume', 'Explode', 'Level']]

#Eerst in volume lvl1 de explode = N nog uit filteren
df_explodelist = df_volume_lvl1.loc[df_volume_lvl1["Explode"]!="N"]
#BOM Explosie
#set i equal to 1 = lvl 1
i=1

#check om te bekijken dat elk item waar BOM explosie Y op staat ook effectief een onderliggende BOM heeft
df_BOM_check = df_explodelist.merge(df_bom,
                                    how='left',
                                    left_on=['MATERIAL_KEY'],
                                    right_on=['MATERIAL_KEY'])
df_BOM_check = df_BOM_check.loc[df_BOM_check['MAT_DESC'].isna()]
#popup error message
app = xw.apps.active      
wb = app.books.active
wb.sheets('error_dump_BOM').clear()
if not df_BOM_check.empty:
    wb.sheets('error_dump_BOM').range('A1').options(index=False).value=df_BOM_check
    error_message('Missing BOMs: zie sheet error_dump voor details.')

while not df_explodelist.empty:
    #BOM explosie, gebruik maken van BOM file om te bekijken welke items verder geëxplodeerd moeten worden
    df_explodelist=df_explodelist.merge(df_bom,
                                        how='left',
                                        left_on=['MATERIAL_KEY'],
                                        right_on=['MATERIAL_KEY'])    
    #nodige kolommen filteren
    df_explodelist = df_explodelist[['BOM_COMPONENT_KEY', 'MATERIAL_DESC', 'Volume','Nr_of_orders', 'BASE_QUANTITY', 'QUANTITY', 'FIXED_QUANTITY_IND','COMPONENT SCRAP (%)']]
    
    #1. Eerst volume vertalen naar onderliggend niveau
    
    #REGEL VAN 3 om volume door te vertalen in BOM naar onderliggend niveau + scrap factor (voor item nummer = 10 (inspectie/filling/...) is die altijd 0) 
    df_explodelist['Volume_requirement'] = (df_explodelist['QUANTITY']*(1+(df_explodelist['COMPONENT SCRAP (%)']/100))/df_explodelist['BASE_QUANTITY'])*df_explodelist['Volume']
    #FIXED QTY indicator in BOM behouden van Quantity in BOM en niet gebruik maken van regel van 3, hier wel maal aantal loten doen om zo wel de fixed qty per lot te krijgen
    df_explodelist.loc[df_explodelist['FIXED_QUANTITY_IND']=='X',
                        'Volume_requirement'] = df_explodelist['QUANTITY']*df_explodelist['Nr_of_orders']
    #groepering doen van volumes op item level om totaal volume op item te hebben    
    df_explodelist = df_explodelist.groupby(['BOM_COMPONENT_KEY', 'MATERIAL_DESC']).agg({#'Volume': 'sum',
                                                                                        'Volume_requirement': 'sum'}).reset_index()
    
    #2. Bekijken of onderliggend niveau al dan niet verder geëxplodeerd moet worden
    
    #BOM component terug exploderen met bom file om te bekijken of het item effectief nog een onderligged niveau heeft, 2 opties NA then is level i + req anders, level i+1
    df_explodelist_req_bom = df_explodelist.merge(df_bom,
                                              how='left',
                                              left_on=['BOM_COMPONENT_KEY'],
                                              right_on=['MATERIAL_KEY'],
                                              suffixes=['', '_req'])
    #2.1 indien niet gaan we die hier toewijzen aan het level i + req van dat level i     
    df_explodelist_req = df_explodelist_req_bom.loc[df_explodelist_req_bom['MATERIAL_KEY'].isna()] 
    df_explodelist_req = df_explodelist_req[['BOM_COMPONENT_KEY', 'MATERIAL_DESC', 'Volume_requirement']]
    #rename and add default columns 
    df_explodelist_req.rename(columns={'BOM_COMPONENT_KEY':'MATERIAL_KEY','MATERIAL_DESC':'ITEM DESCRIPTION','Volume_requirement':'Volume'},inplace=True)
    df_explodelist_req['Explode'] = 'N'
    df_explodelist_req['Level'] = f'lvl_req{i}'
    df_explodelist_req = df_explodelist_req[['MATERIAL_KEY', 'ITEM DESCRIPTION', 'Volume','Explode', 'Level']]
    
    #2.2 indien wel een onderliggend niveau gaan we verder met berekening van lotsize en order op level i + 1   
    df_explodelist = df_explodelist_req_bom.loc[df_explodelist_req_bom['MATERIAL_KEY'].notna()] 
    #duplicate lijnen verwijderen want je gaat hier per BOM lijn een identieke match hebben
    df_explodelist = df_explodelist[['BOM_COMPONENT_KEY', 'MATERIAL_DESC', 'Volume_requirement']]
    df_explodelist = df_explodelist.drop_duplicates()
    
    #volume gaan lotsizen zodat dit nieuwe volume meegenomen kan worden in verdere berekeningen
    #Calculate lotsize, batches en lotsized volume
    #merge met lotsize
    df_explodelist=df_explodelist.merge(df_lotsizes,
                                            how='left',
                                            left_on=['BOM_COMPONENT_KEY'],
                                            right_on=['MATERIAL_KEY'],
                                            suffixes=['', '_DROP']).filter(regex="^(?!.*_DROP)")
    df_explodelist = df_explodelist[['BOM_COMPONENT_KEY', 'MATERIAL_DESC', 'Volume_requirement', 'Lotsize']]
    #UIT TE NEMEN WANT AL DE LOTSIZES ZOUDEN AAN VALUE MOETEN HEBBEN!!!
    df_explodelist['Lotsize'] = df_explodelist['Lotsize'].fillna(0)
    #aantal batchen + afgeronde budget volume bepalen op lvl i
    #batches, F items afronden op 0 decimalen naar boven, H items op 1 decimaal na de komma naar boven
    df_explodelist['Nr_of_orders'] = round(df_explodelist['Volume_requirement']/df_explodelist['Lotsize'],0)
    df_explodelist.loc[df_explodelist['BOM_COMPONENT_KEY'].str.startswith('H'),
                            'Nr_of_orders'] = np.ceil((df_explodelist['Volume_requirement']/df_explodelist['Lotsize'])*10)/10
    #calculate rounded volume
    df_explodelist['Volume_mult_ltsz'] = df_explodelist['Lotsize']*df_explodelist['Nr_of_orders']
    
    #define level based on i
    i = i+1
    df_explodelist['Level'] = f'lvl{i}'
    #rename columns
    df_explodelist.rename(columns={'BOM_COMPONENT_KEY':'MATERIAL_KEY','MATERIAL_DESC':'ITEM DESCRIPTION','Volume_mult_ltsz':'Volume'},inplace=True)
    df_explodelist['Explode'] = 'Y'
    df_explodelist  = df_explodelist[['MATERIAL_KEY', 'ITEM DESCRIPTION', 'Volume', 'Nr_of_orders', 'Explode', 'Level']]
    #schrijf uitkomst weg
    df_volume_total=pd.concat([df_volume_total,df_explodelist, df_explodelist_req], axis=0)
    
    
#Items gaan groupen - onderscheid maken tussen items met bom onder (lvli) en items zonder bom onder (lvlireq)
#1. eerst volume op requirements items bekijken (items zonder BOM explosie)
df_volume_req_items = df_volume_total.loc[df_volume_total['Level'].str.contains('lvl_req')]
df_volume_req_items = df_volume_req_items.groupby(['MATERIAL_KEY', 'ITEM DESCRIPTION', 'Explode']).sum('Volume').reset_index()

#2. Volume op items met BOM explosie
df_volume_items_bom = df_volume_total.loc[~df_volume_total['Level'].str.contains('lvl_req')]
df_volume_items_bom = df_volume_items_bom.groupby(['MATERIAL_KEY', 'ITEM DESCRIPTION', 'Explode']).sum('Volume').reset_index()

#Calculate lotsize, batches en lotsized volume
#merge met lotsize
df_volume_items_bom=df_volume_items_bom.merge(df_lotsizes,
                                              how='left',
                                              left_on=['MATERIAL_KEY'],
                                              right_on=['MATERIAL_KEY'],
                                              suffixes=['', '_DROP']).filter(regex="^(?!.*_DROP)")
#UIT TE NEMEN WANT AL DE LOTSIZES ZOUDEN AAN VALUE MOETEN HEBBEN!!!
df_volume_items_bom['Lotsize'] = df_volume_items_bom['Lotsize'].fillna(0)
#aantal batchen + afgeronde budget volume bepalen op lvl i
#batches, H items afronden op 0 decimalen naar boven, F items op 1 decimaal na de komma naar boven
df_volume_items_bom['Nr_of_orders'] = round(df_volume_items_bom['Volume']/df_volume_items_bom['Lotsize'],0)
df_volume_items_bom.loc[df_volume_items_bom['MATERIAL_KEY'].str.startswith('H'),
                        'Nr_of_orders'] = np.ceil((df_volume_items_bom['Volume']/df_volume_items_bom['Lotsize'])*10)/10
#calculate rounded volume
df_volume_items_bom['Volume_mult_ltsz'] = df_volume_items_bom['Lotsize']*df_volume_items_bom['Nr_of_orders']
df_volume_items_bom  = df_volume_items_bom[['MATERIAL_KEY', 'ITEM DESCRIPTION', 'Lotsize', 'Volume', 'Explode','Nr_of_orders', 'Volume_mult_ltsz']]

#export output    
df_volume_items_bom=df_volume_items_bom.merge(df_master,
                                              how='left',
                                              left_on=['MATERIAL_KEY'],
                                              right_on=['MATERIAL_KEY'])
app = xw.apps.active
wb = app.books.active
wb.sheets('BOM_Explosion_Iven').clear()
wb.sheets('BOM_Explosion_Iven').range('A1').options(index=False).value=df_volume_items_bom

#Popup error missing lotsizes
df_missing_lotsize = df_volume_items_bom.loc[df_volume_items_bom['Lotsize']==0]
#popup error message
app = xw.apps.active      
wb = app.books.active
wb.sheets('error_dump_lotsizes').cells.clear()
if not df_missing_lotsize.empty:
    wb.sheets('error_dump_lotsizes').range('A1').options(index=False).value=df_missing_lotsize
    error_message('Missing lotsizes: zie sheet error_dump voor details.')


#######Capacity Report#################
df_volume_total=df_volume_items_bom.copy()
df_master=import_sheet('Material Master')
df_volume_total=df_volume_total.merge(df_master[['MATERIAL_KEY','MATERIAL_SOURCING_FLAG','M2M','MPG_DESC']],how='left',left_on=['MATERIAL_KEY'],right_on=['MATERIAL_KEY'])

df_volume_total=df_volume_total[df_volume_total['MATERIAL_SOURCING_FLAG']!=2]
df_volume_total=df_volume_total[df_volume_total['PLANT_PROCUREMENT_TYPE'].isin(['E','X'])]
df_volume_total=df_volume_total[df_volume_total['M2M']!='WM2M']
df_volume_total=df_volume_total[~df_volume_total['BASE_UNIT_OF_MEASURE'].isin(['GM','KG'])]
df_recipes['Yield']=df_recipes['HEADER_BASE_QTY']/1000
df_recipes.drop('MATERIAL_DESC',axis=1,inplace=True)
df_capacity=df_volume_total.merge(df_recipes,how='left',left_on=['MATERIAL_KEY'],right_on=['MATERIAL_KEY'])
df_capacity=df_capacity[~df_capacity['WORK_CENTER_RESOURCE'].isin(['Q50AO01','Q60AO01','Q50AO02','Q50AO03','Q50AO04','Q50AO05','M2MDUMMY'])]
df_profit=import_sheet('Profit').drop_duplicates()
df_capacity=df_capacity.merge(df_profit,how='left',left_on=['MPG_DESC'],right_on=['MPG_DESC'])
df_capacity['Profit center']=np.where(df_capacity['Profit center'].isna(),df_capacity['MPG_DESC'],df_capacity['Profit center'])


df_capacity['Volume_mult_ltsz']=np.where(df_capacity['Volume_mult_ltsz'].isna(),df_capacity['Volume'],df_capacity['Volume_mult_ltsz'])

df_capacity['Nr_of_orders']=np.ceil(df_capacity['Volume_mult_ltsz']/df_capacity['Lotsize'])
df_capacity['Mach Hrs']=np.where(df_capacity['OPERATION_BASE_QTY'].astype(float)>1000,df_capacity['Nr_of_orders']*(df_capacity['MACHSU']+df_capacity['MACHCU']+df_capacity['MACHRT']),df_capacity['Nr_of_orders']*(df_capacity['MACHSU']+df_capacity['MACHCU'])+(df_capacity['Volume_mult_ltsz']/df_capacity['Yield'])*df_capacity['MACHRT']/1000)
df_capacity['Lab Hrs']=df_capacity['Nr_of_orders']*(df_capacity['LABSU']+df_capacity['LABCU'])+(df_capacity['Volume_mult_ltsz']/df_capacity['Yield'])*df_capacity['LABRT']/1000

#WEG HALEN EN ERROR ACTIVEREN
#popup error message
if  not df_capacity[df_capacity['RECIPE'].isna()].empty:
     app = xw.apps.active      
     wb = app.books.active     
     wb.sheets('error_dump').cells.clear()  
     wb.sheets('error_dump').range('A1').options(index=False).value=df_capacity[df_capacity['RECIPE'].isna()]
     error_message('Missing recipes: zie sheet error_dump voor details.')

df_pres=import_sheet('Presentation')
df_capacity=df_capacity.merge(df_pres,how='left',left_on=['MATERIAL_KEY'],right_on=['MATERIAL_KEY'])

df_family=df_capacity[['MATERIAL_KEY','FAMILY']]
df_family=df_family[df_family['FAMILY'].notna()].drop_duplicates()
df_capacity.drop('FAMILY',inplace=True,axis=1)
df_capacity=df_capacity.merge(df_family,how='left',left_on=['MATERIAL_KEY'],right_on=['MATERIAL_KEY'])

df_capacity['QUANTITY_X_MULTIPLICITY']=df_capacity['QUANTITY_X_MULTIPLICITY'].fillna(1)
df_capacity['MULTIPLICITY_X_UNITS_IN_PACK']=df_capacity['MULTIPLICITY_X_UNITS_IN_PACK'].fillna(1)
df_capacity=df_capacity.fillna(0)
df_capacity=df_capacity.sort_values(by=['OPERATION_ACTIVITY_NUMBER'],ascending=False)
df_capacity['Mach Hrs']=df_capacity['Mach Hrs'].astype(float)
df_capacity['Lab Hrs']=df_capacity['Lab Hrs'].astype(float)

df_capacity=df_capacity.groupby(['MATERIAL_KEY','ITEM DESCRIPTION','Profit center','Lotsize','Volume_mult_ltsz','RECIPE','FAMILY','Nr_of_orders','RECIPE_DESCRIPTION','HEADER_BASE_QTY','UOM','MULTIPLICITY_X_UNITS_IN_PACK','QUANTITY_X_MULTIPLICITY','CONTAINER','OPERATION_BASE_QTY','WORK_CENTER_RESOURCE','RESOURCE_DESCRIPTION','PRESENTATION'],as_index=False)[['Mach Hrs','Lab Hrs']].sum()
df_capacity.rename(columns={'Volume_mult_ltsz':'Volume','MATERIAL_KEY':'Material','ITEM DESCRIPTION':'Material Description','FAMILY':'Family','HEADER_BASE_QTY':'Base Quantity','Nr_of_orders':'Batches','RECIPE':'Recipe','WORK_CENTER_RESOURCE':'Resource','QUANTITY_X_MULTIPLICITY':'Multi PLAC','MULTIPLICITY_X_UNITS_IN_PACK':'Multi Sales Unit','PRESENTATION':'Presentation'},inplace=True)

df_ref=import_sheet('Ref')
df_capacity=df_capacity.merge(df_ref,how='left',left_on=['Resource'],right_on=['Resource'])
df_capacity['Volume PLAC']=df_capacity['Volume']*df_capacity['Multi PLAC'].astype(float)
df_capacity['Volume Sales Unit']=df_capacity['Volume']*df_capacity['Multi Sales Unit'].astype(float)

df_pres=import_sheet('Presentation')

df_csp_volume=df_capacity[df_capacity['Volume ind']=='Y']
df_csp_volume=df_csp_volume[df_csp_volume['Resource description']!='Pack - Administration CSP']
df_csp_volume=df_csp_volume[df_csp_volume['Sub Department'].isin(['Packaging 1','Packaging 2','Packaging 3','Packaging 4','Packaging 5', 'Packaging DL'])]
df_csp_volume=df_csp_volume.groupby(['Sub Department','Resource','Material'],as_index=False)['Volume PLAC'].mean()
df_csp_volume.rename(columns={'Volume PLAC':'Unique_Volume_PLAC'}, inplace = True)
df_csp_volume.drop_duplicates(subset='Material',inplace=True)
df_capacity=df_capacity.merge(df_csp_volume,how='left',left_on=['Material','Sub Department','Resource'],right_on=['Material','Sub Department','Resource'])
df_capacity['Unique_Volume_PLAC']=np.where(df_capacity['Sub Department'].isin(['Packaging 1','Packaging 2','Packaging 3','Packaging 4','Packaging 5','Packaging DL']),df_capacity['Unique_Volume_PLAC'],np.where(df_capacity['Volume ind']=='Y',df_capacity['Volume PLAC'],0))
df_capacity['Unique_Volume_PLAC']=df_capacity['Unique_Volume_PLAC'].fillna(0)
df_capacity['Unique_Volume_PLAC']=np.where(df_capacity['Material Description'].str.contains('DEV-ENBREL'),0,df_capacity['Unique_Volume_PLAC'])
df_capacity['Unique_Volume_PLAC']=np.where((df_capacity['Material Description'].str.contains('POUCH'))&(df_capacity['Resource']=='W00HP04'),0,df_capacity['Unique_Volume_PLAC'])
df_capacity['Unique_Volume_PLAC']=np.where((df_capacity['Material Description'].str.contains(' BOX '))&(df_capacity['Resource'].isin(['W28IP4B','W28IP4A'])),0,df_capacity['Unique_Volume_PLAC'])
df_capacity['Unique_Volume_PLAC']=np.where((df_capacity['Material Description'].str.contains(' UNO PEN'))&(df_capacity['Resource'].isin(['W28IP4B','W28IP4A'])),0,df_capacity['Unique_Volume_PLAC'])
df_capacity['Volume ind']=np.where(df_capacity['Material Description'].str.contains(' HERID'),'N',df_capacity['Volume ind'])
df_capacity['FillPack ind']=np.where(df_capacity['Material Description'].str.contains(' HERID'),'Other',df_capacity['FillPack ind'])
df_capacity['Volume ind']=np.where(df_capacity['Material Description'].str.contains(' HERID'),'N',df_capacity['Volume ind'])
df_capacity['FillPack ind']=np.where(df_capacity['Material Description'].str.contains(' HERID'),'Other',df_capacity['FillPack ind'])
df_capacity['Form_Batches']=np.where((df_capacity['Presentation'].isin(['AOV','TCC','CART']))&(df_capacity['FillPack ind']=='Formulation'),df_capacity['Batches']*2,df_capacity['Batches'])
df_capacity['Form_Batches']=np.where((df_capacity['FillPack ind']=='Formulation')&(df_capacity['Profit center']=='PREVENAR')&(df_capacity['Material Description'].str.contains('MDV')),df_capacity['Batches']*2,df_capacity['Form_Batches'])
df_capacity['VOLUME X1000']=df_capacity['Volume']/1000
df_capacity['lotxlotsize']=df_capacity['Batches']*df_capacity['Lotsize']
df_capacity['NBC']=0
df_capacity['NBC Total']=0
df_capacity['Versie']='?'
df_capacity['Year']='?'
df_capacity=df_capacity[['Versie','Year','Department','Sub Department','Resource','Resource description','Volume ind','FillPack ind','Material','Material Description','Family','Presentation','Profit center','Volume','Volume PLAC','Unique_Volume_PLAC','Volume Sales Unit','Base Quantity','Lab Hrs','Mach Hrs','Batches','Form_Batches','Lotsize','Multi PLAC','Multi Sales Unit','Recipe','NBC Total','NBC','VOLUME X1000','lotxlotsize']]

wb.sheets('Capacity report').cells.clear
wb.sheets('Capacity report').range('A1').options(index=False).value=df_capacity


#######SAP UPLOAD#######
df_upload_sap = df_volume_items_bom.copy()
df_upload_sap = df_upload_sap[['MATERIAL_KEY', 'Lotsize', 'Volume_mult_ltsz', 'Nr_of_orders']]
expanded_rows = []
errors = []
for _, row in df_upload_sap.iterrows():
       try:
           #Create whole number
           num_orders = int(np.floor(row['Nr_of_orders']))
           #Fractional part
           fractional_order = row['Nr_of_orders'] - num_orders
           #calculate split proportionally
           order_volume = row['Volume_mult_ltsz'] / row['Nr_of_orders'] 
           for _ in range(num_orders): 
               expanded_rows.append([row['MATERIAL_KEY'], row['Lotsize'], order_volume])
           if fractional_order >0:
                expanded_rows.append([row['MATERIAL_KEY'], row['Lotsize'], order_volume * fractional_order])
                       
       except:
           errors.append([row['MATERIAL_KEY'], row['Lotsize'], row['Volume_mult_ltsz'], row['Nr_of_orders']])
           pass

df_upload_final = pd.DataFrame(data=expanded_rows, columns=['Product','Lotsize', 'Rec/ReqQty'])
df_upload_final['Target'] = 'BE37'
df_upload_final['PP-firmed'] = 'X'
df_upload_final['Avail/ReqD'] = '29.11.2026'
df_upload_final = df_upload_final[['Product', 'Target', 'Rec/ReqQty', 'Avail/ReqD', 'PP-firmed']]
wb.sheets('Sap_upload').cells.clear()
wb.sheets('Sap_upload').range('A1').options(index=False).value=df_upload_final

#############modify Finance cap report##########

df_fin=import_sheet('Cap_Fin')
if not df_fin.empty:
    df_fin.rename(columns={'Annual Order Qty':'Volume', '    Base Qty':'Base Quantity','Ann.Labor hours':'Lab Hrs',' Ann.Machine hours':'Mach Hrs','NB Lots':'Batches'},inplace=True)
    df_fin=df_fin[['Material','Material Description','Recipe','Base Quantity','Resource','Volume','Mach Hrs','Lab Hrs','Batches']]
    df_master.rename(columns={'MATERIAL_KEY':'Material'},inplace=True)
    df_pres.rename(columns={'MATERIAL_KEY':'Material'},inplace=True)
    df_family.rename(columns={'MATERIAL_KEY':'Material'},inplace=True)
    df_recipes.rename(columns={'MATERIAL_KEY':'Material','RECIPE':'Recipe'},inplace=True)
    
    df_fin=df_fin.merge(df_master[['Material','MPG_DESC']],left_on=['Material'],right_on=['Material'])
    df_fin=df_fin.merge(df_profit,how='left',left_on=['MPG_DESC'],right_on=['MPG_DESC'])
    df_fin=df_fin.merge(df_pres,how='left',left_on=['Material'],right_on=['Material'])
    df_fin=df_fin.merge(df_ref,how='left',left_on=['Resource'],right_on=['Resource'])
    df_fin=df_fin.merge(df_family,how='left',left_on=['Material'],right_on=['Material'])
    df_fin=df_fin.merge(df_recipes[['Material','MULTIPLICITY_X_UNITS_IN_PACK','QUANTITY_X_MULTIPLICITY']].drop_duplicates(),how='left',left_on=['Material'],right_on=['Material'])
    df_fin['QUANTITY_X_MULTIPLICITY']=df_fin['QUANTITY_X_MULTIPLICITY'].fillna(1)
    df_fin['MULTIPLICITY_X_UNITS_IN_PACK']=df_fin['MULTIPLICITY_X_UNITS_IN_PACK'].fillna(1)
    df_fin.rename(columns={'QUANTITY_X_MULTIPLICITY':'Multi PLAC','MULTIPLICITY_X_UNITS_IN_PACK':'Multi Sales Unit','PRESENTATION':'Presentation','FAMILY':'Family'},inplace=True)
    df_fin['Volume PLAC']=df_fin['Volume']*df_fin['Multi PLAC'].astype(float)
    df_fin['Volume Sales Unit']=df_fin['Volume']*df_fin['Multi Sales Unit'].astype(float)
    df_fin['Lotsize']=df_fin['Volume']/df_fin['Batches'].astype(float)
    df_csp_volume=df_fin[df_fin['Volume ind']=='Y']
    df_csp_volume=df_csp_volume[df_csp_volume['Resource description']!='Pack - Administration CSP']
    df_csp_volume=df_csp_volume[df_csp_volume['Sub Department'].isin(['Packaging 1','Packaging 2','Packaging 3','Packaging 4','Packaging 5', 'Packaging DL'])]
    df_csp_volume=df_csp_volume.groupby(['Sub Department','Resource','Material'],as_index=False)['Volume PLAC'].mean()
    df_csp_volume.rename(columns={'Volume PLAC':'Unique_Volume_PLAC'}, inplace = True)
    df_csp_volume.drop_duplicates(subset='Material',inplace=True)
    df_fin=df_fin.merge(df_csp_volume,how='left',left_on=['Material','Sub Department','Resource'],right_on=['Material','Sub Department','Resource'])
    df_fin['Unique_Volume_PLAC']=np.where(df_fin['Sub Department'].isin(['Packaging 1','Packaging 2','Packaging 3','Packaging 4','Packaging 5','Packaging DL']),df_fin['Unique_Volume_PLAC'],np.where(df_fin['Volume ind']=='Y',df_fin['Volume PLAC'],0))
    df_fin['Unique_Volume_PLAC']=df_fin['Unique_Volume_PLAC'].fillna(0)
    df_fin['Unique_Volume_PLAC']=np.where(df_fin['Material Description'].str.contains('DEV-ENBREL'),0,df_fin['Unique_Volume_PLAC'])
    df_fin['Unique_Volume_PLAC']=np.where((df_fin['Material Description'].str.contains('POUCH'))&(df_fin['Resource']=='W00HP04'),0,df_fin['Unique_Volume_PLAC'])
    df_fin['Unique_Volume_PLAC']=np.where((df_fin['Material Description'].str.contains(' BOX '))&(df_fin['Resource'].isin(['W28IP4B','W28IP4A'])),0,df_fin['Unique_Volume_PLAC'])
    df_fin['Unique_Volume_PLAC']=np.where((df_fin['Material Description'].str.contains(' UNO PEN'))&(df_fin['Resource'].isin(['W28IP4B','W28IP4A'])),0,df_fin['Unique_Volume_PLAC'])
    df_fin['Volume ind']=np.where(df_fin['Material Description'].str.contains(' HERID'),'N',df_fin['Volume ind'])
    df_fin['FillPack ind']=np.where(df_fin['Material Description'].str.contains(' HERID'),'Other',df_fin['FillPack ind'])
    df_fin['Volume ind']=np.where(df_fin['Material Description'].str.contains(' HERID'),'N',df_fin['Volume ind'])
    df_fin['FillPack ind']=np.where(df_fin['Material Description'].str.contains(' HERID'),'Other',df_fin['FillPack ind'])
    df_fin['Versie']='?'
    df_fin['Year']='?'
    df_fin['Form_Batches']=np.where((df_fin['Presentation'].isin(['AOV','TCC','CART']))&(df_fin['FillPack ind']=='Formulation'),df_fin['Batches']*2,df_fin['Batches'])
    df_fin['Form_Batches']=np.where((df_fin['FillPack ind']=='Formulation')&(df_fin['Profit center']=='PREVENAR')&(df_fin['Material Description'].str.contains('MDV')),df_fin['Batches']*2,df_fin['Form_Batches'])
    df_fin['NBC']=0
    df_fin['NBC Total']=0
    df_fin['VOLUME X1000']=df_fin['Volume']/1000
    df_fin['lotxlotsize']=df_fin['Batches']*df_fin['Lotsize']
    df_fin=df_fin[['Versie','Year','Department','Sub Department','Resource','Resource description','Volume ind','FillPack ind','Material','Material Description','Family','Presentation','Profit center','Volume','Volume PLAC','Unique_Volume_PLAC','Volume Sales Unit','Base Quantity','Lab Hrs','Mach Hrs','Batches','Form_Batches','Lotsize','Multi PLAC','Multi Sales Unit','Recipe','NBC Total','NBC','VOLUME X1000','lotxlotsize']]
    df_fin=df_fin[~df_fin['Resource'].isin(['Q50AO01','Q60AO01','Q50AO02','Q50AO03','Q50AO04','Q50AO05','M2MDUMMY'])]
    wb.sheets('Capacity report finance rwk').cells.clear
    wb.sheets('Capacity report finance rwk').range('A1').options(index=False).value=df_fin
