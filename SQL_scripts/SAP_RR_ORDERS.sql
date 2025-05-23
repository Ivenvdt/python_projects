WITH SCHEDULE_RECEIPTS AS 
(SELECT
'SCHEDULE_RECEIPTS' "DATA_SOURCE",
SCHEDULED_RECEIPT.ORDER_TYPE "SUPPLY_TYPE",
SCHEDULED_RECEIPT.PO_STATUS "PO_STATUS",
SCHEDULED_RECEIPT.MATERIAL_KEY "MATERIAL_KEY",
SCHEDULED_RECEIPT.PART_SOURCE "PART_SOURCE", --=CORRECT PRODUCTION VERSION IN RR
SCHEDULED_RECEIPT.PRODUCTION_VERSION "PRODUCTION_VERSION",
SPLIT_PART(SCHEDULED_RECEIPT.ROUTING, '_',1) "ROUTING",
--CONSTRAINT AVAILABLE IN RR, HOWEVER NOT CONSISTENT, I WILL USE S/4 LINKAGE THROUGH ROUTING TO GET THE CONSTRAINT/LINE INFORMATION
--SCHEDULED_RECEIPT."CONSTRAINT"
SCHEDULED_RECEIPT.ALTERNATIVE_BOM "ALTERNATIVE_BOM",
--CASE WHEN SCHEDULED_RECEIPT.CTP_AVAILABLE_START_DATE = '2099-12-31' THEN SCHEDULED_RECEIPT.NETTING_START_DATE ELSE SCHEDULED_RECEIPT.CTP_AVAILABLE_START_DATE END "START_DATE",
--ISSUES WITH UNDERLYING AVAILABILITIES TO PUSH OUT THE CTP DOCK DATE
--CASE WHEN SCHEDULED_RECEIPT.CTP_DOCK_DATE = '2099-12-31' THEN SCHEDULED_RECEIPT.NETTING_DOCK_DATE ELSE SCHEDULED_RECEIPT.CTP_DOCK_DATE END "END_DATE",
SCHEDULED_RECEIPT.CTP_AVAILABLE_START_DATE "START_DATE",
SCHEDULED_RECEIPT.CTP_DOCK_DATE "END_DATE",
SCHEDULED_RECEIPT.NETTING_START_DATE "UNCONSTRAINT_START_DATE",
SCHEDULED_RECEIPT.NETTING_DOCK_DATE "UNCONSTRAINT_END_DATE",
SCHEDULED_RECEIPT.QUANTITY "QUANTITY",
SCHEDULED_RECEIPT.PO_NUMBER "ORDER_NO"
FROM PGSUDH_DL_PROD.PGSUDH_CDM.RR_SCHEDULED_RECEIPT SCHEDULED_RECEIPT
WHERE 
SCHEDULED_RECEIPT.PLANT_KEY ='BE37'
AND SCHEDULED_RECEIPT.ORDER_TYPE IN ('FirmPlannedWO','WO')
),
PLANNED_ORDERS AS (
SELECT
'PLANNED_ORDER' "DATA_SOURCE",
PLANNED_ORDERS.PLANNED_ORDER_TYPE "SUPPLY_TYPE",
'N/A' "PO_STATUS",
PLANNED_ORDERS.material_key "MATERIAL_KEY",
PLANNED_ORDERS.PART_SOURCE "PART_SOURCE", --=CORRECT PRODUCTION VERSION IN RR
SPLIT_PART(PLANNED_ORDERS.PART_SOURCE,'_',1) "PRODUCTION_VERSION",
SPLIT_PART(PART_SOURCE.ROUTING ,'_',1) "ROUTING",
PART_SOURCE.ALTERNATIVE_BOM "ALTERNATIVE_BOM",
--CASE WHEN PLANNED_ORDERS.CTP_AVAILABLE_START_DATE = '2099-12-31' THEN PLANNED_ORDERS.NETTING_START_DATE ELSE PLANNED_ORDERS.CTP_AVAILABLE_START_DATE END "START_DATE",
--ISSUES WITH UNDERLYING AVAILABILITIES TO PUSH OUT THE CTP DOCK DATE
--CASE WHEN PLANNED_ORDERS.CTP_DOCK_DATE = '2099-12-31' THEN PLANNED_ORDERS.NETTING_DOCK_DATE ELSE PLANNED_ORDERS.CTP_DOCK_DATE END "END_DATE",
PLANNED_ORDERS.CTP_AVAILABLE_START_DATE "START_DATE",
PLANNED_ORDERS.CTP_DOCK_DATE "END_DATE",
PLANNED_ORDERS.NETTING_START_DATE "UNCONSTRAINT_START_DATE",
PLANNED_ORDERS.NETTING_DOCK_DATE "UNCONSTRAINT_END_DATE",
PLANNED_ORDERS.ON_TIME_QUANTITY "QUANTITY",
PLANNED_ORDERS.PLANNED_ORDER "ORDER_NO"
FROM PGSUDH_DL_PROD.PGSUDH_CDM.RR_PLANNED_ORDERS PLANNED_ORDERS
LEFT JOIN PGSUDH_DL_PROD.PGSUDH_CDM.RR_PART_SOURCE PART_SOURCE
ON PLANNED_ORDERS.TRANSFER_LOCATION_KEY = PART_SOURCE.LOCATION_KEY
AND PLANNED_ORDERS.PART_SOURCE = PART_SOURCE.BASE_KEY
AND PLANNED_ORDERS.MATERIAL_KEY = PART_SOURCE.MATERIAL_KEY
LEFT JOIN PGSUDH_DL_PROD.PGSUDH_CDM.ECC_PRODUCTION_VERSION AS PRODUCTION_VERSION 
ON PLANNED_ORDERS.material_key = PRODUCTION_VERSION.material_key 
AND SPLIT_PART(PLANNED_ORDERS.PART_SOURCE,'_',1) = PRODUCTION_VERSION.production_version
AND PLANNED_ORDERS.TRANSFER_LOCATION_KEY= PRODUCTION_VERSION.PLANT_KEY
WHERE 
PLANNED_ORDERS.TRANSFER_LOCATION_KEY = 'BE37'
AND PLANNED_ORDERS.PART_SOURCE_TYPE='Make'
),
MATERIAL_MOVEMENTS AS(
SELECT
'ACTUALS' "DATA_SOURCE",
'RECEIPT_PRODUCTION_ORDER' "SUPPLY_TYPE",
'N/A' "PO_STATUS",
MATERIAL_MOVEMENTS.MATERIAL_KEY "MATERIAL_KEY",
PROCESS_ORDERS.PRODUCTION_VERSION "PART_SOURCE",
PROCESS_ORDERS.PRODUCTION_VERSION "PRODUCTION_VERSION",
PROCESS_ORDERS.RECIPE "ROUTING",
'N/A' "ALTERNATIVE_BOM",
MIN(MATERIAL_MOVEMENTS.POSTING_DATE) "START_DATE",
MAX(MATERIAL_MOVEMENTS.POSTING_DATE) "END_DATE",
MIN(MATERIAL_MOVEMENTS.POSTING_DATE) "UNCONSTRAINT_START_DATE",
MAX(MATERIAL_MOVEMENTS.POSTING_DATE) "UNCONSTRAINT_END_DATE",
SUM(CASE WHEN  MOVEMENT_TYPE_KEY = '102' THEN -MM_QUANTITY_SIGN ELSE MM_QUANTITY_SIGN END) "QUANTITY",
MATERIAL_MOVEMENTS.PRODUCTION_ORDER "ORDER_NO"
FROM 
PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_MOVEMENTS MATERIAL_MOVEMENTS
LEFT JOIN 
	(SELECT DISTINCT
	PROCESS_ORDER.PLANT_KEY "PLANT_KEY",
	PROCESS_ORDER.material_key "MATERIAL_KEY", 
	PROCESS_ORDER.production_version "PRODUCTION_VERSION",  
	PRODUCTION_VERSION.tasklist_group "RECIPE", 
	PROCESS_ORDER.process_order "PROCESS_ORDER"
	FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_PROCESS_ORDER AS PROCESS_ORDER
	LEFT JOIN PGSUDH_DL_PROD.PGSUDH_CDM.ECC_PRODUCTION_VERSION AS  PRODUCTION_VERSION 
	ON PROCESS_ORDER.material_key = PRODUCTION_VERSION.material_key 
	and PROCESS_ORDER.production_version = PRODUCTION_VERSION.production_version
	and PRODUCTION_VERSION.plant_key ='BE37'
	WHERE PROCESS_ORDER.PLANT_KEY ='BE37'
	)PROCESS_ORDERS
ON MATERIAL_MOVEMENTS.PLANT_KEY = PROCESS_ORDERS.PLANT_KEY
AND MATERIAL_MOVEMENTS.MATERIAL_KEY = PROCESS_ORDERS.MATERIAL_KEY
AND MATERIAL_MOVEMENTS.PRODUCTION_ORDER = PROCESS_ORDERS.PROCESS_ORDER
--CREATE A TABLE TO FILTER OUT ORDERS THAT ALREADY HAVE A RECEIPT HOWEVER STILL HAVE Work Order Status Release, IN THE MODEL WE WILL GET THIS DATA OUT OF TABLE SCHEDULE RECEIPT INSTEAD OF THE MATERIAL MOVEMENTS
LEFT JOIN 
	(SELECT DISTINCT 
	SCHEDULED_RECEIPT.PO_NUMBER
	FROM PGSUDH_DL_PROD.PGSUDH_CDM.RR_SCHEDULED_RECEIPT SCHEDULED_RECEIPT
	WHERE 
	SCHEDULED_RECEIPT.PLANT_KEY ='BE37'
	AND SCHEDULED_RECEIPT.ORDER_TYPE IN ('WO')
	AND SCHEDULED_RECEIPT.PO_STATUS = 'Released'
	)RELEASE_STATUS_BATCHES
ON MATERIAL_MOVEMENTS.BATCH = RELEASE_STATUS_BATCHES.PO_NUMBER
WHERE 
MATERIAL_MOVEMENTS.PLANT_KEY = 'BE37'
AND (MATERIAL_MOVEMENTS.MATERIAL_KEY LIKE ('H%') OR MATERIAL_MOVEMENTS.MATERIAL_KEY LIKE ('F%'))
AND MATERIAL_MOVEMENTS.MOVEMENT_TYPE_KEY in ('101', '102')
--FILTER OUT BATCHES THAT DO NO HAVE A RELEASE STATUS IN SCHEDULE RECEIPT TABLE
AND RELEASE_STATUS_BATCHES.PO_NUMBER IS NULL
GROUP BY 
MATERIAL_MOVEMENTS.MATERIAL_KEY,
MATERIAL_MOVEMENTS.PRODUCTION_ORDER,
PROCESS_ORDERS.PRODUCTION_VERSION,
PROCESS_ORDERS.RECIPE
),
/*PRODUCTION_LINE AS (
SELECT DISTINCT 
RECIPE_TASKLIST_OPERATION.BUSINESS_INDICATOR_GROUP "RECIPE" ,
RECIPE_TASKLIST_OPERATION.WORK_CENTER_RESOURCE AS "WORK_CENTER", 
WORKCENTER.WORKCENTER_DESC "PRODUCTION_LINE"
FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_WORKCENTER WORKCENTER
INNER JOIN (SELECT DISTINCT PLANT_KEY,BUSINESS_INDICATOR_GROUP,WORK_CENTER_RESOURCE 
            FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_RECIPE_TASKLIST_OPERATION 
            WHERE PLANT_KEY='BE37' --AND WORK_CENTER_RESOURCE LIKE '%W28H%'
            --99 is finance receipe --> to exclude
			AND GROUP_COUNTER != '99'
			-- --DELETION FLAG X REMOVE, only NULL and X in this field
			AND DELETION_INDICATOR_ALTBOM IS NULL
            ) RECIPE_TASKLIST_OPERATION 
ON RECIPE_TASKLIST_OPERATION.WORK_CENTER_RESOURCE = WORKCENTER.WORKCENTER_KEY
AND WORKCENTER.plant_key='BE37'  
WHERE (RECIPE_TASKLIST_OPERATION.plant_key='BE37'
--Code in resource discriptie hebben logica om te linken naar vullijn of packaging lijn
--W28IP01 = Handwork line and W28FI01 Assembly control line
AND (RECIPE_TASKLIST_OPERATION.WORK_CENTER_resource in ('W28FI01', 'W28IP01')
OR SUBSTRING(RECIPE_TASKLIST_OPERATION.WORK_CENTER_resource,4,1) IN ('D','H') and SUBSTRING(RECIPE_TASKLIST_OPERATION.WORK_CENTER_resource,5,1)  IN ('B','D','G','U','W','Y','P')) )
),
*/
PRODUCTION_LINES AS (
--USE THIS TABLE TO HIGHLIGHT MULTIPLE PRODUCTION LINES
SELECT DISTINCT 
RECIPE_TASKLIST_OPERATION.BUSINESS_INDICATOR_GROUP "RECIPE" ,
LISTAGG(RECIPE_TASKLIST_OPERATION.WORK_CENTER_RESOURCE,' // ') WITHIN GROUP(ORDER BY RECIPE_TASKLIST_OPERATION.WORK_CENTER_RESOURCE) AS "WORKCENTER",
LISTAGG(WORKCENTER.WORKCENTER_DESC,' // ') WITHIN GROUP(ORDER BY RECIPE_TASKLIST_OPERATION.WORK_CENTER_RESOURCE) AS "WORKCENTER_NAME"
FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_WORKCENTER WORKCENTER
INNER JOIN (SELECT DISTINCT PLANT_KEY,BUSINESS_INDICATOR_GROUP,WORK_CENTER_RESOURCE 
            FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_RECIPE_TASKLIST_OPERATION 
            WHERE PLANT_KEY='BE37' --AND WORK_CENTER_RESOURCE LIKE '%W28H%'
            --99 is finance receipe --> to exclude
			AND GROUP_COUNTER != '99'
			-- --DELETION FLAG X REMOVE, only NULL and X in this field
			AND DELETION_INDICATOR_ALTBOM IS NULL
            ) RECIPE_TASKLIST_OPERATION 
ON RECIPE_TASKLIST_OPERATION.WORK_CENTER_RESOURCE = WORKCENTER.WORKCENTER_KEY
AND WORKCENTER.plant_key='BE37'  
WHERE (RECIPE_TASKLIST_OPERATION.plant_key='BE37'
--Code in resource discriptie hebben logica om te linken naar vullijn of packaging lijn
--W28IP01 = Handwork line and W28FI01 = Assembly control line, W28IP4B= HW4B - Reguliere handwerk orders, W28IP01 = Pack - Hand Work
AND (RECIPE_TASKLIST_OPERATION.WORK_CENTER_resource in ('W28FI01', 'W28IP01', 'W28IP4B', 'W28IP01')
OR SUBSTRING(RECIPE_TASKLIST_OPERATION.WORK_CENTER_resource,4,1) IN ('D','H') and SUBSTRING(RECIPE_TASKLIST_OPERATION.WORK_CENTER_resource,5,1)  IN ('B','D','G','U','W','Y','P')) )
GROUP BY 
RECIPE_TASKLIST_OPERATION.BUSINESS_INDICATOR_GROUP
),
-- PAC_UOM IS SAP EA *multipliciteit * AMOUNT units in pack (product + diluent)
PAC_UOM AS (
SELECT 
MATERIAL_UOM_RT.MATERIAL_KEY,
MATERIAL_UOM_RT.DENOMINATOR_BASE_UOM
FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_UOM_RT MATERIAL_UOM_RT
WHERE MATERIAL_UOM_RT.ALTERNATE_UNIT_OF_MEASURE ='PAC'
),
--UN_UOM IS SAP EA * MULTIPLICITY
UN_UOM AS (
SELECT 
MATERIAL_UOM_RT.MATERIAL_KEY,
MATERIAL_UOM_RT.DENOMINATOR_BASE_UOM
FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_UOM_RT MATERIAL_UOM_RT
WHERE MATERIAL_UOM_RT.ALTERNATE_UNIT_OF_MEASURE ='UN'
),
MATERIAL_MASTER AS 
(
SELECT
MATERIAL_MASTER.MATERIAL_KEY "MATERIAL_KEY",
MATERIAL_MASTER.material_desc "ITEM_DESCRIPTION", 
MATERIAL_PLANT.plant_procurement_type "PLANT_PROCUREMENT_TYPE",
CASE WHEN MATERIAL_MASTER.material_type_key = 'HALB' AND MATERIAL_PLANT.MATERIAL_SOURCING_FLAG = '7' THEN 'SF4S'
ELSE MATERIAL_MASTER.material_type_key END "MATERIAL_TYPE_KEY",
CASE WHEN MATERIAL_PLANT.mpg_desc IS NULL THEN 'PUURS' ELSE UPPER(MATERIAL_PLANT.mpg_desc) END "MPG_DESC",
CASE WHEN MATERIAL_PLANT.mpg_desc IS NULL THEN '2479110' ELSE MATERIAL_PLANT.FDM_MPG END "MPG_CODE",
MATERIAL_MASTER.MATERIAL_CATEGORY,
MATERIAL_MASTER.MATERIAL_CATEGORY_TEXT,
MATERIAL_MASTER.PRESENTATION_CHARACTERISTIC,
MATERIAL_MASTER.PC1_IND "BUSINESS",
MATERIAL_PLANT.LOT_SIZE,
MATERIAL_PLANT.LOT_SIZE_DESC,
MATERIAL_PLANT.MARKET_CODE,
MATERIAL_PLANT.MIN_LOT_SIZE,
MATERIAL_PLANT.MRP_CONTROLLER
FROM PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_MASTER as MATERIAL_MASTER
INNER JOIN PGSUDH_DL_PROD.PGSUDH_CDM.ECC_MATERIAL_PLANT as MATERIAL_PLANT 
ON MATERIAL_MASTER.material_key = MATERIAL_PLANT.material_key 
AND MATERIAL_PLANT.plant_key ='BE37'
),
FISCAL_PERIOD AS
(
SELECT
DATE_DIM.DATE_SQL "DATE",
DATE_DIM.FISC_CURR_YEAR "FISCAL_YEAR",
DATE_DIM.FISC_CURR_PER "FISCAL_PERIOD"
FROM PGSUDH_DL_PROD.PGSUDH_CDM.DATE_DIM DATE_DIM
WHERE 
CALENDAR_VARIANT = 'Z2'
--AND YEAROFWEEKISO(DATE_SQL) BETWEEN YEAROFWEEKISO(CURRENT_DATE())-1 AND YEAROFWEEKISO(CURRENT_DATE())+2
ORDER BY 
DATE_SQL ASC
)
SELECT DISTINCT 
ORDERS."DATA_SOURCE" "DATA_SOURCE",
ORDERS."SUPPLY_TYPE" "SUPPLY_TYPE",
ORDERS."PO_STATUS" "PO_STATUS",
ORDERS."ORDER_NO" "ORDER_NO",
ORDERS.MATERIAL_KEY "MATERIAL_KEY",
ORDERS."PART_SOURCE" "PART_SOURCE",
ORDERS."PRODUCTION_VERSION" "PRODUCTION_VERSION",
ORDERS."ROUTING" "ROUTING",
ORDERS."ALTERNATIVE_BOM" "ALTERNATIVE_BOM",
PRODUCTION_LINES."WORKCENTER" "WORKCENTER",
PRODUCTION_LINES."WORKCENTER_NAME" "WORKCENTER_NAME",
ORDERS.END_DATE "DATE",
FISCAL_PERIOD_CONSTRAINT."FISCAL_YEAR",
FISCAL_PERIOD_CONSTRAINT."FISCAL_PERIOD",
ORDERS.UNCONSTRAINT_END_DATE "UNCONSTRAINT_END_DATE",
FISCAL_PERIOD_UNCONSTRAINT."FISCAL_YEAR" "FISCAL_YEAR_UNCONSTRAINT",
FISCAL_PERIOD_UNCONSTRAINT."FISCAL_PERIOD" "FISCAL_PERIOD_UNCONSTRAINT",
UN_UOM.DENOMINATOR_BASE_UOM "MULTIPLICITY", 
--PAC_UOM.DENOMINATOR_BASE_UOM "xxxxx",
CASE WHEN PAC_UOM.DENOMINATOR_BASE_UOM IS NULL THEN UN_UOM.DENOMINATOR_BASE_UOM ELSE PAC_UOM.DENOMINATOR_BASE_UOM END "MULTIPLICITY_x_UNITS_IN_PACK",
ORDERS."QUANTITY" "QUANTITY",
ORDERS."QUANTITY"*UN_UOM.DENOMINATOR_BASE_UOM "QUANTITY_x_MULTIPLICITY",
IFF(PAC_UOM.DENOMINATOR_BASE_UOM IS NULL, ORDERS."QUANTITY", ORDERS."QUANTITY" *PAC_UOM.DENOMINATOR_BASE_UOM) AS "QUANTITY_x_MULTIPLICITY_x_UNITS_IN_PACK"
FROM ( 
	SELECT*
	FROM 
	SCHEDULE_RECEIPTS
	UNION ALL
	SELECT*
	FROM 
	PLANNED_ORDERS
	UNION ALL
	SELECT*
	FROM
	MATERIAL_MOVEMENTS
	) AS ORDERS
--LEFT JOIN PRODUCTION_LINE AS PRODUCTION_LINE
--ON PRODUCTION_LINE.recipe = ORDERS.ROUTING
LEFT JOIN PRODUCTION_LINES AS PRODUCTION_LINES
ON PRODUCTION_LINES.recipe = ORDERS.ROUTING
LEFT JOIN PAC_UOM AS PAC_UOM
ON PAC_UOM.MATERIAL_KEY = ORDERS.MATERIAL_KEY
LEFT JOIN UN_UOM AS UN_UOM
ON UN_UOM.MATERIAL_KEY = ORDERS.MATERIAL_KEY
LEFT JOIN MATERIAL_MASTER AS MATERIAL_MASTER
ON MATERIAL_MASTER.MATERIAL_KEY = ORDERS.MATERIAL_KEY
--left join with constraint demand end date
LEFT JOIN FISCAL_PERIOD AS FISCAL_PERIOD_CONSTRAINT
ON ORDERS."END_DATE" = FISCAL_PERIOD_CONSTRAINT."DATE"
--left join with unconstraint demand end date
LEFT JOIN FISCAL_PERIOD AS FISCAL_PERIOD_UNCONSTRAINT
ON ORDERS."UNCONSTRAINT_END_DATE" = FISCAL_PERIOD_UNCONSTRAINT."DATE"
WHERE 
FISCAL_PERIOD_CONSTRAINT."FISCAL_YEAR" BETWEEN YEAR(CURRENT_DATE())-2 AND YEAR(CURRENT_DATE())+2
--REMOVE M2MDUMMY AND WORK CENTERS THAT DO NOT HAVE A MATCH WITH PRODUCTION LINE INFO
AND (PRODUCTION_LINES."WORKCENTER" NOT LIKE ('%M2MDUMMY%') AND PRODUCTION_LINES."WORKCENTER" IS NOT NULL)
AND ORDERS."QUANTITY" !=0
AND MATERIAL_MASTER."PLANT_PROCUREMENT_TYPE" = 'E'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'MF-%'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'MF %'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'CLIN-%'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'CLIN %'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'ET-%'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'ET %'
AND MATERIAL_MASTER."ITEM_DESCRIPTION" not like 'BUFFER %'
--REWORK ORDERS TO REMOVE
AND (LENGTH (ORDERS."ORDER_NO" )=7 AND (ENDSWITH(ORDERS."ORDER_NO",'X') OR ENDSWITH(ORDERS."ORDER_NO",'Y') OR ENDSWITH(ORDERS."ORDER_NO",'Z'))) = FALSE
--Filter out BOX operation of Genotropin on line W28IP4B | HW4B
AND NOT (PRODUCTION_LINES."WORKCENTER" = 'W28IP4B | HW4B' AND MATERIAL_MASTER."MPG_DESC" = 'GENOTROPIN' AND MATERIAL_MASTER."MATERIAL_CATEGORY" = 'SF-PP')
--Filter out ENBREL packaging operation on W28HP29 | Pack - Packline 29 Ass. Enbrel
AND NOT (PRODUCTION_LINES."WORKCENTER" = 'W28HP29 | Pack - Packline 29 Ass. Enbrel' AND MATERIAL_MASTER."MPG_DESC" = 'ENBREL SALES')
--Remove Pouching of Sayana
AND NOT ((PRODUCTION_LINES."WORKCENTER" LIKE 'W00HP03%'
		OR PRODUCTION_LINES."WORKCENTER" LIKE 'W00HP02%'
		OR PRODUCTION_LINES."WORKCENTER" LIKE 'W00HP04%')
	AND MATERIAL_MASTER."MPG_DESC" = 'SAYANA - ESTABLISHED PRODUCTS')
ORDER BY 
FISCAL_PERIOD_CONSTRAINT."FISCAL_YEAR" asc
;
