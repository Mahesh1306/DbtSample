WITH ImportData as (SELECT Top 2500
 ICR.ID 
,ICR.SourceSystem 
,ICR.EplusLookupKey 
,ICR.Date_Added 
,ICR.Dnis 
,ICR.Ani 
,ICR.Call_Duration
,ICR.Transfer_To_Number 
,trim(ICR.SourceBreakdownName) as SourceBreakdownName 
,ICR.StoreID as SN
,ICR.CreatedDateTime 
,R.ID as RID
,NPM.NationalProgramID as NPID
,NP.ProgramIdentifier as NPName
,RP.StartDate as StartDate
,RP.EndDate as EndDate
,RP.ID as RPID
,RP.BillingTypeID as RPBTID
,V.Name as Vendor
FROM ImportCommonRaw ICR 
LEFT JOIN NationalProgramMapping NPM on NPM.Name=ICR.SourceBreakdownName
LEFT JOIN NationalProgram NP on NP.ID=NPM.NationalProgramID
LEFT JOIN Vendor V on V.ID=NP.VendorID
LEFT JOIN Retailer R on R.storenumber=ICR.StoreID 
FULL JOIN RetailerProgram RP on RP.RetailerID=R.ID and RP.ProgramID=NPM.NationalProgramID
WHERE ICR.RecordStatus = 'P' and ICR.SourceSystem = 'Invoca'  order by ICR.Date_Added asc)

ImportDataTransform1 as (
    Select *,
    GETDATE() AS O_HEADERTIMESTAMP,
    ImportData.SN AS StoreID,
    (CAST(ImportData.Date_Added) AS Floor) AS ItemDate FROM ImportData
)

ImportDataTransform2 AS(
    Select *, 
    Concat(Concat(Concat(Concat(ImportDataTransform1.SourceSystem, ' '),  TO_CHAR(ImportDataTransform1.Date_Added, 'Month' )),' '),TO_CHAR(ImportDataTransform1.Date_Added,'YYYY')) AS O_HEADERNAME,
    1 AS O_ImportType,
    GETDATE() AS O_CREATEDAT,
    'System Import' AS O_CREATEDBY FROM ImportDataTransform1
)

ImportDataTransform3 AS(
    SELECT *,
    CONCAT(CONCAT(CONCAT(ImportDataTransform2.StoreID,CONCAT('_',ImportDataTransform2.NPName)),CONCAT('_', REG_REPLACE(ImportDataTransform2.Ani,'[^a-z0-9A-Z]',''))), CONCAT('_',CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(ltrim(SUBSTR(ImportDataTransform2.Date_Added,0,2),0),'/'),ltrim(SUBSTR(ImportDataTransform2.Date_Added,4,2),0)),'/'),ltrim(SUBSTR(ImportDataTransform2.Date_Added,7,4),0)),' '),ltrim(SUBSTR(TO_CHAR(ImportDataTransform2.Date_Added,'MM/DD/YYYY HH12:MI:SS AM'),12,11),0)))) AS O_UID,
    TO_CHAR(ImportDataTransform2.Date_Added, 'MM/DD/YYYY HH12:MI:SS AM' ) AS O_ITEMDATE,
    TRUNC(ImportDataTransform2.Date_Added,'MONTH') AS O_SixtyDayDate,
    0 AS O_ItemValue FROM ImportDataTransform2
)

Lookup1 AS(
    Select top 1 id.id as M_ID,
id.BillableFlag as M_BF,
ISNULL(overridereason, null) AS M_OR,
id.ItemDate as M_Date,
id.UID as M_UID from ImportDetail id WHERE UID = ImportDataTransform3.O_UID and id.ActiveFlag = 1and id.itemdate > dateadd(day,-120,getdate())
)

Lookup2 AS(
    Select top 1
id.RetailerProgramID as SixtyDay_RetailerprogramId, 
id.ANI as SixtyDay_ANI, 
id.ItemDate as SixtyDay_ItemDate
from importdetail id 
where id.RetailerProgramID = ImportDataTransform3.RPID and id.ItemDate > ImportDataTransform3.O_SixtyDayDate and id.Ani = ImportDataTransform3.Ani
and id.ActiveFlag = 1 and id.BillableFlag = 1 and id.RetailerProgramID is not null and id.itemdate > dateadd(day,-120,getdate())
)

ImportDataTransform4 AS(
    Select *,
    IIF(to_date(substr(ImportDataTransform3.O_ITEMDATE, 1, 10), 'MM/DD/YYYY') >= ImportDataTransform3.StartDate AND to_date(substr(ImportDataTransform3.O_ITEMDATE, 1, 10), 'MM/DD/YYYY') <= ImportDataTransform3.EndDate 
    and ImportDataTransform3.RPBTID <> 6, 1, 0) AS O_BILLABLEFLAG,
    IIF(ISNULL(ImportDataTransform3.O_UID), 0, 1) AS O_Row_returned_id,
    0 AS O_ACITVEFLAG FROM ImportDataTransform3
)

ImportDataTransform5 AS(
    Select *,
    GETDATE() AS O_TIMESTAMP,
    'C' AS O_PROCESSED FROM ImportDataTransform4
)

{% for value in ImportDataTransform5 %}
Update ImportCommonRaw set ProcessedDateTime = value.O_TIMESTAMP, RecordStatus = value.O_PROCESSED
WHERE ID = value.ID
 {% endfor %}




