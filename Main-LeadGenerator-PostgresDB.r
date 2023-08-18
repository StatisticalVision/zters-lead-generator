# This is the source script for the Power BI  Lead Generator report to get modified Sales and Answered calls tables.
# It takes Answered calls and sales and attributes each sale to its latest previous call's paid lead source.

rm(list=ls(all=TRUE))
require(data.table)
require(rjson)
require(httr)
require(zoo)
# require(zipcode)
require(stringr)
require(DBI)
require(dotenv)

# Connection to the PostgreSQL DB ----------------------------------------------
db <- Sys.getenv("DB")  #Name of the data base
host_db <- Sys.getenv("HOST")  # service IP address 
db_port <- Sys.getenv("PORT")  # Server port
db_user <- Sys.getenv("USERNAME")
db_password <- Sys.getenv("PASSWORD")

keys = fread("C://Users//saina//Desktop//Prj1//ZTERS_STEP2//api_keys_saina.csv")
db <- 'postgres' 
host_db <- "internal-production.postgres.database.azure.com" 
db_port <- '5432'
db_user <- keys[`Application name`=="Postgres SQL DB", username]
db_password <- keys[`Application name`=="Postgres SQL DB",key]

con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password) 


#1. GET DATA ###################################################################
# Answered calls --------------
queryString <- "SELECT * FROM zters_data_backup.answered_phone_calls_bnkjica8i"
AllAnsweredCalls <- dbGetQuery(con, queryString)

queryString <- "SELECT * FROM zters_data_backup.answered_phone_calls_bnkjica8i_colnames"
AllAnsweredCalls_col <- dbGetQuery(con, queryString)

names(AllAnsweredCalls) <- AllAnsweredCalls_col$col_name
AllAnsweredCalls <- data.table(AllAnsweredCalls)
AllAnsweredCalls <- AllAnsweredCalls[,list(date_created,caller_id,number_customer_dialed,customer_qb_id,call_status,
                                           answering_agent,converted_to_customer)]


# Toilets -----------
queryString <- "SELECT * FROM zters_data_backup.toilet_tickets_bqv7bregq"
AllToilets <- dbGetQuery(con, queryString)

queryString <- "SELECT * FROM zters_data_backup.toilet_tickets_bqv7bregq_colnames"
AllToilets_cols <- dbGetQuery(con, queryString)

names(AllToilets) <- AllToilets_cols$col_name
names(AllToilets)
AllToilets <- data.table(AllToilets)
AllToilets <- AllToilets[,list(start_date_,related_customer,related_site,cancel,service_ticket_status,
                                 expected_customer_charges,expected_hauler_cost)]

# RollOffs ---------------
queryString <- "SELECT * FROM zters_data_backup.rolloff_tickets_bqv7bu5z4"
AllRollOffs <- dbGetQuery(con, queryString)

queryString <- "SELECT * FROM zters_data_backup.rolloff_tickets_bqv7bu5z4_colnames"
AllRollOffs_cols <- dbGetQuery(con, queryString)

names(AllRollOffs) <- AllRollOffs_cols$col_name
names(AllRollOffs)
AllRollOffs <- data.table(AllRollOffs)
AllRollOffs <- AllRollOffs[,list(start_date_,related_customer,related_site,cancel,service_ticket_status,
                                     expected_customer_charges,expected_hauler_cost)]


# Containers ---------------
queryString <- "SELECT * FROM zters_data_backup.storage_container_tickets_bqwdgzi8q"
AllContainers <- dbGetQuery(con, queryString)

queryString <- "SELECT * FROM zters_data_backup.storage_container_tickets_bqwdgzi8q_colnames"
AllContainers_cols <- dbGetQuery(con, queryString)

names(AllContainers) <- AllContainers_cols$col_name
names(AllContainers)
AllContainers <- data.table(AllContainers)
AllContainers <- AllContainers[,list(start_date_,related_customer,related_site,cancel,service_ticket_status,
                                     expected_customer_charges,expected_hauler_cost)]


# Fencing ------------------
queryString <- "SELECT * FROM zters_data_backup.fencing_tickets_bqv7b35hh"
AllFencing <- dbGetQuery(con, queryString)

queryString <- "SELECT * FROM zters_data_backup.fencing_tickets_bqv7b35hh_colnames"
AllFencing_cols <- dbGetQuery(con, queryString)

names(AllFencing) <- AllFencing_cols$col_name
names(AllFencing)
AllFencing <- data.table(AllFencing)
AllFencing <- AllFencing[,list(start_date_,related_customer,related_site,cancel,service_ticket_status,
                                     expected_customer_charges,expected_hauler_cost)]


#1.d Raw Data initial processing --------------------
AllRollOffs <- setDT(AllRollOffs)
AllToilets <- setDT(AllToilets)
AllContainers <- setDT(AllContainers)
AllFencing <- setDT(AllFencing)
AllAnsweredCalls <- setDT(AllAnsweredCalls)

AllDTs = list(AllToilets,AllRollOffs,AllFencing,AllContainers,AllAnsweredCalls)
names(AllDTs) = c("AllToilets","AllRollOffs","AllFencing","AllContainers","AllAnsweredCalls")
for(x in 1:length(AllDTs)){
  print(AllDTs[x])
  #x = 1
  if(names(AllDTs[x]) == "AllAnsweredCalls"){
    # AllDTs[[x]][date_created!="",Date_Created:= as.POSIXct(date_created)]
    AllDTs[[x]][,Date_Created:=date_created]
    AllDTs[[x]][,c("date_created"):=NULL]
  }else{
    # AllDTs[[x]][!is.na(start_date_)&start_date_!="",start_date:= as.POSIXct(start_date_)]
    AllDTs[[x]][,start_date:=start_date_]
    AllDTs[[x]][,c("start_date_"):=NULL]
  }
  setnames(AllDTs[[x]], names(AllDTs[[x]]), gsub(" ","_",str_to_title(gsub("_"," ",names(AllDTs[[x]])))))
  setnames(AllDTs[[x]], names(AllDTs[[x]]), gsub("_Id","_ID",names(AllDTs[[x]])))
  setnames(AllDTs[[x]], names(AllDTs[[x]]), gsub("_Qb","_QB",names(AllDTs[[x]])))
}


#Set the Service
AllToilets[,Service := "Toilets"]
AllRollOffs[,Service := "Roll-Offs"]
AllContainers[,Service := "Containers"]
AllFencing[,Service := "Fencing"]

SalesData = rbindlist(list(AllToilets,AllContainers,AllFencing,AllRollOffs),use.names = TRUE,fill = TRUE)
rm(AllContainers, AllFencing, AllRollOffs, AllToilets, AllContainers_cols, AllFencing_cols, AllRollOffs_cols, AllToilets_cols, AllAnsweredCalls_col)
rm(queryString,x)
# rm(AllDTs,base_url,col_ids,key,NeededVars,x,getData)

#2.DATA PROCESSING ##################################################
#2.a SALES DT-----------------
#Basic Column processing
SalesData = SalesData[Cancel=="" & Service_Ticket_Status != "Pending",]
SalesData = SalesData[Start_Date >= as.POSIXct("2012-01-01"),]

SalesData[,c("Cancel","Service_Ticket_Status"):=NULL]
SalesData[,Start_Date:= as.Date(format(Start_Date,"%Y-%m-%d"))]
setnames(SalesData,c("Related_Customer","Related_Site"),c("Site_Customer_ID","Site_ID"))
SalesData[is.na(Site_Customer_ID) | is.na(Site_ID),.N]
SalesData = SalesData[!is.na(Site_Customer_ID) & !is.na(Site_ID),]

#GET NCS, RCS, ES
CustRelationship = SalesData[,list(Customer_FirstSale  = min(Start_Date)), by=list(Site_Customer_ID)]
SiteRelationship = SalesData[,list(Site_FirstSale = min(Start_Date)), by=list(Site_Customer_ID,Site_ID)]

SalesData = merge(SalesData, CustRelationship, by = c("Site_Customer_ID"), all.x = TRUE)
SalesData[,Customer_Status:= ifelse(Start_Date == Customer_FirstSale, "New Customer", "Returning Customer")]
SalesData = merge(SalesData, SiteRelationship, by = c("Site_Customer_ID", "Site_ID"), all.x = TRUE)
SalesData[,Renewal_Status:= ifelse(Start_Date == Site_FirstSale, "Initiation", "Renewal")]

#Set Customer status in terms of sales: A customer is considered new only on first sale
SalesData[,Site_status := ifelse(Customer_Status == "New Customer" & Renewal_Status == "Initiation" , "NCS", 
                                 ifelse(Customer_Status == "Returning Customer" & Renewal_Status == "Initiation", "RCS",
                                        "ES"))]

#Set customer status in terms of revenue: A customer is considered new on all the sales on first site
SalesData = SalesData[order(Start_Date)]
SalesData[,Customer_Status_Revenue:= ifelse(Start_Date == Customer_FirstSale, "New Customer", NA)]
SalesData[,Customer_Status_Revenue:= na.locf(Customer_Status_Revenue,na.rm = FALSE), by=list(Site_Customer_ID,Site_ID)]
SalesData[is.na(Customer_Status_Revenue),Customer_Status_Revenue:= "Returning Customer"]

SalesData = SalesData[Start_Date >= as.Date("2018-06-08"),]
setnames(SalesData,"Start_Date","Date_Created")

rm(SiteRelationship,CustRelationship)

#2.b ANS CALLS DT ---------------------
AllAnsweredCalls[,Date_Month_Created := format(Date_Created, "%Y-%m")]
AnsCallsdt = copy(AllAnsweredCalls[order(Date_Created)])
setnames(AnsCallsdt,"Customer_QB_ID","Site_Customer_ID")
AnsCallsdt[,Call_Date_Time_Created := Date_Created]
AnsCallsdt[,Date_Created := as.Date(format(Date_Created,"%Y-%m-%d"))]
AnsCallsdt = AnsCallsdt[Call_Status != "vendor call",]
AnsCallsdt = AnsCallsdt[Number_Customer_Dialed != "(346) 954-4209",]


# Lets get the lead source of the call
LS = fread("C:\\Users\\saina\\Desktop\\Prj1\\ZTERS monthly reports\\Monthly_reports\\LeadSources_Annotated.csv",na.strings = "")
AnsCallsdt = merge(AnsCallsdt,LS[,list(Number_Customer_Dialed,Parent_Lead_Source,Service_Call = Service)],
                   by="Number_Customer_Dialed",all.x=TRUE)
AnsCallsdt[Number_Customer_Dialed=="", Parent_Lead_Source:="Other"]
AnsCallsdt[Number_Customer_Dialed=="", Service_Call:="Unspecified"]
#if(AnsCallsdt[is.na(Parent_Lead_Source),.N] > 0){
 # stop()
#}
# View(AnsCallsdt[is.na(Parent_Lead_Source),])
# AnsCallsdt[is.na(Parent_Lead_Source),.N,by=list(Number_Customer_Dialed)]

AnsCallsdt[,Parent_Lead_Source := ifelse(Date_Created <= as.Date("2019-08-12") & 
                                           Number_Customer_Dialed == "(844) 319-9748","Yelp",Parent_Lead_Source)]
rm(LS,AllAnsweredCalls)



#3. GET LATEST PAID LEAD SOURCE ######################################################
#Lets create Paid_Lead_Source column - Contemporaneous Paid lead source of the call
AnsCallsdt[,unique(Parent_Lead_Source)]
brand_ls = c("Main", "Other", "VIP", "ZSites")
AnsCallsdt[,Lead_Root:= ifelse(Parent_Lead_Source %in% brand_ls, "Brand", "Paid")]
AnsCallsdt[,Paid_Lead_Source:= ifelse(Parent_Lead_Source %in% brand_ls, NA, Parent_Lead_Source)]

#Row bind Answered calls and sales table into one big table.
SalesData[,temp1:="SalesDataRecord"]
AnsCallsdt[,temp2:="AnsdtRecord"]
AnsCallsdt = AnsCallsdt[order(Call_Date_Time_Created)]

#intersect(names(SalesData),names(AnsCallsdt))
final_dt = rbindlist(list(SalesData,AnsCallsdt),use.names=TRUE,fill = TRUE)
final_dt[,temp3:= ifelse(is.na(temp1), temp2, temp1)]
final_dt[,c("temp1","temp2"):=NULL]
# rm(firstCalldt)
final_dt = final_dt[order(Site_Customer_ID,Date_Created,temp3)]

# brand_test = final_dt[,list(Leads=paste(sort(unique(Lead_Root)),collapse = ", "), ct = .N),by = list(Site_Customer_ID)]
# View(brand_test[Leads == "Brand" & ct > 1])
# View(final_dt[,list(Date_Created,First_Call_Date,Site_Customer_ID,Site_ID,Parent_Lead_Source,Lead_Root,Paid_Lead_Source,Latest_Paid_Lead_Source,temp3,Service,NCS,ES,RCS)])


#### LOCF - Last Observation Carry Forward if there is a na value in the specified column
#Lets get the latest paid lead source information for each customer
final_dt[,Latest_Paid_Lead_Source:= na.locf(Paid_Lead_Source, na.rm = FALSE),by=Site_Customer_ID]
# View(final_dt[Site_Customer_ID == 277889, list(Date_Created,Call_Date_Time_Created,temp3, First_Call_Date, Customer_FirstSale, Parent_Lead_Source, Latest_Paid_Lead_Source)])

# For Brand only Lead Source customers, Latest_Paid_Lead_Source will always be empty. Lets fill them 
# with their respective brand Lead Source
final_dt[is.na(Latest_Paid_Lead_Source),Latest_Paid_Lead_Source:= Parent_Lead_Source]
final_dt[,Latest_Paid_Lead_Source:= na.locf(Latest_Paid_Lead_Source, na.rm = FALSE),by=Site_Customer_ID]

#Ordered during the month
final_dt[,Date_Month_Created := format(Date_Created, "%Y-%m")]
final_dt[temp3 == "SalesDataRecord",Ordered_during_month:=length(unique(temp3)),
         by=list(Site_Customer_ID,Date_Month_Created)]
final_dt = final_dt[order(Site_Customer_ID,Date_Month_Created,-temp3)]
final_dt[,Ordered_during_month_filled:= na.locf(Ordered_during_month, na.rm = FALSE),
         by=list(Site_Customer_ID,Date_Month_Created)]
final_dt[is.na(Ordered_during_month_filled), Ordered_during_month_filled:= 0]
final_dt=final_dt[order(Date_Created)]

################################################## FINAL TABLES ####################################################################
Sales = copy(final_dt[temp3 == "SalesDataRecord",list(Date_Created,Site_Customer_ID,Site_ID,Expected_Customer_Charges,Expected_Hauler_Cost,
                                                      Service,Latest_Paid_Lead_Source,Site_FirstSale,Customer_FirstSale,
                                                      Customer_Status,Renewal_Status,Customer_Status_Revenue,Site_status)])
AnsweredCalls = copy(final_dt[temp3 == "AnsdtRecord",list(Date_Created,Site_Customer_ID,Caller_ID,Number_Customer_Dialed,Call_Status,Converted_to_Customer=Converted_To_Customer,Customer_FirstSale,
                                                          Parent_Lead_Source,Service_Call,Ordered_during_month=Ordered_during_month_filled)])

AnsweredCalls[,Converted_to_Sale_Customer:= ifelse(Site_Customer_ID %in% unique(Sales[,Site_Customer_ID]), "Yes", "No")]

rm(AnsCallsdt,SalesData,final_dt,brand_ls)

####################################################################################################################################
Sales[is.na(Expected_Customer_Charges),Expected_Customer_Charges:=0]
Sales[is.na(Expected_Hauler_Cost),Expected_Hauler_Cost:=0]
Sales[,NetRevenue:= Expected_Customer_Charges  - Expected_Hauler_Cost]

#' There are some long term repeat customers, who communicate directly to the account manager via 
#' their direct line (so they dont have any lead source) or email which doesnot get recorded in QB.
#' Legacy customers are some of them - Customers who were generated before call pop up/queue. 

#Lets get all the customers with no lead source ever
noLSCustsDT = Sales[!is.na(Site_Customer_ID) & Site_Customer_ID != "",
                    list(ls = paste(sort(unique(Latest_Paid_Lead_Source)),collapse = ", "),
                         QBFirstSaleDate = unique(Customer_FirstSale)),
                    by=list(Site_Customer_ID)][ls == ""]

# noLSCustsDT[QBFirstSaleDate < "2018-08-01",.N]/noLSCustsDT[,.N] #These are legacy customers

f.noLSCustsDT = noLSCustsDT[QBFirstSaleDate < "2018-08-01",list(Site_Customer_ID,ls)]
f.noLSCustsDT[,ls:= "Legacy"]

# AnsweredCalls[ Site_Customer_ID %in% f.noLSCustsDT[,Site_Customer_ID], length(unique(Site_Customer_ID))]
# f.noLSCustsDT = f.noLSCustsDT[!Site_Customer_ID %in% AnsweredCalls[,unique(Site_Customer_ID)]]
#'Some of the customers might have had their first sale before call pop up & then a call & then no sale.
#'We are still marking them 'Legacy'


Sales = merge(Sales, f.noLSCustsDT, by = "Site_Customer_ID", all.x=TRUE)
#Sales[!is.na(Latest_Paid_Lead_Source) & !is.na(ls), .N]
Sales[is.na(Latest_Paid_Lead_Source),Latest_Paid_Lead_Source:= ls]
Sales[,ls:= NULL]
rm(noLSCustsDT ,f.noLSCustsDT)


