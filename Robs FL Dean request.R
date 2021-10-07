library(DBI)
library(RODBC)
library(data.table)
library(stringr)
library(lubridate)
library(dplyr)
library(xlsx)
#Cldb_Directory <- "???I:\\dataCalls\\databases\\CLDB_Queries.accdb"
#Cldb_DBQ <- gsub("\\\\","/",Cldb_Directory)
#MO_Comm_Liab_ClassCode_Descriptions <- setDT(read.csv("I:/DataCalls/Rstudio Datasets/MO_Comm_Liab_ClassCode_Descriptions09092021.csv"))
#con_RH <- dbConnect(odbc::odbc(), .connection_string = "Driver={Microsoft Access Driver (*.mdb)};DBQ=I:/DataCalls/Databases/RiskHistory - Access2003 Format.mdb", timeout = 10)
con_actuarial <- dbConnect(odbc::odbc(), "actuarial", timeout = 10)
#con_RH <- dbConnect(odbc::odbc(),)
Current_year = year(today()) #for SQL Valuation Dates and Column heading
Valuation = dmy(paste("01-01-",year(today())-1)) #for SQL Valuation Dates and Column heading
outstanding_valuation = dmy(paste("01-01-",year(today())-2)) #for SQL Valuation Dates and Column heading
#Valuation
#year(Valuation)
#MO_Comm_RH_SQL_Select = paste("SELECT dbo_loss_detail.major_peril_code, [Mapping - MP - ASLOB].major_peril_name, dbo_loss_detail.gaap_novated_company, dbo_loss_detail.service_office, dbo_loss_detail.claim_number, Year([policy_effective_date]) AS PolYr, Year([accident_date]) AS AccYr, dbo_loss_detail.feature_number, dbo_loss_detail.insured_state, [Mapping - MP - ASLOB].page14_lob, Sum(IIf([dbo_loss_detail]![valuation_date] Between #1/1/",year(Valuation),"# And #12/31/",year(Valuation),"#,[dbo_loss_detail]![salvage_subrogation_amount],0)) AS SalvageSubro, Sum(IIf([dbo_loss_detail]![valuation_date] Between #1/1/",year(Valuation),"# And #12/31/",year(Valuation),"#,[dbo_loss_detail]![indemnity_paid_amount],0)) AS [Ind Paid], Sum(IIf([dbo_loss_detail]![valuation_date] Between #1/1/",year(Valuation),"# And #12/31/",year(Valuation),"#,[dbo_loss_detail]![medical_paid_amount],0)) AS [Med Paid], Sum(IIf([dbo_loss_detail]![valuation_date]=#12/31/",year(Valuation),"#,[dbo_loss_detail]![indemnity_outstanding_amount],0)) AS [Ind OS ",year(Valuation),"], Sum(IIf([dbo_loss_detail]![valuation_date]=#12/31/",year(Valuation),"#,[dbo_loss_detail]![medical_outstanding_amount],0)) AS [Med OS ",year(Valuation),"], Sum(IIf([dbo_loss_detail]![valuation_date]=#12/31/",year(outstanding_valuation),"#,[dbo_loss_detail]![indemnity_outstanding_amount],0)) AS [Ind OS ",year(outstanding_valuation),"], Sum(IIf([dbo_loss_detail]![valuation_date]=#12/31/",year(outstanding_valuation),"#,[dbo_loss_detail]![medical_outstanding_amount],0)) AS [Med OS ",year(outstanding_valuation),"]
#FROM dbo_loss_detail INNER JOIN [Mapping - MP - ASLOB] ON dbo_loss_detail.major_peril_code = [Mapping - MP - ASLOB].major_peril WHERE (((dbo_loss_detail.valuation_date)>#11/30/",year(outstanding_valuation),"# And (dbo_loss_detail.valuation_date)<#1/1/",Current_year,"#) AND ((dbo_loss_detail.business_type)='1')) GROUP BY dbo_loss_detail.major_peril_code, [Mapping - MP - ASLOB].major_peril_name, dbo_loss_detail.gaap_novated_company, dbo_loss_detail.service_office, dbo_loss_detail.claim_number, Year([policy_effective_date]), Year([accident_date]), dbo_loss_detail.feature_number, dbo_loss_detail.insured_state, [Mapping - MP - ASLOB].page14_lob HAVING (((dbo_loss_detail.insured_state)='33') AND (([Mapping - MP - ASLOB].page14_lob) In ('052','171','196','198','172')));",sep = "")
#MO_Comm_RH_SQL_Select
act_sql <- paste("SELECT policy_symbol, policy_serial, service_office, claim_number, feature_number, accident_date, policy_effective_date, major_peril_code, Sum(salvage_subrogation_amount) AS SumOfsalvage_subrogation_amount, Sum(indemnity_paid_amount) AS SumOfindemnity_paid_amount, Sum(medical_paid_amount) AS SumOfmedical_paid_amount",
" FROM loss_detail",
" WHERE (((valuation_date)>'2018-12-31'))",
" GROUP BY policy_symbol, policy_serial, service_office, claim_number, feature_number, accident_date, policy_effective_date, major_peril_code",
" HAVING (((major_peril_code) In ('U08','S75')));",sep="")
act_sql
RH_Query <- dbGetQuery(con_actuarial,act_sql)
dbDisconnect(con_RH)
dbDisconnect(con_actuarial)
RH_Query <- setDT(RH_Query)
RH_Query$Claim_Number2 = paste(RH_Query$service_office,RH_Query$claim_number,substr(paste("000",RH_Query$feature_number,sep = ""),nchar(paste("000",RH_Query$feature_number,sep = ""))-2,nchar(paste("000",RH_Query$feature_number,sep = ""))), sep = "")
#View(RH_Query)
#RH_serv_claim <- as.array(paste(RH_Query$service_office,RH_Query$claim_number,sep = ""))
#RH_serv_claim <- as.array(paste(RH_Query$service_office,RH_Query$claim_number,sep = ""))
#RH_serv_claim
RH_Query$Pol_Yr <- year(RH_Query$policy_effective_date)
RH_Query$class_cd[RH_Query$Pol_Yr >2018] <- NA
#RH_Query[is.na(RH_Query$class_cd)]
#NROW(RH_Query)

con_cldb <- dbConnect(odbc::odbc(), .connection_string = "Driver={Microsoft Access Driver (*.mdb)};DBQ=I:/DataCalls/Databases/CLDB_Queries_V2003.mdb", timeout = 10)

#CLDB_SQL <- paste("SELECT dbo_policy.co_cd, dbo_policy.business_typ_cd, dbo_policy.risk_st_cd, dbo_pc_feature.juris_st_cd, dbo_policy.pol_no AS Policy_Number, Left([dbo_policy]![clm_no],2) & Right([dbo_policy]![clm_no],6) & [dbo_pc_coverage]![fea_no] AS Claim_Number, dbo_policy.clm_no, dbo_pc_coverage.fea_no, dbo_pc_coverage.class_cd, dbo_policy.pol_eff_dt, dbo_policy.pol_xpr_dt, dbo_pc_coverage.major_peril_cd, tblMapMajorPeril.NYLOB
#FROM tblMapMajorPeril INNER JOIN ((dbo_policy INNER JOIN dbo_pc_feature ON dbo_policy.clm_no = dbo_pc_feature.clm_no) INNER JOIN dbo_pc_coverage ON (dbo_pc_feature.fea_no = dbo_pc_coverage.fea_no) AND (dbo_pc_feature.clm_no = dbo_pc_coverage.clm_no)) ON tblMapMajorPeril.MajorPeril = dbo_pc_coverage.major_peril_cd
#GROUP BY dbo_policy.co_cd, dbo_policy.business_typ_cd, dbo_policy.risk_st_cd, dbo_pc_feature.juris_st_cd, dbo_policy.pol_no, Left([dbo_policy]![clm_no],2) & Right([dbo_policy]![clm_no],6) & [dbo_pc_coverage]![fea_no], dbo_policy.clm_no, dbo_pc_coverage.fea_no, dbo_pc_coverage.class_cd, dbo_policy.pol_eff_dt, dbo_policy.pol_xpr_dt, dbo_pc_coverage.major_peril_cd, tblMapMajorPeril.NYLOB
#HAVING (((dbo_policy.risk_st_cd)='MO') AND ((dbo_policy.pol_eff_dt)>#12/31/",Current_year - 6,"# And (dbo_policy.pol_eff_dt)<#1/1/",Current_year,"#)) OR (((dbo_pc_feature.juris_st_cd)='MO') AND ((dbo_policy.pol_eff_dt)>#12/31/",Current_year - 6,"# And (dbo_policy.pol_eff_dt)<#1/1/",Current_year,"#));",sep="")
#CLDB_Query <- setDT(dbGetQuery(con_cldb,CLDB_SQL))
#CLDB_Query$fea_no <- substr(CLDB_Query$fea_no,nchar(CLDB_Query$fea_no)-2,nchar(CLDB_Query$fea_no))
#Error_DB <- CLDB_Query[CLDB_Query$Claim_Number!=paste(substr(CLDB_Query$clm_no,1,2),substr(CLDB_Query$clm_no,nchar(CLDB_Query$clm_no)-5,nchar(CLDB_Query$clm_no)),substr(CLDB_Query$fea_no,nchar(CLDB_Query$fea_no)-2,nchar(CLDB_Query$fea_no)),sep = "")]
#View(CLDB_Query)
#View(Error_DB) #errors most likely are due to changes in procedure / formatting of the 3 fields involved in creating Claim_Number (connection)
#View(RH_Query)
#MO_Comm_Liab <- RH_Query[CLDB_Query,class_cd:=class_cd,on = 'Claim_Number']
#colnames(RH_Query)
#colnames(CLDB_Query)
#View(Error_DB)
NoClass <- setDT(RH_Query[is.na(RH_Query$class_cd)])

NoClass
#View(NoClass[NoClass$PolYr==2018])
#View(NoClass)
#View(NoClass[NoClass$Sum!=0])
Access_In_Array = ""
for (i in 1:NROW(NoClass$Claim_Number)) {
  Access_In_Array<- paste(Access_In_Array,"'",NoClass$service_office[[i]],NoClass$claim_number[[i]],"',",sep = "")
  print(i)}
Access_In_Array = substr(Access_In_Array,1,nchar(Access_In_Array)-1)
Access_In_Array
#View(MO_Comm_Liab)
Min_PolYr = min(RH_Query$Pol_Yr[is.na(RH_Query$class_cd)]) - 1
New_SQL <- paste("SELECT dbo_policy.co_cd, dbo_policy.business_typ_cd, dbo_policy.risk_st_cd, dbo_policy.pol_no AS Policy_Number, Left([dbo_policy]![clm_no],2) & Right([dbo_policy]![clm_no],6) AS claim_number, dbo_policy.clm_no, dbo_pc_coverage.fea_no, dbo_pc_coverage.class_cd, dbo_policy.pol_eff_dt, dbo_policy.pol_xpr_dt, dbo_pc_coverage.major_peril_cd, tblMapMajorPeril.NYLOB
FROM dbo_policy INNER JOIN (tblMapMajorPeril INNER JOIN dbo_pc_coverage ON tblMapMajorPeril.MajorPeril = dbo_pc_coverage.major_peril_cd) ON dbo_policy.clm_no = dbo_pc_coverage.clm_no
GROUP BY dbo_policy.co_cd, dbo_policy.business_typ_cd, dbo_policy.risk_st_cd, dbo_policy.pol_no, Left([dbo_policy]![clm_no],2) & Right([dbo_policy]![clm_no],6), dbo_policy.clm_no, dbo_pc_coverage.fea_no, dbo_pc_coverage.class_cd, dbo_policy.pol_eff_dt, dbo_policy.pol_xpr_dt, dbo_pc_coverage.major_peril_cd, tblMapMajorPeril.NYLOB
HAVING (((Left([dbo_policy]![clm_no],2) & Right([dbo_policy]![clm_no],6)) In (",Access_In_Array,")) AND ((dbo_policy.pol_eff_dt)>#12/31/",Min_PolYr,"#));", sep = "")
#New_SQL
NoClassQuery <- setDT(dbGetQuery(con_cldb,New_SQL))
NoClassQuery$fea_no <- substr(NoClassQuery$fea_no,nchar(NoClassQuery$fea_no)-2,nchar(NoClassQuery$fea_no))
NoClassQuery$Claim_Number2 <- paste(substr(NoClassQuery$clm_no,1,2),substr(NoClassQuery$clm_no,nchar(NoClassQuery$clm_no)-5,nchar(NoClassQuery$clm_no)),substr(NoClassQuery$fea_no,nchar(NoClassQuery$fea_no)-2,nchar(NoClassQuery$fea_no)),sep = "")
#NoClassQuery[NoClassQuery$Claim_Number == 'NJ699086001']
#NoClassQuery[MO_Comm_Liab,service_office := service_office, on = 'Claim_Number']
#View(NoClassQuery)
View(RH_Query)
View(NoClassQuery)
str(RH_Query)
str(NoClassQuery)
View(Other)
new <- merge(RH_Query,NoClassQuery,by = 'Claim_Number2')


final <- new[,c(1:13,23)]
Backup_file <- file.path("","","morfs02","USER_HOME2","MCampbell","Robs FL Dean request.xlsx")

write.xlsx(x = final,file = Backup_file,sheetName = "FL Dean",append = FALSE)
View(new[,c(1:13,23)])
RH_Query[NoClassQuery,class_cd := class_cd, on = 'Claim_Number2']
View(RH_Query)
NROW(unique(NoClassQuery$Claim_Number2))
NROW(unique(RH_Query$Claim_Number2))
Other <- RH_Query[NoClassQuery,Class_Code2 := NoClassQuery$class_cd, on = 'Claim_Number2']
RH_Query$Class_Code = RH_Query$class_cd
RH_Query$Class_Code[is.na(RH_Query$class_cd)] = RH_Query$class_missing[is.na(RH_Query$class_cd)]
nrow(RH_Query[is.na(RH_Query$Class_Code)])/nrow(RH_Query)
RH_Query$Class_Code <- trimws(RH_Query$Class_Code)
MO_Final_Losses<- RH_Query[,c(1:17,21)]
MO_Final_Losses$Status[MO_Final_Losses$`Ind OS 2020`+MO_Final_Losses$`Ind OS 2020` == 0] <- 'CLSD'
MO_Final_Losses$Status[MO_Final_Losses$`Ind OS 2020`+MO_Final_Losses$`Ind OS 2020` != 0] <- 'OPEN'
#MO_Final_Losses$Class_Code
#View(setDT(as.list(unique(MO_Final_Losses$Class_Code))))
#View(RH_Query_ClassCode_Descriptions)
RH_Query_ClassCode_Descriptions$CLASS <- trimws(RH_Query_ClassCode_Descriptions$CLASS)
names(RH_Query_ClassCode_Descriptions) <- c('Class_Code','DESC')
MO_Final_Losses[RH_Query_ClassCode_Descriptions, DESC := DESC, on = 'Class_Code']#MO_Final_Losses[is.na(MO_Final_Losses$DESC)]
#MO_Final_Losses$Class_Code
#View(MO_Final_Losses)
#str(MO_Final_Losses)
#str(MO_Comm_Liab_ClassCode_Descriptions)
#View(MO_Final_Losses)
#dbDisconnect(con_cldb)


BOR_str<-file.path("I:","DataCalls","Databases","BOR Premiums - Access2003 format.mdb")
con_BOR <- dbConnect(odbc::odbc(), .connection_string = paste("Driver={Microsoft Access Driver (*.mdb)};DBQ=",BOR_str,sep=""), timeout = 10)
BOR_Comm_qry <- paste("SELECT dbo_financial_view_detail.prem_entry_co_cd, dbo_financial_view_detail.statis_st_cd, dbo_major_peril_info.page14_lob, dbo_financial_view_detail.cls_cd, dbo_financial_view_detail.pol_sym_cd, dbo_financial_view_detail.pol_ser_no, dbo_financial_view_detail.pol_eff_dt, dbo_financial_view_detail.pol_xpr_dt, Sum(dbo_financial_view_detail.wrtn_prem_amt) AS SumOfwrtn_prem_amt
FROM dbo_financial_view_detail INNER JOIN dbo_major_peril_info ON dbo_financial_view_detail.maj_prl_cd = dbo_major_peril_info.major_peril
WHERE (((dbo_financial_view_detail.valtn_dt_cd)>#12/31/",Current_year-2,"# And (dbo_financial_view_detail.valtn_dt_cd)<#1/1/",Current_year,"#) AND ((dbo_financial_view_detail.bus_typ_cd)='1'))
GROUP BY dbo_financial_view_detail.prem_entry_co_cd, dbo_financial_view_detail.statis_st_cd, dbo_major_peril_info.page14_lob, dbo_financial_view_detail.cls_cd, dbo_financial_view_detail.pol_sym_cd, dbo_financial_view_detail.pol_ser_no, dbo_financial_view_detail.pol_eff_dt, dbo_financial_view_detail.pol_xpr_dt
HAVING (((dbo_financial_view_detail.statis_st_cd)='33') AND ((dbo_major_peril_info.page14_lob) In ('171','172','196','198','052')) AND ((Sum(dbo_financial_view_detail.wrtn_prem_amt))<>0));",sep = "")

WP_Data <- setDT(dbGetQuery(con_BOR,BOR_Comm_qry))
names(WP_Data) <- c('prem_entry_co_cd','statis_st_cd','page14_lob','Class_Code','pol_sym_cd','pol_ser_no','pol_eff_dt','pol_xpr_dt','SumOfwrtn_prem_amt')
WP_Data[MO_Comm_Liab_ClassCode_Descriptions, DESC := DESC, on = 'Class_Code']
unique(WP_Data$Class_Code[is.na(WP_Data$DESC)|WP_Data$DESC == "#N/A" ])
WP_Pivot <- WP_Data %>%
  group_by(prem_entry_co_cd,page14_lob,DESC)%>%
  summarise(Total = sum(SumOfwrtn_prem_amt))
WP_Pivot <- setDT(WP_Pivot)
View(WP_Pivot)
#list.files(file.path("","","morfs02","USER_HOME2","MCampbell"))
write.xlsx(x = WP_Pivot,file = Backup_file,sheetName = "WP Pivot",append = TRUE)

