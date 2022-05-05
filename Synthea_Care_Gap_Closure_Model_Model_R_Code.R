#############################################################
########          POC R Studio on MS Azure           ########
########            Care Gap Closure Model           ######## 
########                   May 2022                  ######## 
#############################################################

#Access Azure Portal - Storage Account and Read files
#Data Wrangling and Predictive Model
#Step 1 - Include Azure profile
source("C:/dev/Rprofile.R")
#Step 2 - Invoke necessary libraries for analyses and modeling.
library(AzureStor)    #Manage storage in Microsoft's 'Azure' cloud
library(AzureRMR)     #Interface to 'Azure Resource Manager'
library(psych)        #A general purpose toolbox for personality, psychometric theory and experimental psychology. Functions are primarily for multivariate analysis. 
library(ggplot2) 	    #A system for creating graphics, based on "The Grammar of Graphics". 
library(caret) 		    #Misc functions for training and plotting classification and regression models.
library(rpart) 		    #Recursive partitioning for classification, regression and survival trees.  
library(rpart.plot) 	#Plot 'rpart' models. Extends plot.rpart() and text.rpart() in the 'rpart' package.
library(RColorBrewer) #Provides color schemes for maps (and other graphics). 
library(party)		    #A computational toolbox for recursive partitioning.
library(partykit)	    #A toolkit with infrastructure for representing, summarizing, and visualizing tree-structure.
library(pROC) 		    #Display and Analyze ROC Curves.
library(ISLR)		      #Collection of data-sets used in the book 'An Introduction to Statistical Learning with Applications in R.
library(randomForest)	#Classification and regression based on a forest of trees using random inputs.
library(dplyr)		    #A fast, consistent tool for working with data frame like objects, both in memory and out of memory.
library(ggraph)		    #The grammar of graphics as implemented in ggplot2 is a poor fit for graph and network visualizations.
library(igraph)		    #Routines for simple graphs and network analysis.
library(mlbench) 	    #A collection of artificial and real-world machine learning benchmark problems, including, e.g., several data sets from the UCI repository.
library(GMDH2)		    #Binary Classification via GMDH-Type Neural Network Algorithms.
library(apex)		      #Toolkit for the analysis of multiple gene data. Apex implements the new S4 classes 'multidna'.
library(mda)		      #Mixture and flexible discriminant analysis, multivariate adaptive regression splines.
library(WMDB)		      #Distance discriminant analysis method is one of classification methods according to multiindex.
library(klaR)		      #Miscellaneous functions for classification and visualization, e.g. regularized discriminant analysis, sknn() kernel-density naive Bayes...
library(kernlab)	    #Kernel-based machine learning methods for classification, regression, clustering, novelty detection.
library(readxl)    	  #n Import excel files into R. Supports '.xls' via the embedded 'libxls' C library.                                                                                                                                                                 
library(GGally)  	    #The R package 'ggplot2' is a plotting system based on the grammar of graphics.                                                                                                                                                                  
library(mctest)		    #Package computes popular and widely used multicollinearity diagnostic measures.
library(sqldf)		    #SQL for dataframe wrangling.
library(reshape2)     #Pivoting table
library(anytime)      #Caches TZ in local env
library(survey)       #Summary statistics, two-sample tests, rank tests, glm.... 
library(mice)         #Library for multiple imputation
library(MASS)         #Functions and datasets to support Venables and Ripley
library(rjson)        #Load the package required to read JSON files.
require(lubridate)    #Date manipulation

#Apply credentials from profile
az <- create_azure_login(tenant=Azure_tenantID)
rg <- az$get_subscription(Azure_SubID)$get_resource_group(Azure_ResourceGrp)

#retrieve storage account
stor <- rg$get_storage_account("olastorageac")
stor$get_blob_endpoint()
stor$get_file_endpoint()

# same as above
blob_endp <- blob_endpoint("https://olastorageac.blob.core.windows.net/",key=Azure_Storage_Key)
file_endp <- file_endpoint("https://olastorageac.file.core.windows.net/",key=Azure_Storage_Key)

# shared access signature: read/write access, container+object access and set expiry date
sas <- AzureStor::get_account_sas(blob_endp, permissions="rwcdl", expiry=as.Date("2030-01-01"))
#create an endpoint object with a SAS, but without an access key
blob_endp <- stor$get_blob_endpoint(sas=sas)

#An existing container
Synthea_csv <- blob_container(blob_endp, "syntheacsv")
Synthea_json <- blob_container(blob_endp, "syntheajson")

# list blobs inside a blob container
list_blobs(Synthea_csv)
list_blobs(Synthea_json)

#Temp download of files needed for data wrangling
storage_download(Synthea_csv, "patients.csv", "~/patients.csv")
storage_download(Synthea_csv, "procedures.csv", "~/procedures.csv")
storage_download(Synthea_csv, "immunizations.csv", "~/immunizations.csv")

#Read csv in memory
patients<-read.csv("patients.csv")
procedures<-read.csv("procedures.csv")
immunizations<-read.csv("immunizations.csv")

#Delete Temp downloaded of files
file.remove("patients.csv")
file.remove("procedures.csv")
file.remove("immunizations.csv")

#Data cleaning and wrangling to get features required for modeling
#Number of columns
ncol(patients)
#Number of rows
nrow(patients)
#View fields in files
names(patients)
#View subset of file
head(patients,2)
#View structure of file
str(patients)

#Step 3
#Descriptive Statistics 
summary(patients)


#Data wrangling from Patients, Procedures and Immunizations
pat_df<-sqldf("select distinct Id,BIRTHDATE,SSN as Dummy_Phone_No,MARITAL,RACE,ETHNICITY,GENDER from patients")
pat_df$currentDate <- with(pat_df, Sys.Date())
pat_df$Age <- with(pat_df, trunc((BIRTHDATE %--% currentDate) / years(1)))
#Exclude Children for now - Age 18 and above only, making calls or chat
pat_df_adult<-sqldf("select * from pat_df where Age>=18")
#Colonoscopy eligibility
pat_df_adult_1<-sqldf("select x.*,
                   case when GENDER='M' and Age>=50 then 'Eligible'
                        else 'Not Eligible'
                   end as Colonoscopy      
                   from pat_df_adult x")
#Mammography eligibility
pat_df_adult_2<-sqldf("select x.*,
                   case when GENDER='F' and Age>=40 then 'Eligible'
                        else 'Not Eligible'
                   end as Mammography      
                   from pat_df_adult_1 x")
#Immunizations eligibility
pat_df_adult_3<-sqldf("select x.*,
                   case when Age>=18 then 'Eligible'
                        else 'Not Eligible'
                   end as Immunizations      
                   from pat_df_adult_2 x")
#Data wrangling from Procedures and Immunizations
proc_col_df<-sqldf("select distinct PATIENT as Id,'Colonoscopy Done' as Code from procedures where DESCRIPTION='Colonoscopy'")
proc_mam_df<-sqldf("select distinct PATIENT as Id,'Mammography Done' as Code from procedures where DESCRIPTION like '%Screening Mammography%'")
immu_hep_df<-sqldf("select distinct PATIENT as Id,'Hepatitis Done' as Code from immunizations where DESCRIPTION='Hep A  adult' or DESCRIPTION='Hep B  adult'")
immu_flu_df<-sqldf("select distinct PATIENT as Id,'Influenza Done' as Code from immunizations where DESCRIPTION='Influenza  seasonal  injectable  preservative free'")
#All Care Gaps Closure Eligibility
pat_df_elig_col<-sqldf("select distinct Id,Dummy_Phone_No, 'Colonoscopy' as Eligibility
                         from pat_df_adult_3 where Colonoscopy='Eligible'")
pat_df_elig_mam<-sqldf("select distinct Id,Dummy_Phone_No, 'Mammography' as Eligibility
                         from pat_df_adult_3 where Mammography='Eligible'")
pat_df_elig_imm<-sqldf("select distinct Id,Dummy_Phone_No, 'Immunizations' as Eligibility
                         from pat_df_adult_3 where Immunizations='Eligible'")
#Exclude procedures completed already
pat_col<-sqldf("select distinct x.Id,x.Dummy_Phone_No,'Colonoscopy' as Final_Eligibility from pat_df_elig_col x
                              where x.Id not in (select distinct Id from proc_col_df)")
pat_mam<-sqldf("select distinct x.Id,x.Dummy_Phone_No,'Mammography' as Final_Eligibility from pat_df_elig_mam x
                  where x.Id not in (select distinct Id from proc_mam_df)")
pat_imm_hep<-sqldf("select distinct x.Id,x.Dummy_Phone_No, 'Hepatitis' as Final_Eligibility from pat_df_elig_imm x
                             where x.Id not in (select distinct Id from immu_hep_df)")
pat_imm_flu<-sqldf("select distinct x.Id,x.Dummy_Phone_No,'Influenza' as Final_Eligibility from pat_df_elig_imm x
                             where x.Id not in (select distinct Id from immu_flu_df)")
#Consolidated view
Care_Gaps<-sqldf("select * from pat_col
                union
                select * from pat_mam
                union
                select * from pat_imm_hep
                union
                select * from pat_imm_flu")
#Add made up row
#testdata<-read.csv('/Users/olajideajayi/OneDrive - Microsoft/Documents/testdata.csv',row.names=T)
#testdata<-read.csv(file.choose())
#attach(testdata)
#Care_Gaps_2<-rbind(Care_Gaps,testdata)
#write.csv(Care_Gaps_2, file = "Care_Gaps_Closure_Data.csv")
Care_Gaps_Final<-sqldf("select * from Care_Gaps order by Id")

#Write output of prediction to csv 
write.csv(Care_Gaps_Final, file = "Care_Gaps_Final.csv")
#Creating the data for JSON file
#jsonData <- toJSON(Final_Result)
#write(jsonData,"Final_Result_json.json")

#Upload Model results data into container in Azure
cont_upload <- blob_container(blob_endp, "modeloutput")
upload_blob(cont_upload, src="C:\\Users\\olajideajayi\\OneDrive - Microsoft\\Documents\\Care_Gaps_Final.csv")
#upload_blob(cont_upload, src="C:\\Users\\olajideajayi\\OneDrive - Microsoft\\Documents\\Final_Result_json.json")

#Remove Azure Credentials from environment after use 
rm(Azure_SubID) 
rm(Azure_Storage_Key)
rm(Azure_tenantID)
rm(Azure_ResourceGrp)