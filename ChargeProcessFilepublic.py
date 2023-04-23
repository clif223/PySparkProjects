#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[437]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os


# In[438]:


os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-18.0.1.1"


# In[439]:


conf = SparkConf() \
.setAppName("ChargePrep") \
.setMaster("local") \
.set("spark.driver.extraClassPath", "C:/PySpark/*") \
.set("spark.executor.extraClassPath", "C:/PySpark/*")


# In[440]:


sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)


# In[441]:


#Set Dataframe variables and file locations
global mappingframe
global dataframe
global SQLprovidermappingResults
global SQLlocationmappingResults
dataframepath = r"ChargeCSVfile"
mappingfilepath = r"ProviderMappingJSONfilr"
origfilepath = r"DiagnosisCSVfile"
casenumber = str(33333)
sqltablename = "Case" + casenumber + "ChargeFile"


# In[442]:


#Set PostGre Credentials
postgreuser = "**postgreUsername**"
postgrepassword = "**postgrePASSWORD**"
postgredriver = "**PostgreDriver**"
postgredatabase = "**PostGreDatabase**"
postgreURL = f"jdbc:postgresql://localhost:****/{postgredatabase}?user={postgreuser}&password={postgrepassword}"


# In[443]:


#Set SQL Server Credentials
ssmsuser = "**SSMSuser**"
ssmspassword = "**SSMSPassword**"
ssmsdriver = "**SSMSDriver**"
ssmsdatabase = "**SSMSDatabase**"
ssmsURL =  f"jdbc:sqlserver://localhost:****;database={ssmsdatabase};encrypt=true;trustServerCertificate=true;"


# In[444]:


#Set file paths for the location and provider mapping
providermappingfilepath = r"ProviderMappingJSONfilr"
locationmappingfilepath = r"ProviderMappingJSONfilr"


# In[445]:


#FUnction sets the dataframes for the mapping files
def setDataframesforMapping(dfpath, mfpath, lpath):
    global dataframe
    dataframe = spark.read.options(delimiter=",", header=True).csv(dfpath)
    global mappingframe
    mappingframe = spark.read.option("multiline","true").json(mfpath)
    global locationframe
    locationframe = spark.read.options(delimiter=",", header=True).csv(lpath)


# In[446]:


#Function converts the vendor ID to the internal doctor ID
def MapProviders():
    global SQLprovidermappingResults
    dataframe.createOrReplaceTempView("ChargesTable")
    mappingframe.createOrReplaceTempView("MappingsTable")
    SQLprovidermappingResults = spark.sql("select c.patientid, c.dateofservice, m.CenID as DoctorID, c.cptcode, c.extlocid, c.diaglist as DiagnosisList, c.Units, c.Measurement_Unit from ChargesTable c inner join MappingsTable m on c.NPI = m.NPI")


# In[447]:


setDataframesforMapping(dataframepath, providermappingfilepath, locationmappingfilepath)
MapProviders()


# In[448]:


#dataframe = SQLprovidermappingResults
#dataframe.show()


# In[449]:


#Function converts the vendor's location ID to the Internal location ID
def LocationMapping():
    global SQLlocationmappingResults
    #dataframe.createOrReplaceTempView("ChargesTable")
    SQLprovidermappingResults.createOrReplaceTempView("ChargesTable")
    locationframe.createOrReplaceTempView("LocationTable")
    SQLlocationmappingResults = spark.sql("select c.patientid, c.dateofservice, c.doctorid, l.InternalLocationID as locationid, diagnosislist, c.cptcode, c.units, c.measurement_unit from ChargesTable c inner join LocationTable l on c.extlocid = l.ExternalLocationID")


# In[450]:


LocationMapping()


# In[451]:


#dataframe = SQLlocationmappingResults
#dataframe.show()


# In[452]:


#Function connects to the PostGre database
global diagdataframe
global csvdataframe

def connecttoPostgre():
    global diagdataframe 
    diagdataframe =spark.read \
    .format("jdbc") \
    .option("url", postgreURL) \
    .option("dbtable", "diagsmap") \
    .option("user", postgreuser) \
    .option("password", postgrepassword) \
    .option("driver", postgredriver) \
    .load()


# In[453]:


connecttoPostgre()


# In[454]:


#Function loads the table dataframe to PostGre
def loadtabletoPostgre(df,casenum):
    try:
        print("Import table to Postgre")
        df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", postgreURL) \
        .option("user", postgreuser) \
        .option("password", postgrepassword) \
        .option("driver", postgredriver) \
        .option("dbtable", "Diagmap_for_case" + casenumber) \
        .save()
        print("Table imported into PostGre")
    except Exception as e:
        print("Data load error: " + str(e))


# In[455]:


def extractforPost(df):
    try:
        frame = df
        frame.show()
        loadtabletoPostgre(df,casenumber)
    except Exception as e:
        print("Data extract error: " + str(e))


# In[456]:


extractforPost(SQLlocationmappingResults)


# In[457]:


#Function run the query on PostGre that runs the function diagchange to convert the diagnosis from ICD9 to ICD10 codes
def getpostgreresult(pref, casenum):
    global postgreresult
    postgreresult = spark.read.format("jdbc") \
        .option("url",postgreURL) \
        .option("driver", postgredriver) \
        .option("query", r"select patientid, dateofservice, doctorid, locationid, cptcode, units, measurement_unit, diagchange (string_to_array(diagnosislist, ','))  from " + pref + casenum) \
        .option("user", postgreuser) \
        .option("password", postgrepassword) \
        .load()
    
getpostgreresult("diagmap_for_case", casenumber)


# In[458]:


postgreresult.show()


# In[459]:


dataframe = postgreresult


# In[460]:


dataframe.show()


# In[461]:


#Function pulls the Unit of Measure IDs in order for it to add to the database and be read
def SetUoMIDs(df):
    return df.withColumn("UnitofMeasureID", expr("CASE WHEN measurement_unit = 'UN' THEN 371 " +
                                                 "WHEN measurement_unit = 'ML' THEN 372 " +
                                                 "WHEN measurement_unit = 'MG' THEN 373 " +
                                                 "ELSE '371' END"))


# In[462]:


dataframe = SetUoMIDs(SQLlocationmappingResults)


# In[463]:


dataframe.show()


# In[464]:


#Functions loads data into SSMS
def loadtossms(df, sqltablename):
    try:
        print("load to sql...")
        df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", ssmsURL) \
        .option("user", ssmsuser) \
        .option("password", ssmspassword) \
        .option("driver", ssmsdriver) \
        .option("dbtable", sqltablename) \
        .save()
    except Exception as e:
        print("Data load error: " + str(e))
        
def extracttossms(dataframe):
    try:
        frame=dataframe
        frame.show()
        loadtossms(dataframe, sqltablename)
        print('Data successfully loaded to SQL')
    except Exception as e:
        print("Data extract error: " + str(e))


# In[465]:


extracttossms(dataframe)


# In[ ]:





# In[ ]:






# In[ ]:




