#!/usr/bin/env python
# coding: utf-8

# In[1]:


from configparser import ConfigParser
from mysql.connector import MySQLConnection, Error

class Database_Connect:
    """
    class to make DB connection
    It accesses the config.ini file where in configurations needed for database
    connect through mysql and pyspark are kept.
    """
    
    
    def __init__(self, filename='config.ini',section='mysql'):
        self._filename=filename
        self._section=section
    
    
    def read_db_config(self):
        parser=ConfigParser()
        parser.read(self._filename)
        self._db={}
        if parser.has_section(self._section):
            items = parser.items(self._section)
            for item in items:
                self._db[item[0]]=item[1]
        else:
            raise Exception('{0} not found in the {1} file'.format(self._section, self._filename))

        return self
    
    def db_connect(self):
        try:
            print(self._db)
            self._con = MySQLConnection(**self._db)
            if self._con.is_connected():
                print('connected to db')
                return self
            else:
                print('not connected to db')
                return None
        except Error as error:
            print('error',error)
            
    def close(self):
        if self._con is not None and self._con.is_connected():
            self._con.close()
    
    def get_connection(self):
        if self._con is not None and self._con.is_connected():
            return self._con
        else:
            return None
    def get_config(self):
        if self._db is not None:
            return self._db
        else:
            return None
        
    def set_config(self, key, val):
        if self._db is not None:
            self._db[key]=val
            return self
        else:
            return None


# In[2]:


import findspark
findspark.init('/home/ec2-user/spark-2.4.7-bin-hadoop2.7')
from pyspark.sql import SparkSession, Row, DataFrame
import pydeequ


# In[3]:


##create spark session
spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())


# In[4]:


#load data
file_location ='/home/ec2-user/spark-2.4.7-bin-hadoop2.7/examples/src/main/resources/1000 Records.csv'
file_type= 'csv'

#csv options
infer_schema="true"
first_row_is_header="true"
delimiter=","


# In[5]:


df = spark.read.format(file_type)                 .option("inferSchema", infer_schema)                 .option("header", first_row_is_header)                 .option("sep", delimiter)                 .load(file_location)


# In[6]:


df.printSchema()
df.show(1)


# In[7]:


#store data in file
from pydeequ.analyzers import *
from pydeequ.repository import *

metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, 'verify_newfile.json')
repository = FileSystemMetricsRepository(spark, metrics_file)
key_tags = {'tag': 'analyzer'}

resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)


# In[8]:


quantileProbs =[0.25,0.5,0.75,0.9]
relError=0.0
import pandas as pd
#pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
query="Salary >100000"
myresult = AnalysisRunner(spark)                .onData(df)                .addAnalyzer(Size(where=query))                .addAnalyzer(Completeness("Emp ID",query))                .addAnalyzer(Compliance("Salary >100000 and 'Year of Joining' > 10",query))                .addAnalyzer(ApproxCountDistinct("Emp ID",query))                .addAnalyzer(ApproxQuantile("Salary",0.25,0.05, query))                .addAnalyzer(ApproxQuantiles("Salary",quantileProbs,relError))                .addAnalyzer(Correlation("Salary","Age in Company (Years)",query))                .addAnalyzer(CountDistinct(["Emp ID","SSN","Phone No","E Mail","Salary","Year of Joining"]))                .addAnalyzer(DataType("Emp ID",query))                .addAnalyzer(Distinctness("EMP ID",query))                .addAnalyzer(Distinctness(["Age in Company (Years)","Salary"], query))                .addAnalyzer(Entropy('SSN',query))                .addAnalyzer(Histogram("Salary",where=query))                .addAnalyzer(Maximum('Salary',query))                .addAnalyzer(MaxLength('E Mail',query))                .addAnalyzer(Mean('Salary',query))                .addAnalyzer(Minimum('Salary',query))                .addAnalyzer(MinLength('E Mail',query))                .addAnalyzer(MutualInformation(['Age in Yrs','Age in Company (Years)'], query))                .addAnalyzer(StandardDeviation('Salary',query))                .addAnalyzer(Sum('Salary',query))                .addAnalyzer(Uniqueness(['Age in Yrs','Age in Company (Years)'],query))                .addAnalyzer(UniqueValueRatio(['Age in Yrs','Age in Company (Years)'],query))                .addAnalyzer(PatternMatch('Phone No',r'\\d{3}-\\d{3}-\\d{4}',where=query))                 .run()

myresult_df = AnalyzerContext.successMetricsAsDataFrame(spark, myresult)
myresult_df.show()


# In[9]:


import pyspark.sql.functions as F
from pyspark.sql.types import StringType

def addUpdateRepository(tag_value: str ='my new column',df_metrics: DataFrame= None):
               
        df_metrics = df_metrics.withColumn('tag',F.lit(tag_value))                                .withColumn('timestamp',F.current_timestamp().cast(StringType()))
        df_metrics.write.format('jdbc')                   .options(**Database_Connect(section='pyspark')
                           .read_db_config().get_config()).mode('append').save()


# In[10]:


#add analysisresult dataframe to database
addUpdateRepository('analyzer',myresult_df)


# In[12]:


def getMonths_df():
    """
    get the months from database
    """
    months_df =spark.read.format('jdbc').options(**Database_Connect(section='pyspark').read_db_config().set_config('dbtable','months').get_config()).load()
    print(months_df)
    return months_df

mylist=getMonths_df().select('months_val').rdd.map(lambda row : row[0]).collect()
#mylist1=list(getMonths_df().select('months_val').toPandas()['months_val'])
#print(mylist1)


# In[13]:


def size_func(x):
    return x >= df.count()

def completeness_func(x):
    a=df.where((df['Salary']>100000) & (df['Age in Yrs'] >10)).count()
    b=df.count()
    return x ==a/b


# In[14]:


from pydeequ.checks import *
from pydeequ.verification import *

key_tags = {'tag': 'verification_Check'}
resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)



_check = Check(spark, CheckLevel.Error, "Review Check")          .hasSize(lambda x: size_func(x),'size is not greater than or equal to 1000')          .hasCompleteness("Emp ID",lambda x: completeness_func(x),'Salary and Last_percent_Hike_Cleaned completeness fails for 100000 and 10 ' )          .isComplete("First Name", 'No values should be empty')          .isComplete("Month of Joining")          .isContainedIn("Month of Joining",mylist)          .isComplete("Phone No")          .containsEmail("E Mail")          .containsSocialSecurityNumber("SSN")          .isUnique("Emp ID")
         

checkResult = VerificationSuite(spark)     .onData(df)     .addCheck(_check)     .useRepository(repository)     .saveOrAppendResult(resultKey)     .run()


# In[15]:


checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()


# In[16]:


checkResult_df1 = VerificationResult.successMetricsAsDataFrame(spark, checkResult)
checkResult_df1.show()


# In[17]:


##insert verification analysis in database
addUpdateRepository('verification_Check',checkResult_df1)


# In[ ]:




