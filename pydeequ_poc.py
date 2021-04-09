#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark


# In[2]:


findspark.init('/home/ec2-user/spark-2.4.7-bin-hadoop2.7')


# In[3]:


from pyspark.sql import SparkSession, Row, DataFrame
import pydeequ


# In[24]:


##wrapper function for pydeequ

import pyspark.sql.functions as F
from pydeequ.pandas_utils import ensure_pyspark_df
import mysql.connector
from mysql.connector import Error
#from pydeequ.verification import VerificationResult


class MetricsRepository(pydeequ.repository.MetricsRepository):
    
    @classmethod
    def helper_metrics_file(cls, spark_session: SparkSession, filename: str = "metrics.json", path: str = None):
        """
        Helper method to create the metrics file for storage
        """
        if path is None:
            path = spark_session._jvm.com.google.common.io.Files.createTempDir()          
        f = spark_session._jvm.java.io.File(path, filename)
        f_path = f.getAbsolutePath()
        return f_path
    
    @classmethod
    def create_db_connection(cls):
        conn = None
        try:
            conn = mysql.connector.connect(host='host.us-east-1.rds.amazonaws.com',
                                           port='3306',
                                           database='pydeequ_metrics',
                                           user='<dbuser>',
                                           password='<dbpassword>')
            if conn.is_connected():
                return conn
        except Error as e:
            print(e)
            return None
        
    @classmethod
    def close_db_connection(cls, con):
        if con is not None and con.is_connected():
            con.close()
            

class FileSystemMetricsRepository(pydeequ.repository.FileSystemMetricsRepository,MetricsRepository):
    
    def __init__(self, spark_session: SparkSession, path: str = None):

        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        if not path: path = self.helper_metrics_file(self._spark_session)
        self.path = path
        self.deequFSmetRep = spark_session._jvm.com.amazon.deequ.repository.fs.FileSystemMetricsRepository
        self.repository = self.deequFSmetRep(self._jspark_session, path)
        super().__init__(spark_session, path)

class DatabaseMetricsRepository(MetricsRepository):
    
    def __init__(self, con):
        self.con = con
        #TODO create connection and store it it self_con
        cursor = self.con.cursor()
        self.metric_table =cursor.execute("CREATE TABLE IF NOT EXISTS "                                           "`metrics`(`entity` varchar(255), `instance` varchar(255), "                                           "`name` varchar(255), `value` float, `tag` varchar(255), "                                           "`timestamp` varchar(255) ) ")

        

class AnalysisRunner:
    """
    Runs a set of analyzers on the data at hand and optimizes the resulting computations to minimize
    the number of scans over the data. Additionally, the internal states of the computation can be
    stored and aggregated with existing states to enable incremental computations.
    :param spark_session SparkSession: SparkSession
    """

    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session

    def onData(self, df):
        """
        Starting point to construct an AnalysisRun.
        :param dataFrame df: tabular data on which the checks should be verified
        :return: new AnalysisRunBuilder object
        """
        df = ensure_pyspark_df(self._spark_session, df)
        return AnalysisRunBuilder(self._spark_session, df)
    
class AnalyzerContext(pydeequ.analyzers.AnalyzerContext):
    
    @classmethod
    def addUpdateRepository(self,spark_session: SparkSession, tag_value: str ='my new column',df_metrics: DataFrame= None):
               
        df_metrics = df_metrics.withColumn('tag',F.lit(tag_value))                                .withColumn('timestamp',F.current_timestamp().cast(StringType()))
        df_metrics.write.format('jdbc').options(
        url='jdbc:mysql://host.us-east-1.rds.amazonaws.com:3306/pydeequ_metrics',
        driver='com.mysql.jdbc.Driver',
        dbtable='metrics',
        user='<dbuser>',
        password='<dbpassword>').mode('append').save()
        return self

class AnalysisRunBuilder(pydeequ.analyzers.AnalysisRunBuilder):
    
    def __init__(self, spark_session: SparkSession, df: DataFrame ):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        self._df = df
        self._AnalysisRunBuilder = self._jvm.com.amazon.deequ.analyzers.runners.AnalysisRunBuilder(df._jdf)
        super().__init__(spark_session, df)
        
    
class VerificationResult(pydeequ.verification.VerificationResult):
    
    @classmethod
    def addUpdateRepository(self,spark_session: SparkSession, tag_value: str ='my new column',df_metrics: DataFrame= None):
               
        df_metrics = df_metrics.withColumn('tag',F.lit(tag_value))                                .withColumn('timestamp',F.current_timestamp().cast(StringType()))
        df_metrics.write.format('jdbc').options(
        url='jdbc:mysql://host.us-east-1.rds.amazonaws.com:3306/pydeequ_metrics',
        driver='com.mysql.jdbc.Driver',
        dbtable='metrics',
        user='<dbuser>',
        password='<dbpassword>').mode('append').save()
        return df_metrics
    


# In[5]:


##create spark session
spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())


# In[6]:


#load data
df = spark.read.csv('/home/ec2-user/spark-2.4.7-bin-hadoop2.7/examples/src/main/resources/1000 Records.csv',header=True)


# In[7]:


df.printSchema()
df.show(1)


# In[8]:


from pyspark.sql.functions import col
from pyspark.sql.types import (StructField, IntegerType,DoubleType,
                               StringType,StructType)

df =df.withColumn("Salary",col("Salary").cast(DoubleType()))
df =df.withColumn("Date of Joining",col("Date of Joining").cast(IntegerType()))
df =df.withColumn('Age in Company (Years)',col('Age in Company (Years)').cast(IntegerType()))
df=df.withColumnRenamed('Weight in Kgs.','Weight in Kgs')      .withColumnRenamed('Age in Yrs.','Age in Yrs')      .withColumnRenamed('Phone No. ','Phone No')
df=df.withColumn('Weight in Kgs',col('Weight in Kgs').cast(DoubleType()))      .withColumn('Age in Yrs',col('Age in Yrs').cast(DoubleType()))      .withColumn('Month of Joining',col('Month of Joining').cast(IntegerType()))      .withColumn('Phone No',col('Phone No').cast(IntegerType()))


# In[9]:


df.printSchema()


# In[10]:


#store data in database
con = DatabaseMetricsRepository.create_db_connection()
db_repo = DatabaseMetricsRepository(con)


# In[11]:


#store data in file
from pydeequ.analyzers import ResultKey

metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, 'verify_newfile.json','/home/ec2-user/')
repository = FileSystemMetricsRepository(spark, metrics_file)
key_tags = {'tag': 'analyzer'}

resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)


# In[12]:


from pydeequ.analyzers import (Completeness, Compliance, ApproxCountDistinct,
                               Size,Mean, Correlation, MutualInformation, PatternMatch)
######Profiling#############
analysisResult = AnalysisRunner(spark)                     .onData(df)                     .addAnalyzer(Size())                     .addAnalyzer(Completeness("Emp ID"))                     .addAnalyzer(ApproxCountDistinct('Emp ID'))                     .addAnalyzer(Mean('Salary'))                     .addAnalyzer(Completeness('Month of Joining'))                     .addAnalyzer(Compliance("Salary greater than 10000","Salary >=55000"))                     .addAnalyzer(Correlation('Age in Company (Years)','Age in Yrs'))                     .addAnalyzer(Correlation('Age in Yrs','Salary'))                     .addAnalyzer(Correlation('Age in Yrs','Weight in Kgs'))                     .addAnalyzer(MutualInformation(['Age in Yrs','Weight in Kgs']))                     .addAnalyzer(MutualInformation(['Age in Yrs','Age in Company (Years)']))                     .useRepository(repository)                     .saveOrAppendResult(resultKey)                     .run()


# In[13]:


repository.path


# In[14]:


##refactor analysis result as dataframe
analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)


# In[15]:


analysisResult_df.show(truncate=False)


# In[16]:


#add analysisresult dataframe to database
AnalyzerContext.addUpdateRepository(spark,'analyzer',analysisResult_df)


# In[17]:


#########verification and check


# In[18]:


#get data from database table; to be passed on to verificationsuite
def getMonths_df():
    months_df =spark.read.format('jdbc').options(
        url='jdbc:mysql://host.us-east-1.rds.amazonaws.com:3306/pydeequ_metrics',
        driver='com.mysql.jdbc.Driver',
        dbtable='months',
        user='<dbuser>',
        password='<dbpassword>').load()
    print(months_df)
    return months_df
    
mylist=getMonths_df().select('months_val').rdd.map(lambda row : row[0]).collect()
mylist1=list(getMonths_df().select('months_val').toPandas()['months_val'])
print(mylist1)


# In[19]:


from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.checks import *

key_tags = {'tag': 'verification_Check'}
resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)


_check = Check(spark, CheckLevel.Error, "Review Check")          .hasSize(lambda x: x>= 1000)          .isComplete("First Name")          .isComplete("Month of Joining")          .isContainedIn("Month of Joining",mylist)          .isComplete("Phone No")          .containsEmail("E Mail")          .containsSocialSecurityNumber("SSN")          .isUnique("Emp ID")

checkResult = VerificationSuite(spark)     .onData(df)     .addCheck(_check)     .useRepository(repository)     .saveOrAppendResult(resultKey)     .run()


# In[20]:


checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult, pandas=True)
checkResult_df


# In[21]:


checkResult_df1 = VerificationResult.successMetricsAsDataFrame(spark, checkResult)


# In[22]:


checkResult_df1.show()


# In[25]:


##insert verification analysis in database
VerificationResult.addUpdateRepository(spark,'verification_Check',checkResult_df1)


# In[26]:


#load the metrics file

result_metrep_df = repository.load()     .before(ResultKey.current_milli_time())     .getSuccessMetricsAsDataFrame()
result_metrep_df.show()


# In[27]:


####constraint suggestion
from pydeequ.suggestions import *

suggestionResult = ConstraintSuggestionRunner(spark)              .onData(df)              .addConstraintRule(DEFAULT())              .run()

# Constraint Suggestions in JSON format
print(json.dumps(suggestionResult, indent=2))


# In[28]:


##show suggestion matrix as dataframe
for key, val in suggestionResult.items():
    print(key)

k =suggestionResult.get('constraint_suggestions')
#print(k)


# In[29]:


import pandas as pd

df_suggestion = pd.DataFrame.from_dict(k)
df_suggestion.head(5)


# In[ ]:




