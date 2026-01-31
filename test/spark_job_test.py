import sys
import os,pytest
from pyspark.sql.types import(
    StructType,StructField,IntergerType,StringType,DoubleType,BooleanType,
)
from pyspark.sql import SparkSession
from data.spark_job import process_transaction

@pytest.fixture(scope="module")
def spark():
    spark=SparkSession.builder \
       .appName("PysparkUnitTest") \
       .master('local["*"]') \
       .getOrCreate()
    yield spark
    spark.stop()

def TestTransactionProcessing():
   @pytest.fixture(autouse=True)
   def setup_data(self,spark):
       txn_schema=StructType([
           StructField("transaction_id",StringType(),False),
           StructField("cardholder_id",StringType(),False),
           StructField("merchant_id",StringType(),False),
           StructField("merchant_name",StringType(),False),
           StructField("merchant_category",StringType(),False),
           StructField("transaction_amount",StringType(),False),
           StructField("transaction_timestamp",StringType(),False),
           StructField("transaction_status",StringType(),False),
           StructField("fraud_flag",StringType(),False),
           StructField("merchant_location",StringType(),False),
       ])

       data = [
            ("T001","CH001","M001","Walmart","Groceries", 120.50, "2025-02-04T10:00:00Z","SUCCESS",False,"NY,USA"),
            ("T002","CH002","M002","Expedia","Travel", 9500.75, "2025-02-04T12:30:00Z","PENDING",True, "TO,CA"),
            ("T003","CH003","M003","Amazon","Shopping", 75.20,  "2025-02-04T15:45:00Z","FAILED", False,"SF,USA"),
        ]
       
       self.tranf_df=spark.createDataFrame(data,schema=txn_schema)

       card_schema=StructType([
           StructField("cardholder_id",StringType(),False),
           StructField("customer_name",StringType(),False),
           StructField("reward_points",IntergerType(),False),
           StructField("risk_score",DoubleType(),False),
          
       ])

       cards = [
            ("CH001","John Doe",4500,0.15),
            ("CH002","Jane Smith",1200,0.35),
            ("CH003","Ali Khan",8000,0.10),
        ]
       
       self.cards=spark.createDataFrame(cards,schema=card_schema)

       def test_transaction_category(self):
           def process_transaction()

