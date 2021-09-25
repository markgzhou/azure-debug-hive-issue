package com.examples

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD


// define main method (Spark entry point)
object GetStarted {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(GetStarted.getClass.getName)
      .set("spark.sql.hive.metastore.version", "2.3.7").set("spark.sql.hive.metastore.jars", "builtin")
    val spark: SparkSession = SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    // Change Logging level
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)

    // print Hello World to logs
    println("Hello, world!")

    // show DBs
    spark.sql("SHOW DATABASES").show(false)

    // create database
    spark.sql("CREATE DATABASE IF NOT EXISTS azure_test ").show(false)

    //    create table
    spark.sql(
      """
        |create table IF NOT EXISTS azure_test.my_table
        |(
        |   my_id       Long      comment 'my id',
        |   my_value    string    comment 'my value'
        |)  using delta location '/tmp/data/azure_test/my_table';
        |""".stripMargin).show(false)

    // generate DataFrame with schema
    val rowsRdd: RDD[Row] = sc.parallelize(
      Seq(
        Row("first", 2.0, 7.0),
        Row("second", 3.5, 2.5),
        Row("third", 7.0, 5.9)
      )
    )

    val schema = new StructType()
      .add(StructField("name", StringType, true))
      .add(StructField("val1", DoubleType, true))
      .add(StructField("val2", DoubleType, true))

    val df = spark.createDataFrame(rowsRdd, schema)
    df.show(false)
    df.createOrReplaceTempView("myTempTable")

    spark.sql("create table IF NOT EXISTS azure_test.my_table2 as (select * from myTempTable)").show(false)

    print(HiveMetaStoreUtil.getHiveTableNonePartitionColNames("azure_test", "my_table2").mkString("Array(", ", ", ")"))
    print(HiveMetaStoreUtil.getHiveTableNonePartitionCols("azure_test", "my_table2").mkString("Array(", ", ", ")"))

    print(">>>>> COMPLETE!!!")
  }
}