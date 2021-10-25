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

    val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

    val df = spark.sql(
      """
        |SELECT `id`,  `uid`,  `role`,  `watchtime`,  `watchtimes`,  `nickname`,  `mobile`,  `hospital`,  `office`,  `jobtitle`,  `videoid`,  `speaker`,  `doctorcode`,  `amscode`,  `isspeaker`,  `joinType`,  `oce_doctorname`,  `oce_hospital`,  `oce_office`,  `oce_doctorcode` , "insert" as delta_op , now() as current_timestamp
        | FROM
        | (
        |  select `id`,  `uid`,  `role`,  `watchtime`,  `watchtimes`,  `nickname`,  `mobile`,  `hospital`,  `office`,  `jobtitle`,  `videoid`,  `speaker`,  `doctorcode`,  `amscode`,  `isspeaker`,  `joinType`,  `oce_doctorname`,  `oce_hospital`,  `oce_office`,  `oce_doctorcode` from pre_ods.emt_report
        |    where  task_run_time = (select max(task_run_time) FROM pre_ods.emt_report
        | WHERE data_date=20211022)
        | AND data_date=20211022
        |  EXCEPT ALL
        |  (
        |  select `id`,  `uid`,  `role`,  `watchtime`,  `watchtimes`,  `nickname`,  `mobile`,  `hospital`,  `office`,  `jobtitle`,  `videoid`,  `speaker`,  `doctorcode`,  `amscode`,  `isspeaker`,  `joinType`,  `oce_doctorname`,  `oce_hospital`,  `oce_office`,  `oce_doctorcode` from pre_ods.emt_report
        |    where  task_run_time = (select max(task_run_time) FROM pre_ods.emt_report
        | WHERE data_date=20211021)
        | AND data_date=20211021
        |  )
        |)
        |""".stripMargin)

    df.show()
  }
}