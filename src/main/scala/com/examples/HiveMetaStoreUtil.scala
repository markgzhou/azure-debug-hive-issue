package com.examples

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.FieldSchema

import scala.collection.JavaConverters._

object HiveMetaStoreUtil {
  private var isClosed: Boolean = _
  private var hiveMetaStoreClient: HiveMetaStoreClient = _

  private def hiveConf: HiveConf = {
    new HiveConf
  }

  private def createHiveMetaStoreClient(): Unit = {
    try {
      this.hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)
      isClosed = false
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  def getHiveMetaStoreClient: HiveMetaStoreClient = {
    if (this.hiveMetaStoreClient == null) {
      createHiveMetaStoreClient()
    }
    if (isClosed) {
      this.hiveMetaStoreClient.reconnect()
    }
    this.hiveMetaStoreClient
  }

  def closeHiveMetaStoreClient(): Unit = {
    if (this.hiveMetaStoreClient != null && !isClosed) {
      this.hiveMetaStoreClient.close()
      isClosed = true
    }
  }

  def getHiveTableNonePartitionColNames(hiveDBName:String,tableName: String): Array[String] = {
    getHiveTableNonePartitionCols(hiveDBName, tableName).map(_.getName)
  }

  def getHiveTableNonePartitionCols(hiveDBName:String, tableName: String): Array[FieldSchema] = {
    getHiveMetaStoreClient
      .getTable( hiveDBName, tableName)
      .getSd
      .getCols
      .asScala
      .toArray
  }


}
