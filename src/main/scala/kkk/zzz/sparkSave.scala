package kkk.zzz

import org.apache.spark.sql.{Dataset, SparkSession}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * spark存数据集合入hive
 */
object sparkSave {

  System.setProperty("HADOOP_USER_NAME", "hive")

  /*
      val sparklocal: SparkSession = SparkSession.builder().master("local").appName("datahubDDDemo")
      .config("spark.sql.warehouse.dir", "hdfs://172.16.32.154:8020/user/hive/warehouse")
      // 如果不加的话，会导致只能显示目录，不能读取表数据
      .config("dfs.client.use.datanode.hostname", "true")
      .config("spark.hadoop.hive.metastore.uris", "thrift://172.16.32.154:9083")
  //    .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()
      */

  val spark: SparkSession = SparkSession.builder().master("yarn").appName("datahubSLDemo")
    // 如果不加的话，会导致只能显示目录，不能读取表数据
    .config("dfs.client.use.datanode.hostname", "true").enableHiveSupport()
    .getOrCreate()


  def getSpark(): SparkSession = {
    spark.newSession()
  }

  def stop(): Unit = {
    spark.stop()
  }

  def saveToHive(dy: scala.collection.mutable.Seq[(String, String)]): Unit = {

    import spark.implicits._

    val senata: Dataset[(String, String)] = spark.createDataset(dy)

    if (senata.collect().length > 0) {
      senata.collect().foreach(println)
      println("======================================================================================================")
      val df1 = senata.toDF("c1", "c2")

      val tableName = "datahub_fff1"
      df1.createOrReplaceTempView(tableName)
      println("===========================================================================fffff======================")
      spark.sql("select c1,c2 from datahub_fff1").toDF("c1", "c2").collect().foreach(println)
      println("===========================================================================ccccc======================")
      //存入表中
      spark.sql("use default")
      spark.sql("set hive.exec.dynamici.partition=true")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      println("===========================================================================ccdddfffvvv======================")
      spark.sql("insert into table datahub10001 " +
        "select c1,c2 from datahub_fff1")

      println("===========================================================================ssssssssss======================")

    }
    //    val tempTable = sqlContext
    //      .read
    //      .format("json")
    //      .json(subRdd)
    //      .select(df1.columns.map(new Column(_)): _*)
    //      .coalesce(1)
    //      .write
    //      .mode(SaveMode.Append)
    //      .insertInto("datahub10001")
  }

}
