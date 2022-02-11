package cn.com.ph.data.spark

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkIceberg {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("iceberg").master("local[2]")
      .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hive_prod.type", "hive")
      .config("spark.sql.catalog.hive_prod.uri", "thrift://127.0.0.1:9083")
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .config("spark.default.parallelism", 2)
      .getOrCreate()

//      val spark = SparkSession.builder.appName("iceberg").master("local[2]")
//        .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
//        .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
//        .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://127.0.0.1:9000/user/hive/warehouse/")
//        .config("spark.default.parallelism", 2)
//        .getOrCreate()


    val df: DataFrame = spark.createDataFrame(Seq(
      (123, "name", "addr","update_time2","32232312"),
      (124, "name", "addr","update_time2","42232312")
    )) toDF("uid", "name", "addr","update_time","ts")

//    spark.sql(s"CREATE TABLE hive_prod.default.test_iceberg (uid bigint, name string, addr string, update_time string, ts string) USING iceberg")

//    df.writeTo("hive_prod.default.test_iceberg").overwritePartitions()
//    df.write
//      .format("iceberg")
//      .mode("overwrite")
//      .save("hive_prod.default.ods_user_event_ice")

//    df.writeTo("prod.default.test_iceberg")
//      .tableProperty("write.format.default", "orc")
//      .partitionedBy($"name")
//      .createOrReplace()


//    spark.table("hive_prod.default.ods_user_event_ice").show(10)
//    spark.sql("select * from hive_prod.default.ods_user_event_ice").show(10)
//    spark.sql("select * from hive_prod.default.ods_user_event_ice.snapshots").show(10)
    spark.sql("select * from hive_prod.default.ods_user_event_ice.history").show(10)
//    spark.sql(s"select h.made_current_at, s.operation, h.snapshot_id, h.is_current_ancestor, s.summary['spark.app.id'] from hive_prod.default.test_iceberg.history h join hive_prod.default.test_iceberg.snapshots s on h.snapshot_id = s.snapshot_id order by made_current_at").show(10)
//    spark.sql("select * from hive_prod.default.test_iceberg.manifests").show(10)

//    spark.read.format("iceberg").load("hive_prod.default.test_iceberg").show(10)

    //exception
//    spark.read.format("iceberg").load("hdfs://127.0.0.1:9000/user/hive/warehouse/test_iceberg/").show(10)

//    spark.read
//      .option("as-of-timestamp", "11499162860000")
//      .format("iceberg")
//      .load("hive_prod.default.test_iceberg")
//      .show(10)

//    spark.read
//      .format("iceberg")
//      .option("start-snapshot-id", "10963874102873")
//      .option("end-snapshot-id", "163874143573109")
//      .load("hive_prod.default.test_iceberg")
//      .show(10)

    spark.stop()
  }

}
