package cn.com.ph.data.spark

import org.apache.hadoop.hbase.client.Append
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkHudi {

  def main(args: Array[String]) {

    val basePath = "/user/hive/warehouse/ods_user_event"

    val spark = SparkSession.builder.appName("hudi").master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", 4)
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport().getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.createDataFrame(Seq(
      ("uid6", "name", "addr6new","update_time2","32232312"),
      ("uid7", "name", "addr7new","update_time2","42232312")
    )) toDF("uid", "name", "addr","update_time","ts")

//    df.write
//      .format("org.apache.hudi")
//      .option("hoodie.embed.timeline.server",false)
//      .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "uid")
//      .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "name")
//      .option(HoodieWriteConfig.TBL_NAME.key, "ods_user_event")
//      .mode(SaveMode.Append)
//      .save(basePath)


//
//    val df = spark.read
//      .format("org.apache.hudi")
////      .option("as.of.instant", "20220117083045637")
//      .load(basePath)
//
//    df.createOrReplaceTempView("test")
//////    spark.sql("select * from test where _hoodie_commit_time < 20220114174624247").show(10)
//    spark.sql("select * from test").show(10)



//    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from test order by commitTime").map(k => k.getString(0)).take(50)
//    val beginTime = commits(commits.length - 3) // commit time we are interested in
//    // incrementally query data
//    val tripsIncrementalDF = spark.read.format("org.apache.hudi").
//      option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
//      option(QUERY_TYPE.key, beginTime).
//      load(basePath)
//    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")
//
//    spark.sql("select `_hoodie_commit_time`, name from  hudi_trips_incremental").show()


    //delete
    val delDf = spark.sql("select * from test where _hoodie_partition_path='namenew7'")

    delDf.write.format("hudi").
      option("hoodie.embed.timeline.server",value = false).
      option(OPERATION.key,"delete").
      option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "uid").
      option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "name").
      option(HoodieWriteConfig.TBL_NAME.key, "ods_user_event").
      mode(SaveMode.Append).
      save(basePath)

    spark.stop()
  }

}
