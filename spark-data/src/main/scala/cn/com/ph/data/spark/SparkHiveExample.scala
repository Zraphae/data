/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.com.ph.data.spark

// $example on:spark_hive$

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}
// $example off:spark_hive$


object SparkHiveExample {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)

  // $example off:spark_hive$

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .config("hive.exec.dynamic.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()


    //    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")


//    spark.sparkContext.setCheckpointDir("hdfs://pengzhaos-MacBook-Pro.local:9000/tmp/")
    val hiveTableName = "test.test_partition"
//    val hiveTmpTableName = s"${hiveTableName}_delta_tmp"
//    spark.sql(s"create table if not exists $hiveTmpTableName like $hiveTableName")

    val tableDataFrame = spark.sql(s"select * from $hiveTableName")
    tableDataFrame.show(10)

    val dataTmpPathStr = s"hdfs://pengzhaos-MacBook-Pro.local:9000/tmp/test_partition"
    tableDataFrame
      .write
      .format("ORC")
      .partitionBy("tel")
      .mode(SaveMode.Overwrite)
      .save(dataTmpPathStr)

    spark.sql(s"truncate table $hiveTableName")


    val dataPathStr = s"hdfs://pengzhaos-MacBook-Pro.local:9000/user/hive/warehouse/test.db/"
    val conf = new Configuration
    val dataTmpPath = new Path(dataTmpPathStr)
    val dataPath = new Path(dataPathStr)
    val hdfs = dataTmpPath.getFileSystem(conf)

    hdfs.rename(dataTmpPath, dataPath)

    spark.sql(s"msck repair table $hiveTableName")



//    val conf = new Configuration
//    val path = new Path(dataTmpPath)
//    val hdfs = path.getFileSystem(conf)
//    val fileList = hdfs.listStatusIterator(new Path(dataTmpPath))
//    var list: Seq[String] = List()
//    while(fileList.hasNext){
//      val fileStatus = fileList.next
//      if(fileStatus.isDirectory){
//        list = list :+ fileStatus.getPath.getName
//      }
//    }
//    list.foreach(println(_))
//
//    val partitionList = spark.sparkContext.parallelize(list, 10)
//
//    println(s"===============>${partitionList.getNumPartitions}")
//
//    partitionList.foreach(partitionName => {
//      println(s"===========>$partitionName")
//      val split = partitionName.split("=")
//      spark.sql(s"""load data inpath '$dataTmpPath/$partitionName' overwrite into table test.person_partation partition (${split(0)} = '${split(1)}')""")
//    })



//    spark.sql(s"""load data inpath '$dataTmpPath' overwrite into table test.person_partation""")

    //    tableDataFrame.persist(StorageLevel.DISK_ONLY)

//    spark.sql(s"truncate table $hiveTableName")

//    tableDataFrame
//      .checkpoint(true)
//      .write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(hiveTmpTableName)

//    spark.sql(s"DROP TABLE $hiveTableName")
//    spark.sql(s"ALTER TABLE $hiveTmpTableName RENAME TO $hiveTableName")

    //    val structType: StructType = spark.table(hiveTableName).schema
    //    structType.foreach(structField => println(s"======>name1: ${structField.name}"))

    //    val hiveTableSchema1: StructType = new StructType
    //    val hiveTableSchema1: Seq[StructField] = structType
    //      .sortBy(_.name.toUpperCase)
    //    hiveTableSchema1.foreach(structField => println(s"=====>name2: ${structField.name}"))

    //    val sortedStructFields: Array[StructField] = spark.table(hiveTableName).schema.sortBy(_.name.toUpperCase).toArray
    //    val sortedStructType = new StructType(sortedStructFields)
    //    sortedStructType.foreach(structField => println(s"=======>${structField.name}"))
    //
    //    val map: util.TreeMap[String, Any] = new util.TreeMap[String, Any]()
    //    sortedStructType.foreach(structField => map.put(structField.name, structField))
    //    map.forEach((key, value) => {
    //      println(s"====>key: $key, value: $value")
    //    })

    //    val list = List("CREATE_DATE", "CREATED_BY")
    //    val map: util.TreeMap[String, Any] = new util.TreeMap[String, Any]()
    //    list.foreach(item => map.put(item, item))
    //    map.forEach((key, value) => {
    //      println(s"====>key: $key, value: $value")
    //    })


    //    val hiveSchema = spark.table(hiveTableName).schema
    //    hiveSchema.foreach(println(_))
    //
    //    println("==================================")
    //
    //    hiveSchema.foreach(structField => {
    //      val dataType = structField.dataType
    //      dataType match {
    //        case StringType =>
    //          println(s"==========>StringType")
    //        case TimestampType =>
    //          println(s"==========>TimestampType")
    //        case decimalType if decimalType.typeName.toUpperCase.contains("DECIMAL") =>
    //          println(s"==========>TimestampType")
    //        case DateType =>
    //          println(s"==========>DateType")
    //        case IntegerType =>
    //          println(s"==========>IntegerType")
    //        case _ =>
    //          println(s"==========>default case: ${dataType.typeName}")
    //      }
    //
    //      println(s"name: ${structField.name}, dataType: ${structField.dataType}")
    //    })


    spark.stop()
    // $example off:spark_hive$
  }


}
