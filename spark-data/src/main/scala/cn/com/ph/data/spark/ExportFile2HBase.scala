package cn.com.ph.data.spark

import java.util.UUID

import cn.com.ph.data.spark.utils.{HexStringPartition, ParameterTool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField}
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

object ExportFile2HBase {

  val DEFAULT_RS_GROUP_NAME = "default"

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters() < 8) {
      println("\nUsage: ExportFile2HBase " +
        "--conf.file.path <confFilePath> " +
        "--data.file.path <dataFilePath> " +
        "--data.file.type <dataFileType> " +
        "--hbase.zookeeper.quorum <hbaseZookeeperQuorum> " +
        "--hbase.table.name <hbaseTableName> " +
        "--hbase.family.name <hbase.family.name> " +
        "--hbase.region.num <hbaseRegionNum> " +
        "--id.col.name <idColName> " +
        "--null.String <nullString> " +
        "--num.split.per.region <numSplitPerRegion> " +
        "--hbase.rs.group.name <hbaseRSGroupName> " +
        "--hbase.file.path <hbaseFilePath> " +
        "--profiles.active <profilesActive>")
      return
    }

    val confFilePath = params.get("conf.file.path")
    val dataFilePath = params.get("data.file.path")
    val dataFileType = params.get("conf.file.type")
    val hbaseZookeeperQuorum = params.get("hbase.zookeeper.quorum")
    val hbaseTableNameStr = params.get("hbase.table.name")
    val hbaseFamilyName = params.get("hbase.family.name")
    val hbaseRegionNum = params.getInt("hhbase.region.num")
    val idColName = params.get("id.col.name")
    val nullString = params.get("null.String", "\\N")
    val numSplitPerRegion = params.getInt("num.split.per.region", 10)
    val hbaseRSGroupName = params.get("num.split.per.region", DEFAULT_RS_GROUP_NAME)
    val hFilePath = params.get("hfile.path")
    val profilesActive = params.get("profiles.active")

    val spark = SparkSession.builder
      .getOrCreate()


    val hfilePathName = hbaseTableNameStr.split(":").mkString("_")
    val stagingFolder = s"$hFilePath/$hfilePathName"

    val conf = getConfigration(profilesActive)
    hdfsRm(stagingFolder, conf)

    val sortedSchema = getSortedStructFields(confFilePath)
    val sortedSchemaWithIdx: Seq[(StructField, Int)] = sortedSchema.zipWithIndex

    var idKeysStructFields: Seq[(StructField, Int)] = List()
    for (i <- sortedSchemaWithIdx.indices) {
      if (idColName.toUpperCase.split(",").contains(sortedSchema(i))) {
        idKeysStructFields = idKeysStructFields :+ sortedSchema(i)
      }
    }

    val fileContext = spark.read.format(dataFileType).load(dataFilePath)
    val pairsDataSet = fileContext.rdd.map(record => (UUID.randomUUID().toString, record))
    val saltedRDD = pairsDataSet.repartitionAndSortWithinPartitions(new HexStringPartition(hbaseRegionNum))

    val nullStrBytes = Bytes.toBytes(nullString)
    val familyNameBytes = Bytes.toBytes(hbaseFamilyName)
    val rdd: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = saltedRDD.map(record => {
      var kvList: Seq[KeyValue] = List()
      val rowKey = Bytes.toBytes(record._1)
      for (i <- sortedSchemaWithIdx.indices) {
        val colNameBytes = Bytes.toBytes(sortedSchemaWithIdx(i)._1.name)
        record._2.getString(sortedSchemaWithIdx(i)._2.toInt) match {
          case null => {
            val keyValue = new KeyValue(rowKey, familyNameBytes, colNameBytes, nullStrBytes)
            kvList = kvList :+ keyValue
          }
          case colValue: String => {
            val keyValue = new KeyValue(rowKey, familyNameBytes, colNameBytes, Bytes.toBytes(colValue.toString))
            kvList = kvList :+ keyValue
          }
        }
      }
      (new ImmutableBytesWritable(rowKey), kvList)
    })

    val hConf = HBaseConfiguration.create(conf)
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", hbaseTableNameStr)
    val hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.flatMapValues(_.iterator)
    hFileRdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf)

    val conn = ConnectionFactory.createConnection(hConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableNameStr))
    val load = new LoadIncrementalHFiles(hConf)
    load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(hbaseTableNameStr)))

  }

  def getConfigration(profilesActive: String): Configuration = {
    val conf = new Configuration
    if(profilesActive.toUpperCase.equals("PRD")){
      conf.addResource("my-hdfs-site-prd.xml")
      conf.addResource("my-hbase-site-prd.xml")
    }else if(profilesActive.toUpperCase.equals("STG")){
      conf.addResource("my-hdfs-site-stg.xml")
      conf.addResource("my-hbase-site-stg.xml")
    }


    conf
  }

  def hdfsRm(uri: String, conf: Configuration) {
    val path = new Path(uri);
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(uri), conf)
    if (hdfs.exists(path)) hdfs.delete(path, true)
  }

  def getSortedStructFields(confFilePatn: String): Seq[StructField] = {
    val stream = getClass.getResourceAsStream(confFilePatn)
    val yaml = new Yaml
    val kvs = yaml.load(stream).asInstanceOf[java.util.Map[String, String]].asScala
    val sortedStructFields: Seq[StructField] = kvs.map(kv => {
      StructField(kv._1.toUpperCase, StringType, true)
    }).toList.sortBy(_.name)

    sortedStructFields
  }
}
