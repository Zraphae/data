package cn.com.ph.data.spark

import java.util.Collections

import cn.com.ph.data.spark.utils.{HexStringPartition, ParameterTool, RandomUtil}
import org.apache.commons.lang3.RandomUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminClient
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
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
        "--fields.terminated.by <fieldsTerminatedBy> " +
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
    val dataFileType = params.get("data.file.type", "text")
    val fieldsTerminatedBy = params.get("fields.terminated.by", "\u0001")
    val hbaseZookeeperQuorum = params.get("hbase.zookeeper.quorum")
    val hbaseTableNameStr = params.get("hbase.table.name")
    val hbaseFamilyName = params.get("hbase.family.name", "info")
    val hbaseRegionNum = params.getInt("hhbase.region.num")
    val idColName = params.get("id.col.name")
    val nullString = params.get("null.String", "\\N")
    val numSplitPerRegion = params.getInt("num.split.per.region", 20)
    val hbaseRSGroupName = params.get("num.split.per.region", DEFAULT_RS_GROUP_NAME)
    val hFilePath = params.get("hfile.path")
    val profilesActive = params.get("profiles.active")

    val spark = getSparkSession


    val hfilePathName = hbaseTableNameStr.split(":").mkString("_")
    val stagingFolder = s"$hFilePath/$hfilePathName"

    val conf = getConfigration(profilesActive)
    hdfsRm(stagingFolder, conf)

    val hConf = HBaseConfiguration.create(conf)
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", hbaseTableNameStr)

    reCreateHBaseTB(hbaseTableNameStr, hConf, hbaseFamilyName, hbaseRegionNum, hbaseRSGroupName)


    val sortedSchema = getSortedStructFieldsV2(confFilePath)
    val sortedSchemaWithIdx: Seq[(StructField, Int)] = sortedSchema.zipWithIndex

    var idKeysStructFields: Seq[(StructField, Int)] = List()
    for (i <- sortedSchemaWithIdx.indices) {
      if (idColName.toUpperCase.split(",").contains(sortedSchema(i))) {
        idKeysStructFields = idKeysStructFields :+ sortedSchemaWithIdx(i)
      }
    }

    spark.sparkContext.broadcast(idKeysStructFields)
    spark.sparkContext.broadcast(sortedSchemaWithIdx)

    val fileContext = spark.read.format(dataFileType).load(dataFilePath)
    val txtRDD: RDD[Array[String]] = fileContext.rdd.map(_.getString(0).split(fieldsTerminatedBy))
    val dataSet = spark.createDataFrame(txtRDD.map(Row.fromSeq(_)), StructType(sortedSchema))

    val pairsDataSet = dataSet.rdd.map(record => {
      var idValues: Seq[String] = List()
      for (i <- idKeysStructFields.indices) {
        idKeysStructFields(i)._1.dataType match {
          case _: DecimalType =>
            idValues = idValues :+ record.getAs[java.math.BigDecimal](idKeysStructFields(i)._2).toPlainString
          case _ =>
            var value = record.get(idKeysStructFields(i)._2)
            if (null == value) {
              value = "NULL" + System.currentTimeMillis() + RandomUtils.nextInt()
            }
            idValues = idValues :+ value.toString
        }
      }
      (RandomUtil.makeRowKey(idValues.mkString("_")), record)
    })


    val saltedRDD = pairsDataSet
      .repartitionAndSortWithinPartitions(new HexStringPartition(hbaseRegionNum * numSplitPerRegion))

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

    val hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.flatMapValues(_.iterator)
    hFileRdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf)

    val conn = ConnectionFactory.createConnection(hConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableNameStr))
    val load = new BulkLoadHFilesTool(hConf)
    load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(hbaseTableNameStr)))

  }


  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.executor.heartbeatInterval", "180s")
      .config("spark.reducer.maxReqsInFlight", 1)
      .config("spark.shuffle.io.retryWait", "90s")
      .config("spark.shuffle.io.maxRetries", 10)
      .config("spark.network.timeout", "300s")
      .getOrCreate()
  }

  def getConfigration(profilesActive: String): Configuration = {
    val conf = new Configuration
    if (profilesActive.toUpperCase.equals("PRD")) {
      conf.addResource("my-hdfs-site-prd.xml")
      conf.addResource("my-hbase-site-prd.xml")
    } else if (profilesActive.toUpperCase.equals("STG")) {
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

  def getSortedStructFieldsV2(confFilePatn: String): Seq[StructField] = {
    val stream = getClass.getResourceAsStream(confFilePatn)
    val yaml = new Yaml
    val yamlObjMap = yaml.load(stream).asInstanceOf[java.util.Map[String, String]].asScala

    val metaSynchColSchemas = yamlObjMap.get("metaSynchColSchemas").asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]].asScala

    if (metaSynchColSchemas.size < 1) {
      throw new Exception(" table schema config file parse error or not exists. ")
    }

    val sortedStructFields = metaSynchColSchemas.map(colSchema => {
      StructField(colSchema.get("columnName").toUpperCase, StringType, true)
    }).toList.sortBy(_.name)
    sortedStructFields
  }

  def mvTableRSGroup(hTableName: TableName, hConf: Configuration, hbaseRSGroupName: String) = {
    val hbaseAdminUser = "hbase_super_user"
    val hbaseSuperGroup = "supergroup"
    val userForTesting = User.createUserForTesting(hConf, hbaseAdminUser, Array[String](hbaseSuperGroup))
    val connection = ConnectionFactory.createConnection(hConf, userForTesting)

    val rsGroupAdminClient = new RSGroupAdminClient(connection)
    rsGroupAdminClient.moveTables(Collections.singleton(hTableName), hbaseRSGroupName)

    if (connection != null) {
      connection.close()
    }
  }

  def reCreateHBaseTB(hbaseTableNameStr: String, hConf: Configuration, hbaseFamilyName: String, hbaseRegionNum: Int, hbaseRSGroupName: String) = {
    val conn = ConnectionFactory.createConnection(hConf)
    val hTableName = TableName.valueOf(hbaseTableNameStr)
    val hAdmin = conn.getAdmin
    val tableExists = hAdmin.tableExists(hTableName)
    if (tableExists) {
      hAdmin.disableTable(hTableName)
      hAdmin.deleteTable(hTableName)
    }

    val familyDesc = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(hbaseFamilyName))
      .setCompressionType(Compression.Algorithm.SNAPPY)
      .build()
    val hTableDesc = TableDescriptorBuilder.newBuilder(hTableName).setColumnFamily(familyDesc).build()

    val splitALGO = new HexStringSplit
    val splitKeys = splitALGO.split(hbaseRegionNum)
    hAdmin.createTable(hTableDesc, splitKeys)

    if (!DEFAULT_RS_GROUP_NAME.equals(hbaseRSGroupName)) {
      mvTableRSGroup(hTableName, hConf, hbaseRSGroupName)
    }
    if (hAdmin != null) {
      hAdmin.close()
    }
    if (conn != null) {
      conn.close()
    }

  }
}
