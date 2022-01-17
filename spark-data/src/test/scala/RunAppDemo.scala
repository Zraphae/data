////import java.util.Random
//
////import java.util.Random
//
//import java.sql.{Date, Timestamp}
//import java.text.DateFormat
//import java.time.Instant
//import java.util.stream.IntStream
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.hive.ql.plan.HiveOperation
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import org.apache.spark.sql.execution.QueryExecution
//import org.apache.spark.sql.execution.command.{AnalyzeColumnCommand, ShowColumnsCommand, ShowCreateTableCommand, ShowDatabasesCommand, ShowFunctionsCommand, ShowPartitionsCommand, ShowTablePropertiesCommand, ShowTablesCommand}
//
//import scala.util.Random
//
////import scala.util.Random
//
//object RunAppDemo {
//  def main(args: Array[String]) {
//
//
////    val curr = System.currentTimeMillis()
////    val a = 2000 * 24 * 60 * 60 * 1000L
////    val b = curr - a
////    println(curr)
////    println(a)
////    println(b)
////    val random = Math.abs(Random.nextLong())
////    println(random)
////    val radio = random % a
////    println(random % a)
////    val radomdate = b + radio
////    val timestamp = new Timestamp(radomdate)
////    println(timestamp)
//
//
////    val hiveTableName = "test.person"
////    val hiveTableNameArr = hiveTableName.split("\\.")
////    val hiveWarehousePathStr = s"/user/hive/warehouse/${hiveTableNameArr(0)}.db/${hiveTableNameArr(1)}"
////    println(hiveWarehousePathStr)
//
////    val conf = new Configuration
////    val hdfs = FileSystem.get(conf)
////
////    val origPath = new Path(s"/tmp/test2")
////    val destPath = new Path("/tmp/test1")
////    hdfs.delete(origPath, true)
////    hdfs.rename(destPath, origPath)
////
////    hdfs.close
//
//    val spark = SparkSession
//      .builder()
//      .master("local")
//      .appName("Spark Hive Example")
//      .config("hive.exec.dynamic.partition", true)
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val sql = "select * from test a left join test2 b on a.id=b.id where a.name>'123'"
//    val logicalPlan = spark.sessionState.sqlParser.parsePlan(sql)
//    val res = logicalPlan.toJSON
//    printf(res)
////    val childrenSize = res.children.size
////    IntStream.range(0,childrenSize).forEach(i => printf(res.children.apply(i).verboseString))
//
//
//
////    val logicalPlan: LogicalPlan = spark.sessionState.sqlParser.parsePlan(sql)
////    val queryExecution: QueryExecution = spark.sessionState.executePlan(logicalPlan)
////    val outputAttr: Seq[Attribute] = queryExecution.sparkPlan.output
////    val colNames: Seq[String] = outputAttr.map(a => a.name)
////    println(colNames)
//
//
//
////    logicalPlan match {
////      case _: ShowCreateTableCommand => print(1)
////      case _: ShowColumnsCommand => print(2)
////      case _: ShowDatabasesCommand => print(3)
////      case _: ShowFunctionsCommand => print(4)
////      case _: ShowPartitionsCommand => print(5)
////      case _: ShowTablesCommand => print(6)
////      case _: AnalyzeColumnCommand => print(7)
////      case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
////      case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
////      case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
////      case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
////      case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
////      case _: ShowTablesCommand => HiveOperation.SHOWTABLES
//////      case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIE
////    }
//
//
//
////    println(res.allAttributes.attrs)
//
////    val test1 = CatalystSqlParser.parseExpression("select * from test where id = '1'")
////    println(test1)
////    val structType = CatalystSqlParser.parseTableSchema("select * from test where id = '1'")
////    println(structType)
////    print(CatalystSqlParser.parseTableSchema(sql))
//
//  }
//}
