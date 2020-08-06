//import java.util.Random

//import java.util.Random

import java.sql.{Date, Timestamp}
import java.text.DateFormat
import java.time.Instant

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

//import scala.util.Random

object RunAppDemo {
  def main(args: Array[String]) {


    val curr = System.currentTimeMillis()
    val a = 2000 * 24 * 60 * 60 * 1000L
    val b = curr - a
    println(curr)
    println(a)
    println(b)
    val random = Math.abs(Random.nextLong())
    println(random)
    val radio = random % a
    println(random % a)
    val radomdate = b + radio
    val timestamp = new Timestamp(radomdate)
    println(timestamp)


//    val hiveTableName = "test.person"
//    val hiveTableNameArr = hiveTableName.split("\\.")
//    val hiveWarehousePathStr = s"/user/hive/warehouse/${hiveTableNameArr(0)}.db/${hiveTableNameArr(1)}"
//    println(hiveWarehousePathStr)

//    val conf = new Configuration
//    val hdfs = FileSystem.get(conf)
//
//    val origPath = new Path(s"/tmp/test2")
//    val destPath = new Path("/tmp/test1")
//    hdfs.delete(origPath, true)
//    hdfs.rename(destPath, origPath)
//
//    hdfs.close()
  }
}
