package cn.com.ph.data.spark

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, FileFileFilter, FileFilterUtils}
import org.apache.spark.sql.SparkSession

import java.io.File

object SparkWordCount {

  def main(args: Array[String]) {

    val keyWord: List[String] = List("数据管理","数据挖掘","数据网络","数据平台","数据中心","数据科学","数字控制","数字技术",
        "数字通信","数字网络","数字智能","数字终端","数字营销","数字化","大数据","云计算","云 IT","云生态","云服务","云平台","区块链","物联网","机器学习")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark WordCount Example")
      .getOrCreate()

    val files = FileUtils.listFiles(new File("/Users/zhaopeng/Downloads/2019encode/"), FileFilterUtils.suffixFileFilter("encode"), null)
    files.forEach(file => wordCount(spark, keyWord, file))

    spark.stop()
  }

  def wordCount(spark : SparkSession, keyWord : List[String], file : File): Unit = {
    spark.read.text("file:/Users/zhaopeng/Downloads/2019encode/" + file.getName)
      .rdd.flatMap(line => line.mkString.split(" "))
      .filter(word => keyWord.contains(word))
      .map(word => (word, 1)).reduceByKey(_+_)
      .saveAsTextFile("file:/Users/zhaopeng/Downloads/result/" + file.getName.substring(0,6))
  }


}
