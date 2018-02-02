package com.hunan.tlp

import java.util.Properties

import com.hunan.entity.{LinkScore, Node}
import org.apache.spark.sql.{SaveMode, SparkSession}


object TlpMain {
  def compute(spark:SparkSession, algorithmArgs:Properties): Unit ={
    import spark.implicits._
    val narrowIndex = algorithmArgs.getProperty("narrowIndex").toDouble
    val timeFormat = algorithmArgs.getProperty("timeFormat","yyyy-MM-dd HH:mm:ss")
    val numberOfWindows = algorithmArgs.getProperty("numberOfWindows").toInt

    val dTLPLPAlgorithm = new DTLPLPAlgorithm(spark)
    val temporalInfoGraph = dTLPLPAlgorithm.getTemporalInfoGraph(algorithmArgs.getProperty("graphFilePath"),narrowIndex,numberOfWindows,timeFormat)
    val resLinks = dTLPLPAlgorithm.getLinkScoresOnPregel(temporalInfoGraph)

    val resNodes = resLinks.rdd.flatMap(link=>Seq(Node(link.source.substring(4).toLong,link.source,link.source.substring(4).toLong,0,10,0)
      ,Node(link.target.substring(4).toLong,link.target,link.target.substring(4).toLong,0,10,0))).toDF.distinct()

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    resLinks.toDF().write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","tlp_link",properties)
    resNodes.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","tlp_node",properties)

    /**
      * 写入测试数据集
      */
    resNodes.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","tlp_node_test",properties)
    val testLinks = spark.sparkContext.textFile(algorithmArgs.getProperty("testFilePath")).map(_.split(",")).map(arr=>{
      LinkScore("node"+arr(0),"node"+arr(1),1)
    }).toDF()
    testLinks.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","tlp_link_test",properties)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("tlp").config("spark.sql.shuffle.partitions",10).getOrCreate()
    val algorithmArgs = new Properties()
    algorithmArgs.setProperty("graphFilePath",args(0))
    algorithmArgs.setProperty("testFilePath",args(1))
    algorithmArgs.setProperty("narrowIndex",args(2))
    algorithmArgs.setProperty("timeFormat","yyyy-MM-dd HH:mm:ss")
    algorithmArgs.setProperty("numberOfWindows",args(3))

    compute(spark,algorithmArgs)
    spark.stop()
  }
}
