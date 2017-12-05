package com.hunan.tlp

import java.util.Properties

import com.hunan.entity.Node
import org.apache.spark.sql.SparkSession


object TlpMain {
  def compute(spark:SparkSession, graphFilePath:String, algorithmArgs:Properties): Unit ={
    import spark.implicits._
    val narrowIndex = algorithmArgs.getProperty("narrowIndex").toDouble
    val timeFormat = algorithmArgs.getProperty("timeFormat")
    val numberOfWindows = algorithmArgs.getProperty("numberOfWindows").toInt

    val dTLPLPAlgorithm = new DTLPLPAlgorithm(spark)
    val temporalInfoGraph = dTLPLPAlgorithm.getTemporalInfoGraph(graphFilePath,narrowIndex,numberOfWindows,timeFormat)
    val resLinks = dTLPLPAlgorithm.getLinkScoresOnPregel(temporalInfoGraph)

    val resNodes = resLinks.rdd.flatMap(link=>Seq(Node(link.source,"node"+link.source,link.source,0,5,0)
      ,Node(link.target,"node"+link.target,link.target,0,5,0))).toDF

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    resLinks.toDF().write.jdbc("jdbc:mysql://localhost:3306/graph","tl_link",properties)
    resNodes.write.jdbc("jdbc:mysql://localhost:3306/graph","tl_node",properties)
  }
}
