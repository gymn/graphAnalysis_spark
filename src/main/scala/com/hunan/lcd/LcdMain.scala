package com.hunan.lcd

import java.util.Properties

import com.hunan.entity.{LinkScore, Node}
import com.hunan.lcd.lib.{Diffusion, SearchCenter, preProcess}
import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LcdMain {
  def compute(spark:SparkSession, graphFilePath:String, algorithmArgs:Properties): Unit ={
    import spark.implicits._
    /**
      * attractThreshold:社团吸引度阈值
      * maxSteps: 寻找社团中心最大步数
      * seeds：种子节点
      */
    val attractThreshold = algorithmArgs.getProperty("attractThreshold").toDouble
    val maxSteps = algorithmArgs.getProperty("maxSteps").toInt
    val seeds = spark.sparkContext.parallelize(algorithmArgs.getProperty("seeds").split(",").map(_.toLong))

    val GSParameter = 1.0 //高斯分布参数

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(spark.sparkContext,graphFilePath,false,10).cache()
    /**
      * 预处理
      */
    val preGraph: Graph[Double, _] = preProcess.run(graph,GSParameter).cache()

    /**
      * 找到种子节点附近的社团中心
      */
    val centers: RDD[VertexId] = SearchCenter.run(preGraph,seeds,maxSteps)

    /**
      * 从社团中心扩张形成社团，最终获取潜在特征节点
      */
    val res: RDD[VertexId] = Diffusion.run(graph,centers,attractThreshold).union(centers).distinct()

    /**
      * 利用sparksql将结果格式化写入数据库
      */
    val inNodes = res.map((_,1)).toDF("id","type")
    val nodesDF = preGraph.vertices.toDF("id","symbolSize").join(inNodes,Seq("id"),"left").selectExpr("id","symbolSize","ifnull(type,0) as type")
      .map(row=>Node(row.getAs[Long]("id"),row.getAs[Long]("id").toString,row.getAs[Long]("id"),row.getAs[Int]("type"),row.getAs[Double]("symbolSize"),row.getAs[Int]("type"))).toDF()

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    nodesDF.write.jdbc("jdbc:mysql://localhost:3306/graph","lc_node",properties)

    graph.edges.map(e=>LinkScore(e.srcId,e.dstId,1.0)).toDF().write.jdbc("jdbc:mysql://localhost:3306/graph","lc_link",properties)
  }
}
