package com.hunan.lcd

import java.util.Properties

import com.hunan.entity.{LinkScore, Node}
import com.hunan.lcd.lib.{Diffusion, SearchCenter, preProcess}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object LcdMain {
  def compute(spark:SparkSession, algorithmArgs:Properties): Unit ={
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

    val rdd = spark.sparkContext.textFile(algorithmArgs.getProperty("graphFilePath"))
    val edgeRdd =rdd.map(_.split(",")).map(arr=>Edge(arr(0).toLong,arr(1).toLong,1)).repartition(10)

    val graph: Graph[Int, Int] = Graph.fromEdges(edgeRdd,1).cache()
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
    val narrowCoefficient: Double = 30/preGraph.vertices.values.max()
    val inNodes = res.map((_,1)).toDF("id","type")
    val nodesDF = preGraph.vertices.toDF("id","symbolSize").join(inNodes,Seq("id"),"left").selectExpr("id","symbolSize","ifnull(type,0) as type")
      .map(row=>Node(row.getAs[Long]("id"),"node"+row.getAs[Long]("id").toString,row.getAs[Long]("id"),row.getAs[Int]("type"),row.getAs[Double]("symbolSize")*narrowCoefficient,row.getAs[Int]("type"))).toDF()

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    nodesDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","lcd_node",properties)

    val edgeDF = graph.edges.map(e=>LinkScore("node"+e.srcId,"node"+e.dstId,1.0)).toDF()
    edgeDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","lcd_link",properties)
    edgeDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","lcd_link_test",properties)

    /**
      * 将测试数据写入数据库
      */
    val testData = spark.sparkContext.textFile(algorithmArgs.getProperty("testFilePath")).map(_.split(",")).map(arr=>{
      Node(arr(0).toLong,"node"+arr(0),arr(0).toLong, arr(1).toInt,10,arr(1).toInt)
    }).toDF
    testData.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/graph","lcd_node_test",properties)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("lcd").config("spark.sql.shuffle.partitions",10).getOrCreate()
    val properties = new Properties()
    properties.setProperty("graphFilePath",args(0))
    properties.setProperty("testFilePath",args(1))
    properties.setProperty("attractThreshold",args(2))
    properties.setProperty("maxSteps",args(3))
    properties.setProperty("seeds",args(4))

    compute(spark,properties)
    spark.stop()
  }
}
