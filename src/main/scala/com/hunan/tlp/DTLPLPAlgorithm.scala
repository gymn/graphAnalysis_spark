package com.hunan.tlp

import com.hunan.entity.{LinkScore, TEdge}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class DTLPLPAlgorithm(spark:SparkSession) {
  import spark.implicits._
  /**
    * 生成时序信息图
    * @param graphFilePath
    * @param narrowIndex
    * @return
    */
  def getTemporalInfoGraph(graphFilePath:String,narrowIndex:Double,numberOfWindows:Int,timeFormat:String): Graph[Int, Double] = {
    import spark.implicits._
    val rawData = spark.sparkContext.textFile(graphFilePath).map(_.split(",").map(_.stripPrefix("\"").stripSuffix("\"")))
      .map(arr=>TEdge(math.min(arr(0).toLong,arr(1).toLong),math.max(arr(0).toLong,arr(1).toLong),TlpUtil.convertTimeStr(arr(2),timeFormat))).toDS.cache()
    val startTime = rawData.selectExpr("min(timestamp) as start").collect()(0).getAs[Long]("start")
    val endTime = rawData.selectExpr("max(timestamp) as end").collect()(0).getAs[Long]("end")
    /**
      * 最新的切面的id为0，向历史时间方向递增
      */
    val interval = (endTime - startTime)/numberOfWindows
    def timestamp2snapshotId = udf((timestamp: Long)=> -1*(timestamp-endTime)/interval)
    val LinksWithSnapshotIdDS = rawData.select($"source",$"target",timestamp2snapshotId($"timestamp") as "snapshotId")

    /**
      * 1、将每个切片中的连边count
      * 2、每个snapshot中的link和前一个snapshot求取差值
      * 2、根据snapshotId为差值分配不同权重
      * 3、sum（count*权重） group by Edge
      */
    val linkCountForEachSnapshotDS = LinksWithSnapshotIdDS.groupBy($"source",$"target",$"snapshotId").count()
    linkCountForEachSnapshotDS.createOrReplaceTempView("linkCountForEachSnapshot")

    val EdgeRdd = spark.sql(
      """
        |SELECT tb1.source,tb1.target,tb1.snapshotId,ifnull(tb1.count,0)-ifnull(tb2.count,0) as diff
        |from linkCountForEachSnapshot tb1 left join linkCountForEachSnapshot tb2
        |on tb1.snapshotId+1=tb2.snapshotId and tb1.source=tb2.source and tb1.target=tb2.target""".stripMargin)
        .select($"source",$"target",$"diff"*pow(narrowIndex,$"snapshotId") as "snap_weight")
        .groupBy($"source",$"target").agg(sum("snap_weight") as "weight")
      .rdd.map(row=>Edge(row.getAs[Long]("source"),row.getAs[Long]("target"),row.getAs[Double]("weight"))).filter(_.attr>0)

    val vertexRDD:RDD[(VertexId,Int)] = EdgeRdd.flatMap(e => Seq((e.srcId,0),(e.dstId,0)))
    Graph(vertexRDD,EdgeRdd)//得到时序信息图
  }

  /**
    *基于pregel迭代计算每条边的预测权重
    */
  def getLinkScoresOnPregel(temporalInfoGraph:Graph[Int, Double]): Dataset[LinkScore] ={

    val maxIterations = 2  //迭代2次
    val initial = 1.0 //设置初始生命值

    type VTruple = (VertexId, Double)

    //节点数据更新函数
    def vprog(vid:VertexId, vdata:Array[VTruple], message:Array[VTruple])
    :Array[VTruple]={
      if(message.length==0)
        vdata
      else
        message
    }

    //发送消息
    def sendMsg(e:EdgeTriplet[Array[VTruple], Double])={
      //筛选出没有经过目标点的消息，并将添加节点信息、衰减生命值后的消息发送给目标节点
      val srcArr = e.dstAttr.map { t => if(t._1!=e.srcId) (t._1,t._2*e.attr) else null}.filter(_!=null)
      val dstArr = e.srcAttr.map { t => if(t._1!=e.dstId) (t._1,t._2*e.attr) else null}.filter(_!=null)

      if(srcArr.length==0 && dstArr.length==0)
        Iterator.empty
      else
        Iterator((e.dstId,dstArr),(e.srcId,srcArr))
    }

    //消息的合并
    def addArrs(arr1: Array[VTruple], arr2: Array[VTruple]): Array[VTruple] = arr1 ++ arr2

    //最后，调用pregel接口
    val newG = temporalInfoGraph.mapVertices((vid,_)=>Array[VTruple]( (vid, initial) )).pregel(Array[VTruple](), maxIterations, EdgeDirection.Out)(vprog, sendMsg, addArrs)

    //聚合节点收到的信息的函数
    def aggre(vid: VertexId,attr:Array[VTruple]):Array[(VertexId, Double)] = {
      val simMap: scala.collection.mutable.Map[VertexId, Double]= scala.collection.mutable.Map()
      //val count = attr.size
      for(t<-attr){
        simMap(t._1)=simMap.getOrElse(t._1,0.0)+t._2
      }
      simMap.toArray
    }

    def convertToEdgeScore(v:(VertexId, Array[(VertexId, Double)])): Array[LinkScore] ={
      val list = for(e <- v._2 if v._1<e._1) yield LinkScore(v._1,e._1,e._2)
      list
    }

    /**
      * 计算每个连边的评分
      */
    newG.mapVertices(aggre).vertices.flatMap(convertToEdgeScore).toDS()
  }
}
