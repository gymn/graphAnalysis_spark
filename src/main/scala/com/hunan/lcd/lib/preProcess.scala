package com.hunan.lcd.lib

import org.apache.spark.graphx._

/** 基于三角关系、高斯模糊的节点中心性计算
  * Created by hunan on 2017/4/10.
  */
object preProcess {

  def noiseReduction(graph:Graph[Double, Int],sigma: Double): Graph[Double, PartitionID] ={
    val phi_1 = 0.6826;  //积分值
    val weight_0: Double = phi_1/sigma; //自身权重
    val weight_1: Double = 1-weight_0  //邻居权重

    val nbrSum: VertexRDD[(Double, Int)] = graph.aggregateMessages[(Double,Int)]( //计算邻居加权和以及邻居数量
      ctx=>{
        ctx.sendToSrc((ctx.dstAttr*weight_1,1))
        ctx.sendToDst((ctx.srcAttr*weight_1,1))
      },(a,b)=>(a._1+b._1,a._2+b._2),TripletFields.All
    )
    graph.mapVertices((vid, attr)=>attr*weight_0).joinVertices(nbrSum)((vid, n1, n2)=>{ //自身和邻居求加权平均数作为最终的中心性度量
      n1+n2._1/n2._2
    })
  }

  /**
    * 计算每个节点的节点中心性：(t/n)*logn  t为三角关系数（邻居之间的连边数，n为度数）
    */
  def calculateCentrality(graph: Graph[Int, Int]): Graph[Double, Int] ={
    val triGraph = graph.triangleCount().mapVertices((vid,d)=>d.toDouble)
    val graph1: Graph[Double, PartitionID] = triGraph.joinVertices(triGraph.degrees)((vid, t, d)=>{
      (t/d)*Math.log(d)
    })
    graph1
  }

  def run(graph: Graph[Int, Int],sigma:Double):  Graph[Double,_] ={
    val graph1 = calculateCentrality(graph)
    noiseReduction(graph1,sigma)
  }
}
