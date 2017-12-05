package com.hunan.lcd.lib

import com.hunan.lcd.po.VertexStates
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/** 基于随机游走的社团中心发现
  * Created by hunan on 2017/4/10.
  */
object SearchCenter {
  /**
    * 初始化所有节点的状态：选取搜索起始点、初始化节点每一步的下一次搜索方向
    */
  def initialNode(densityGraph:Graph[Double,_],seeds:RDD[VertexId]):Graph[VertexStates,_] ={
    //初始化节点标签
    val seedsVertex = seeds.map(vid=>(vid,true))

    //确定每个节点的下一步搜索方向 todo 随机化？？
    val direct: VertexRDD[Long] = densityGraph.aggregateMessages[(VertexId,Double)](
      ctx=>{
        if(ctx.dstAttr>ctx.srcAttr)
          ctx.sendToSrc((ctx.dstId,ctx.dstAttr))
        else
          ctx.sendToDst((ctx.srcId,ctx.srcAttr))
      },(a,b)=>{
        if(a._2>b._2) a else b
      }
    ).mapValues(v=>v._1)

    //整合信息
    densityGraph.outerJoinVertices(seedsVertex)((vid, data, u)=>{
      u match {
        case Some(b)=>VertexStates(data,b,0L,1)
        case None =>VertexStates(data,false,0L,0)
      }
    }).outerJoinVertices(direct)((vid, data, u)=>{
      u match {
        case Some(d)=>VertexStates(data.centrality,data.active,d,data.count)
        case None =>VertexStates(data.centrality,data.active,vid,data.count) //如果节点没有下一步信息，说明节点自己是一个极值点，下一步设置为自己id即可
      }
    })
  }

  /**
    * 搜索中心节点
    */
  def search(graph: Graph[VertexStates, _],maxSteps:Int): Graph[VertexStates,_] ={
    //节点计算
    def vertexProgram(vid:VertexId, data:VertexStates,message:VertexStates): VertexStates ={
      if(message.count<0) data   //初始化信息
      else if(data.nextStep==vid)VertexStates(data.centrality,true,vid,data.count+message.count)  //当前节点为中心节点
      else message.active match{         //一般情况
        case true=>VertexStates(data.centrality,message.active,data.nextStep,message.count)
        case false=>VertexStates(data.centrality,message.active,data.nextStep,0)
      }
    }
    //发送消息
    def sendMessage(edge:EdgeTriplet[VertexStates,_]) ={
      if(edge.srcAttr.active&&edge.srcAttr.nextStep==edge.dstId)
        Iterator((edge.dstId,VertexStates(0,true,0,edge.srcAttr.count)),(edge.srcId,VertexStates(0,false,0,0)))
      else if(edge.dstAttr.active&&edge.dstAttr.nextStep==edge.srcId)
        Iterator((edge.srcId,VertexStates(0,true,0,edge.dstAttr.count)),(edge.dstId,VertexStates(0,false,0,0)))
      else
        Iterator.empty
    }
    //合并中间消息
    def mergeMessage(a:VertexStates,b:VertexStates): VertexStates ={
      VertexStates(0,a.active||b.active,0,a.count+b.count)
    }
    val pregelRes: Graph[VertexStates,_] = graph.pregel(VertexStates(0,false,0,-1),maxSteps,EdgeDirection.Either)(
      vertexProgram,sendMessage,mergeMessage
    )
    pregelRes
  }

  /**
    * 搜索社团中心
    * @param densityGraph 原始图
    * @param seeds 种子节点
    * @return 返回社团中心节点id
    */
  def run(densityGraph:Graph[Double,_],seeds:RDD[VertexId],maxSteps:Int) ={
    val initialGraph = initialNode(densityGraph,seeds)

    search(initialGraph,maxSteps).vertices.filter(v=>v._2.active&&v._2.count!=0).map(v=>v._1)
  }
}
