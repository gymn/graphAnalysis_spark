package com.hunan.lcd.lib

import com.hunan.lcd.po.{CommunityState, ComputeState, DiffusionState}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/** 基于批量适应度函数的潜在目标节点识别
  * Created by hunan on 2017/4/10.
  */
object Diffusion {
  /**
    * 从社团节点向外寻找潜在节点并纳入社团
    * @param graph 初始图
    * @param threshold 并入节点时判断节点是否归属社团的阈值
    * @return
    */
  def compute(graph: Graph[CommunityState,Int],threshold:Double): Graph[CommunityState,Int] ={
    val neighbors: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
    val degrees: VertexRDD[Int] = graph.aggregateMessages(ctx => { ctx.sendToSrc(ctx.attr); ctx.sendToDst(ctx.attr) }, _ + _,
      TripletFields.None)

    val graph2 = graph.outerJoinVertices(neighbors)((vid,data,u)=>{
      u match{
        case Some(u)=>DiffusionState(data.inCommunity,data.communityId,u)
        case None=>DiffusionState(data.inCommunity,data.communityId,Array())
      }
    }).outerJoinVertices(degrees)((vid,data,u)=>{
      u match {
        case Some(u)=>ComputeState(data.inCommunity,data.ommunityId,data.neignborIds,u)
        case None =>ComputeState(data.inCommunity,data.ommunityId,data.neignborIds,0)
      }
    })

    val vertx_2: VertexRDD[CommunityState] = graph2.aggregateMessages[(CommunityState,Double)](ctx=>{
      if(ctx.dstAttr.inCommunity&&ctx.srcAttr.inCommunity){  //连边两端点均为社团点
        if(/*ctx.srcAttr.neignborIds.intersect(ctx.dstAttr.neignborIds).size>0&&*/  //6.20 delete condition:如果两个点有共同邻居
        ctx.attr.toDouble/(ctx.srcAttr.degree+ctx.dstAttr.degree-ctx.attr)>0.5) { //且两社团之间的连边占总连边比例大于阈值 todo 改变阈值
          ctx.sendToDst((CommunityState(true, Math.min(ctx.dstId, ctx.srcId),0),0.5)) //如果两个社团点相邻且有三角关系则合并两个社团并以较小的社团id为id
          ctx.sendToSrc((CommunityState(true, Math.min(ctx.dstId, ctx.srcId),0),0.5))
        }
      } else if(ctx.dstAttr.inCommunity){   //dst为社团点
        if(ctx.attr.toDouble/ctx.srcAttr.degree>threshold) {
          ctx.sendToSrc((CommunityState(true, ctx.dstId,0),ctx.attr.toDouble/ctx.srcAttr.degree))
        }
      }else if(ctx.srcAttr.inCommunity)  //src为社团点
        if(ctx.attr.toDouble/ctx.dstAttr.degree>threshold) {
          ctx.sendToDst((CommunityState(true, ctx.srcId,0),ctx.attr.toDouble/ctx.srcAttr.degree))
        }
    },(c1,c2)=>{
      if(c1._2==c2._2)
        (CommunityState(true,Math.min(c1._1.communityId,c2._1.communityId),0),0.5)
      else if(c1._2>c2._2)
        (CommunityState(true,c1._1.communityId,0),c1._2)
      else
        (CommunityState(true,c2._1.communityId,0),c2._2)
    }).mapValues(_._1)

    val graph3 = graph.outerJoinVertices(vertx_2)((vid,data,u)=>{
      u match {
        case Some(u)=>CommunityState(u.inCommunity,u.communityId,data.insideEdges)
        case None=>data
      }
    }).cache()
    //println("compute:")
    //graph3.triplets.collect().foreach(println)

    graph3
  }

  /**
    * 一轮社团扩散后进行图压缩，将同一社团的所有点压缩成一个节点
    */
  def compress(graph: Graph[CommunityState,Int]): Graph[CommunityState,Int] ={

    //所有未加入社团的节点以及这些节点间的连边,将这些连边的值设置为1
    val edgeRDD2 = graph.subgraph(et=>(!et.dstAttr.inCommunity)&&(!et.srcAttr.inCommunity),(vid,state)=>{
      !state.inCommunity
    }).edges.mapValues(_=>1)

    //已加入社团的节点以及外连边,提取社团和节点的连边
    val edgeRDD1 = graph.subgraph(et=>et.dstAttr.inCommunity||et.srcAttr.inCommunity).triplets
      .filter(tri=>tri.srcAttr.communityId!=tri.dstAttr.communityId)
      .map(tri=>{
      if(tri.srcAttr.communityId<tri.dstAttr.communityId)
        (tri.srcAttr.communityId+"-"+tri.dstAttr.communityId,tri.attr)
      else
        (tri.dstAttr.communityId+"-"+tri.srcAttr.communityId,tri.attr)
    }).reduceByKey(_+_).map(m=>{
      val nodes: Array[String] = m._1.split("-")
      Edge(nodes(0).toLong,nodes(1).toLong,m._2)
    })

    //提取社团的内连边(不包括社团之间的连边)
    val CV: RDD[(VertexId, Int)] = graph.vertices.map(v=>(v._2.communityId,v._2.insideEdges))
    val CE = graph.triplets.filter(e=>e.srcAttr.communityId==e.dstAttr.communityId&&
    e.srcAttr.inCommunity&&e.dstAttr.inCommunity).map(e=>(e.dstAttr.communityId,e.attr))

    val vertices = CV.union(CE).reduceByKey(_+_,24).map(v=>(v._1,CommunityState(if(v._2>0)true else false,v._1,v._2)))

    //vertices.collect().foreach(println)

    val g = Graph(vertices.repartition(10),edgeRDD1.union(edgeRDD2).repartition(10))
//    println("compress:")
//    g.triplets.collect().foreach(println)
    g
  }

  def insideEdges(graph:Graph[CommunityState,Int]): Int ={
    graph.vertices.map(v=>v._2.insideEdges).reduce(_+_)
  }

  def outsideEdges(graph:Graph[CommunityState,Int]):Int={
    val num = graph.triplets.filter(tri=>(!tri.srcAttr.inCommunity&&tri.dstAttr.inCommunity)||
      (tri.srcAttr.inCommunity&& !tri.dstAttr.inCommunity))
      .map(_.attr)
    if(num.count()==0) 0 else num.reduce(_+_)
  }


  /**
    * 执行起点
    * @param graph 原图
    * @param rdd 社团中心节点
    * @param threshold 阈值
    */
  def run(graph: Graph[Int,Int],rdd:RDD[VertexId],threshold:Double): RDD[VertexId] ={
    val vertxRDD = rdd.map(id=>(id,id))
    //初始化社团状态
    val graph1 = graph.outerJoinVertices[VertexId, (Boolean,Long)](vertxRDD)((vid,data,u)=>{
      u match{
        case Some(t)=>(true, t)
        case None=>(false,vid)
      }
    })

    //计算初始小社团,将每个初始社团中心的同一三角回路的邻居纳入进来。
    val neighbors: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
    val graph2 = graph1.outerJoinVertices(neighbors)((vid, data: (Boolean, VertexId), u)=>{
      u match{
        case Some(u)=>DiffusionState(data._1,data._2,u)
        case None=>DiffusionState(data._1,data._2,Array())
      }
    })

    val vertx_2: VertexRDD[CommunityState] = graph2.aggregateMessages[CommunityState](ctx=>{
      if(ctx.dstAttr.inCommunity&&ctx.srcAttr.inCommunity){ //如果连边两端都在社团中
        if(ctx.srcAttr.neignborIds.intersect(ctx.dstAttr.neignborIds).size>0) { //如果两个点有共同邻居
          ctx.sendToDst(CommunityState(true, Math.min(ctx.dstId, ctx.srcId),0)) //如果两个社团点相邻且有三角关系则合并两个社团并以较小的社团id为id
          ctx.sendToSrc(CommunityState(true, Math.min(ctx.dstId, ctx.srcId),0))
        }
      } else if(ctx.dstAttr.inCommunity){ //如果dst节点在社团中
        //如果两个点有共同邻居
        if(ctx.srcAttr.neignborIds.intersect(ctx.dstAttr.neignborIds).size>0) {
          ctx.sendToSrc(CommunityState(true, ctx.dstId,0))
        }
      }else if(ctx.srcAttr.inCommunity)
      //如果两个点有共同邻居
        if(ctx.srcAttr.neignborIds.intersect(ctx.dstAttr.neignborIds).size>0) {
          ctx.sendToDst(CommunityState(true, ctx.srcId,0))
        }
    },(c1,c2)=>{
      CommunityState(true,Math.min(c1.communityId,c2.communityId),0)
    })

    val graph3 = graph.outerJoinVertices(vertx_2)((vid,data,u)=>{
      u match {
        case Some(u)=>u
        case None=>CommunityState(false,vid,0)
      }
    }).cache().mapEdges(_=>1)  //初始化所有连边的值为1

    //graph3.triplets.collect().foreach(println)
    //压缩
    val graph4 = compress(graph3).cache()

    //note:for test
   // println("第一轮压缩")
    //println(graph4.triplets.map(e=>e.srcId+":"+e.srcAttr.inCommunity+","+e.dstId+","+e.attr).collect().mkString("\n"))

    //记录计算前后的节点数，如果新一轮迭代节点数没有变化就终止
    var preCount = graph3.vertices.count()
    var postCount = graph4.vertices.count()

    //记录计算前后的适应度，如果新一轮迭代适应度值没有增加就终止
    var in = insideEdges(graph4)
    var out = outsideEdges(graph4)

    var postAdapt = in.toDouble/(in+out)
    var preAdapt = 0.0

    var preGraph: Graph[CommunityState, Int] = graph4
    var postGraph:Graph[CommunityState,Int] = null

    var steps = 0
    println("step: "+steps)
    println(postCount+","+postAdapt)

    while(postCount<preCount&&preAdapt<postAdapt){

      preAdapt = postAdapt
      preCount = postCount

      postGraph = compute(preGraph,threshold)
      preGraph = compress(postGraph).cache()

      in = insideEdges(preGraph)
      out = outsideEdges(preGraph)
      postAdapt = in.toDouble/(in+out)
      postCount = preGraph.vertices.count()
      steps+=1

      println("step: "+steps)
      println(postCount+","+postAdapt)
    }
    //最后利用preGraph和初始graph的id号对比得到潜在节点RDD
//    def getMetriccs(preGraph:Graph[CommunityState, Int]) {
//      val originalV = graph.vertices.map(v => v._1).map((_, 1))
//      val finalV = preGraph.vertices.filter(v => !v._2.inCommunity).map(v => (v._1, 1))
//
//      val res = originalV.union(finalV).reduceByKey(_ + _).filter(_._2 == 1).map(_._1)
//
//      println(Mertics.getAll(res,))
//    }

    val originalV = graph.vertices.map(v => v._1).map((_, 1))
    val finalV = preGraph.vertices.filter(v => !v._2.inCommunity).map(v => (v._1, 1))

    originalV.union(finalV).reduceByKey(_ + _).filter(_._2 == 1).map(_._1)
  }
}
