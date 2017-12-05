package com.hunan.lcd.po

import org.apache.spark.graphx.VertexId

/**
  * Created by hunan on 2017/4/12.
  */
/**
  * 搜索阶段的节点状态
  * @param centrality 节点中心性
  * @param active 节点是否活跃
  * @param nextStep 当前节点的下一步方向
  * @param count 节点计数
  */
case class VertexStates(centrality:Double, active:Boolean,nextStep:VertexId,count:Long)

/**
  * 初始化搜索起始点的节点状态
  * @param centrality 节点中心性
  * @param active 节点是否活跃
  */
case class ActiveStates(centrality:Double, active:Boolean)

/**
  * 扩散寻找潜在节点时的节点状态
  * @param inCommunity 节点是否已经被划分到社团中
  * @param ommunityId 如果被划分到社团则为社团id（即社团最初始的一个节点的id），否则为自己的id
  */
case class DiffusionState(inCommunity:Boolean,ommunityId:Long,neignborIds:Array[Long])

case class CommunityState(inCommunity:Boolean,communityId:Long,insideEdges:Int)

case class ComputeState(inCommunity:Boolean,communityId:Long,neignborIds:Array[Long],degree:Int)
