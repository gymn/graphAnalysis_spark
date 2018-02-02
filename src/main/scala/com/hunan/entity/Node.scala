package com.hunan.entity

case class Node(id:Long, name:String, value:Long, category: Int, symbolSize:Double, `type`:Int)
case class TEdge(source:Long, target:Long, timestamp:Long)
case class LinkScore(source:String, target:String,weight:Double)