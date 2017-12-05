name := "graphanalysis_spark"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "aliyun Maven Repository" at "http://maven.aliyun.com/nexus/content/groups/public"
externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.44"