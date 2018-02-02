package com.hunan

import java.util.Properties

import com.hunan.lcd.LcdMain
import com.hunan.tlp.TlpMain
import org.apache.spark.sql.SparkSession

/**
  * for test
  */
object Main {
  def computeTL(): Unit ={
    val spark = SparkSession.builder().appName("test_tl").master("local[*]").config("spark.sql.shuffle.partitions",10).getOrCreate()
    val properties = new Properties()
    properties.setProperty("narrowIndex","0.8")
    properties.setProperty("timeFormat","yyyy-MM-dd HH:mm:ss")
    properties.setProperty("numberOfWindows","5")
    properties.setProperty("graphFilePath","E:\\科研工作\\原型系统\\TL_example_data\\enron\\enron_train.csv")
    properties.setProperty("testFilePath","E:\\科研工作\\原型系统\\TL_example_data\\enron\\enron_test.csv")

    TlpMain.compute(spark,properties)
    spark.stop()
  }

  def computeLC(): Unit ={
    val spark = SparkSession.builder().appName("test_lc").master("local[*]").config("spark.sql.shuffle.partitions",10).getOrCreate()
    val properties = new Properties()
    properties.setProperty("attractThreshold","0.8")
    properties.setProperty("maxSteps","1")
    properties.setProperty("seeds","30,17")
    properties.setProperty("graphFilePath","E:\\科研工作\\原型系统\\LC_exapmle_data\\karate\\karate_train.csv")
    properties.setProperty("testFilePath","E:\\科研工作\\原型系统\\LC_exapmle_data\\karate\\karate_test.csv")

    LcdMain.compute(spark,properties)
    spark.stop()
  }
  def main(args: Array[String]): Unit = {
    computeLC()
  }
}
