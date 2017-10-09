package com.scalaT

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoujun on 17-9-24.
  */
class Test {

}
object Test{
  def main(args: Array[String]): Unit = {
    new SparkContext(new SparkConf().setAppName("demo")
            .setMaster("spark://zhoujun:7077")
            .setJars(List("/home/zhoujun/IdeaProjects/java/out/artifacts/spark_jar/spark.jar"))
            .set("spark.executor.memory", "1g")
         )
      .parallelize(List("hello java a world","hello scala a world","hello python"))
      .flatMap(s => s.split(" "))
      .foreach(println)

  }
}
