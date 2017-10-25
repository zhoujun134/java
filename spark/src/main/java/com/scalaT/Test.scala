package com.scalaT

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoujun on 17-9-24.
  */
class Test {

}
object Test{
  def main(args: Array[String]): Unit = {
//    new SparkContext(new SparkConf().setAppName("demo")
//            .setMaster("spark://zhoujun:7077")
//            .setJars(List("/home/zhoujun/IdeaProjects/java/out/artifacts/spark_jar/spark.jar"))
//            .set("spark.executor.memory", "1g")
//         )
//      .parallelize(List("hello java a world","hello scala a world","hello python"))
//      .flatMap(s => s.split(" "))
//      .foreach(println)
    val sc = new SparkContext(new SparkConf().setAppName("demo")
                .setMaster("local[2]"))

    val data = List(
      Tuple2(new Person("asas", 21), 1222),
      Tuple2(new Person("xsssa", 21), 1222),
      Tuple2(new Person("asa", 21), 1222),
      Tuple2(new Person("xs", 31), 1222),
      Tuple2(new Person("dgf", 51), 1222),
      Tuple2(new Person("weew", 22), 1222)
    )

    val rdd = sc.parallelize(data);
    rdd.foreach(println)

  }
}

class Person(name: String, age: Int) extends Serializable{
  override def toString = s"{name = " + name +  ",  age = "+ age +"  }"
}
