package src.main.scala.method.oldVersion

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.math._

import src.main.scala.dataFormat.Record
import src.main.scala.selfPartitioner._

object ReadRDD{
    def readRDDFromFileByPartition(sc : SparkContext, path : String, partitionsNum : Int) : RDD[Record] = {
        var line : RDD[Record] = sc.textFile(path)
                                   .map(l => l.split("\t"))
                                   .filter(l => l.length == 5)
                                   .map(l => new Record(l(0).toInt, l(1), l(2), l(3).toFloat, l(4).toFloat))
                                   .map(l => (l, 1))
                                   .partitionBy(new PartitionerByAnyMinute(partitionsNum))
                                   .map(l => l._1)
        line
    }
}