package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

import scala.math.ceil

import src.main.scala.dataFormat.RecordWithSnap

class PartitionerByTime(numParts : Int, myBaseSettings : BaseSettings) extends Partitioner{
    val partitionLength : Int = myBaseSettings.totalSnap / numParts
    override def numPartitions : Int = numParts
    override def getPartitions(key : Any) : Int = {
        val keys : RecordWithSnap = key.asInstanceOf[RecordWithSnap]
        (keys.currSec - myBaseSettings.beginSecond) / partitionLength
    }
}
