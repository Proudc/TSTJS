package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

import scala.math.ceil

import src.main.scala.dataFormat.RecordWithSnap
import src.main.scala.dataFormat.BaseSetting


class PartitionerByTime(numParts : Int, myBaseSettings : BaseSetting) extends Partitioner{
    val partitionLength : Int = myBaseSettings.totalSnap / numParts
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        val keys : RecordWithSnap = key.asInstanceOf[RecordWithSnap]
        (keys.currSec - myBaseSettings.beginSecond) / partitionLength
    }
}
