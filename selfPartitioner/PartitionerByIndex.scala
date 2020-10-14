package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

import scala.math.ceil

import src.main.scala.dataFormat.Record
import src.main.scala.dataFormat.BaseSetting

/**
* Input the number of partitions and divide the total time according to the number of partitions
*/
class PartitionerByIndex(numParts : Int, myBaseSettings : BaseSetting) extends Partitioner{
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        val keys : Int = key.asInstanceOf[Int]
        val baseNum : Int = myBaseSettings.spacePartitionsNum * (myBaseSettings.oneParSnapNum / myBaseSettings.indexSnapInterval + 1)
        keys / baseNum
    }
}