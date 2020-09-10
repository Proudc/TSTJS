package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

import scala.math.ceil

import src.main.scala.dataFormat.Record

/**
* Input the number of partitions and divide the total time according to the number of partitions
*/
class PartitionerByAnyMinute(numParts : Int) extends Partitioner{
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        val keys        : Record = key.asInstanceOf[Record]
        val time        : String = keys.time
        val hour        : Int    = time.substring(0, 2).toInt
        val minute      : Int    = time.substring(3, 5).toInt
        val second      : Int    = time.substring(6, 8).toInt
        val totalMinute : Int    = hour * 60 + minute
        val paredTime   : Double = ceil((23 * 60 + 59).toDouble / numParts.toDouble).toDouble
        (totalMinute / paredTime).toInt
    }
}