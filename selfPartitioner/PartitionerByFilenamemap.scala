package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

import scala.math.ceil
import scala.util.control.Breaks

import src.main.scala.dataFormat.Record

/**
* Input the number of partitions and divide the total time according to the number of partitions
*/
class PartitionerByFilenamemap(numParts : Int, mapID : Array[String]) extends Partitioner{
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        val keys        : String = key.asInstanceOf[String]
        var id : Int = 0
        var loop = new Breaks
        loop.breakable{
            for (i <- 0 until mapID.length){
            if (mapID(i).equals(keys)){
                id = i
                loop.break()
            }
        }
        }
        id
    }
}