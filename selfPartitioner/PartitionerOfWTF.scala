package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

/**
* When using wholeTextFile, use this partitioner to repartition
*/
class PartitionerOfWTF(numParts : Int) extends Partitioner{
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        var keys : Int = key.asInstanceOf[Int];
        keys
    }
}