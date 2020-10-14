package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

import scala.math.ceil

import src.main.scala.dataFormat.Record

/**
* Input the number of partitions and divide the total time according to the number of partitions
*/
class PartitionerByFilename(numParts : Int) extends Partitioner{
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        val keys        : String = key.asInstanceOf[String]
        val lines1 : Array[String] = keys.split("/")
        val lines2 : Array[String] = lines1(lines1.length - 1).split("\\.")
        val line : String = lines2(0);
        var firNum : Int = 0
        var secNum : Int = 0
        if (line.charAt(4) == 'z'){
            var firStr = line.slice(3, 4)
            var secStr = line.slice(10, line.length)
            firNum = firStr.toInt
            secNum = secStr.toInt
        }else{
            var firStr = line.slice(3, 5)
            var secStr = line.slice(11, line.length)
            firNum = firStr.toInt
            secNum = secStr.toInt
        }
        firNum * 100 + secNum
    }
}