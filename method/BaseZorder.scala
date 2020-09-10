package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast

import scala.math._
import scala.collection.mutable.ArrayBuffer

import src.main.scala.selfPartitioner.PartitionerByAnyMinute
import src.main.scala.index.ThreeDimRTree
import src.main.scala._
import src.main.scala.util.PublicFunc
import src.main.scala.dataFormat.Record

/**
* Implementation method based on file z-order sorting
*/
object BaseZorder{

    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .appName("BaseZorder")
                    .getOrCreate()
        val sc = spark.sparkContext

        val delta         : Int = 500
        val contiSnap     : Int = 360
        val totalSnap     : Int = 17280 
        val totalTrajNums : Int = 1000
        val partitionsNum : Int = 48
        val patIDListSize : Int = 21
        val rootPath      : String = "file:///mnt/disk_data_hdd/changzhihao/random_traj_data/101Day0/trajectory"
        val pathSuffix    : String = ".txt"
        val inputFilePath : String = rootPath + "*.txt"
        var patIDList     : Array[Int] = new Array[Int](patIDListSize)
        for (i <- 0 to patIDListSize - 1){
            patIDList(i) = i
            println(i)
        }
        val recordRDD : RDD[Record] = readRDDAndMapToRecord(sc, inputFilePath, partitionsNum)
        println("Finish readRDDAndMapToRecord...")
        val indexRDD  : RDD[ThreeDimRTree] = setIndexOnPartition(recordRDD, totalSnap, partitionsNum)
        println("Finish setIndexOnPartition...")
        doSearchEntry(sc, patIDList, rootPath, pathSuffix, inputFilePath, partitionsNum, delta, contiSnap, totalSnap, indexRDD, totalTrajNums)
    }


    /**
    * Read the file to create an RDD and map the format to Record
    * @param sc a sparkcontext object
    * @param inputFilePath input file path
    * @param partitionsNum number of partitions when returning RDD
    * @return return an RDD of type Record
    */
    def readRDDAndMapToRecord(sc : SparkContext, inputFilePath : String, partitionsNum : Int) : RDD[Record] = {
        val inputRDD  : RDD[String] = sc.textFile(inputFilePath)
        println(inputRDD.count())
        val recordRDD : RDD[Record] = inputRDD.map(l => l.split("\t"))
                                              .filter(l => l.length == 5)
                                              .map(l => new Record(l(0).toInt, l(1), l(2), l(3).toDouble, l(4).toDouble))
                                              .map(l => (l, 1))
                                              .partitionBy(new PartitionerByAnyMinute(partitionsNum))
                                              .map(l => l._1)
        println(recordRDD.count())
        recordRDD
    }

    /**
    * Create a three-dimensional rtree index inside each partition
    * @param inputRDD RDD that needs to be indexed
    * @param totalSnap the number of all records in the RDD
    * @param partitionsNum The number of partitions of the RDD
    * @return return an RDD of type ThreeDimRTree
    */
    def setIndexOnPartition(inputRDD : RDD[Record], totalSnap : Int, partitionsNum : Int) : RDD[ThreeDimRTree] = {
        val indexRDD : RDD[ThreeDimRTree] = inputRDD.mapPartitions(l => mapToThreeDimRTree(l, totalSnap, partitionsNum))
        println(indexRDD.count())
        indexRDD
    }

    /**
    * The mapPartitions() function used in the setIndexOnPartition() function
    * Is the specific process of indexing each partition
    */
    private def mapToThreeDimRTree(iterator : Iterator[Record], totalSnap : Int, partitionsNum : Int) : Iterator[ThreeDimRTree] = {
        val minCap      : Int = 10
        val maxCap      : Int = 30
        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / partitionsNum * partitionID
        var root        : ThreeDimRTree = new ThreeDimRTree(null, 1, minCap, maxCap, null, 1, -1)
        while(iterator.hasNext){
            val record : Record = iterator.next()
            val offset : Int = PublicFunc.changeTimeToSnap(record.time) - startSnap
            var MBB    : Map[String, Double] = Map[String, Double]()
            MBB += "minOffset" -> offset.toDouble
            MBB += "maxOffset" -> offset.toDouble
            MBB += "minLon" -> record.lon
            MBB += "maxLon" -> record.lon
            MBB += "minLat" -> record.lat
            MBB += "maxLat" -> record.lat
            var node : ThreeDimRTree = new ThreeDimRTree(MBB, 0, minCap, maxCap, null, 0, record.id)
            root = ThreeDimRTree.insert(root, node)
        }
        val rootList : Array[ThreeDimRTree] = Array(root)
        rootList.iterator
    }

    /**
    * The main entry to the query
    */
    def doSearchEntry(sc : SparkContext, patIDList : Array[Int], pathPrefix : String, pathSuffix :String, inputFilePath : String, 
                        partitionsNum : Int, delta : Int, contiSnap : Int, totalSnap : Int, indexRDD : RDD[ThreeDimRTree], totalTrajNums : Int) : Unit = {
        patIDList.foreach(patID => {
                // The variable of "patPath" need to rewrite according to the specific path
                val patPath     : String = pathPrefix + patID.toString + pathSuffix
                println(patPath)
                val patCoorList : Array[(Double, Double)] = sc.textFile(patPath)
                                                           .map(l => l.split("\t"))
                                                           .map(l => (l(3).toDouble, l(4).toDouble))
                                                           .collect()
                println(patCoorList.length)
                val bcPatCoor : Broadcast[Array[(Double, Double)]] = sc.broadcast(patCoorList)
                val candiList : Array[Array[Int]] = indexRDD.mapPartitions(l => mapSearchWithIndex(l, sc, partitionsNum, delta, contiSnap, totalSnap, bcPatCoor))
                                                            .glom()
                                                            .collect()
                println(patCoorList.length)
                val patCandiList : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                for (i <- 0 to (candiList.length - 1)){
                    for (j <- 0 to (candiList(i).length - 1)){
                        patCandiList += (totalTrajNums * i + candiList(i)(j))
                    }
                }
                val bcPatCandiList : Broadcast[ArrayBuffer[Int]] = sc.broadcast(patCandiList)
                val inputRDD : RDD[String] = sc.textFile(inputFilePath, (partitionsNum * totalTrajNums))
                val finalList = inputRDD.mapPartitions(l => mapSearchWithRefine(l, sc, partitionsNum, delta, contiSnap, totalSnap, totalTrajNums, bcPatCandiList, bcPatCoor))
                                        .collect()
                print(finalList)
            }
        )
    }

    private def mapSearchWithIndex(iterator : Iterator[ThreeDimRTree], sc : SparkContext, 
                                    partitionsNum : Int, delta : Int, contiSnap : Int, totalSnap : Int, bcPatCoor : Broadcast[Array[(Double, Double)]]) : Iterator[Int] = {
        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / partitionsNum * partitionID
        val stopSnap    : Int = totalSnap / partitionsNum * (partitionID + 1)
        val patCoorList : Array[(Double, Double)] = bcPatCoor.value
        val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(0)._2))
        val latDiff     : Double = delta / 111111
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        while(iterator.hasNext){
            val root     : ThreeDimRTree = iterator.next()
            var currSnap : Int = startSnap
            while(currSnap < stopSnap){
                var MBB : Map[String, Double] = Map[String, Double]()
                MBB += "minOffset" -> (currSnap - startSnap).toDouble
                MBB += "maxOffset" -> (currSnap - startSnap).toDouble
                MBB += "minLon" -> (patCoorList(currSnap)._1 - lonDiff)
                MBB += "maxLon" -> (patCoorList(currSnap)._1 + lonDiff)
                MBB += "minLat" -> (patCoorList(currSnap)._2 - latDiff)
                MBB += "maxLat" -> (patCoorList(currSnap)._2 + latDiff)
                resultList ++= root.search(MBB)
                currSnap += contiSnap
            }
        }
        resultList.iterator
    }

    private def mapSearchWithRefine(iterator : Iterator[String], sc : SparkContext, partitionsNum : Int, delta : Int, contiSnap : Int, 
                                    totalSnap : Int, totalTrajNums : Int, bcPatCandiList : Broadcast[ArrayBuffer[Int]], bcPatCoor : Broadcast[Array[(Double, Double)]]) : Iterator[Int] = {
        val partitionID : Int = TaskContext.get.partitionId
        val patCandiList = bcPatCandiList.value
        val result : ArrayBuffer[Int] = new ArrayBuffer[Int]()
        if (!(patCandiList.contains(partitionID))){
            /**
            * This branch means that the partition does not need to be
            * calculated and returns -1
            */
            result += -1
        }else{
            val patCoorList : Array[(Double, Double)] = bcPatCoor.value
            var currSnap    : Int = totalSnap / partitionsNum * (partitionID / totalTrajNums)
            val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(0)._2))
            val latDiff     : Double = delta / 111111
            result += partitionID
            while(iterator.hasNext){
                val line   : String = iterator.next()
                val words  : Array[String] = line.split("\t")
                val lon    : Double = words(3).toDouble
                val lat    : Double = words(4).toDouble
                val minLon : Double = patCoorList(currSnap)._1 - lonDiff
                val maxLon : Double = patCoorList(currSnap)._1 + lonDiff
                val minLat : Double = patCoorList(currSnap)._2 - latDiff
                val maxLat : Double = patCoorList(currSnap)._2 + latDiff
                if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                    result += currSnap
                }
                currSnap += 1
            }
        }
        result.iterator
    }
}