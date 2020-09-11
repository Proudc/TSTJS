package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast

import scala.math._
import scala.collection.mutable.ArrayBuffer

import src.main.scala.dataFormat.BaseSetting
import src.main.scala.dataFormat.RecordWithSnap
import src.main.scala.selfPartitioner.PartitionByTime



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

        val myBaseSettings : BaseSettings = new BaseSettings
        myBaseSettings.setDelta(500)
        myBaseSettings.setContiSnap(360)
        myBaseSettings.setTotalSnap(17280)
        myBaseSettings.setBeginSecond(0)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setRootPath("file:///mnt/disk_data_hdd/changzhihao/random_traj_data/101Day0/")
        myBaseSettings.setTimePartitionsNum(48)
        myBaseSettings.setSpacePartitionsNum(2)
        myBaseSettings.setTotalTrajNums(100)
        myBaseSettings.setPatIDList(21)
        
        val inputFilePath : String = myBaseSettings.rootPath.concat("trajectory*.txt")
        
        val recordRDD : RDD[RecordWithSnap] = readRDDAndMapToRecord(sc, inputFilePath, myBaseSettings)

        val indexRDD  : RDD[ThreeDimRTree] = setIndexOnPartition(recordRDD, totalSnap, myBaseSettings)

        doSearchEntry(sc, myBaseSettings, indexRDD)

        sc.stop()
    }


    /**
    * Read the file to create an RDD and map the format to Record
    * @param sc a sparkcontext object
    * @param inputFilePath input file path
    * @param partitionsNum number of partitions when returning RDD
    * @return return an RDD of type Record
    */
    def readRDDAndMapToRecord(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSettings) : RDD[RecordWithSnap] = {
        val inputRDD  : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, 24)
        val recordRDD : RDD[RecordWithSnap] = inputRDD.map(l => new RecordWithSnap(ByteBuffer.wrap(l.slice(0, 4)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(4, 8)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(8, 16)).getDouble, 
                                                                                    ByteBuffer.wrap(l.slice(16, 24)).getDouble))
                                                      .map(l => (l, 1))
        recordRDD
    }

    /**
    * Create a three-dimensional rtree index inside each partition
    * @param inputRDD RDD that needs to be indexed
    * @param totalSnap the number of all records in the RDD
    * @param partitionsNum The number of partitions of the RDD
    * @return return an RDD of type ThreeDimRTree
    */
    def setIndexOnPartition(inputRDD : RDD[RecordWithSnap], myBaseSettings : BaseSettings) : RDD[ThreeDimRTree] = {
        val indexRDD : RDD[ThreeDimRTree] = inputRDD.mapPartitions(l => mapToThreeDimRTree(l, myBaseSettings)).persist(StorageLevel.MEMORY_AND_DISK)
        indexRDD
    }

    /**
    * The mapPartitions() function used in the setIndexOnPartition() function
    * Is the specific process of indexing each partition
    */
    private def mapToThreeDimRTree(iterator : Iterator[RecordWithSnap], myBaseSettings : BaseSettings) : Iterator[ThreeDimRTree] = {
        val minCap : Int = 10
        val maxCap : Int = 30
        var root   : ThreeDimRTree = new ThreeDimRTree(null, 1, minCap, maxCap, null, 1, -1)
        for (record <- iterator){
            val myMBB : MBB = new MBB(record.currSec, record.currSec, record.lon, record.lon, record.lat, record.lat)
            var node  : ThreeDimRTree = new ThreeDimRTree(myMBB, 0, minCap, maxCap, null, 0, record.id)
            root = ThreeDimRTree.insert(root, node)
        }
        Array(root, myMBR).iterator
    }

    /**
    * The main entry to the query
    */
    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSettings, indexRDD : indexRDD : RDD[ThreeDimRTree]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach(patID => {
                // TODO
                val patPath : String = myBaseSettings.rootPath + patID.toString + "..."
                val patCoorList : Array[(Int, Double, Double)] = sc.binaryRecords(patPath)
                                                               .map(l => (ByteBuffer.wrap(l.slice(4, 8)).getInt, 
                                                                            ByteBuffer.wrap(l.slice(8, 16)).getDouble, 
                                                                                ByteBuffer.wrap(l.slice(16, 24)).getDouble))
                                                               .collect()
                val bcPatCoor : Broadcast[Array[(Int, Double, Double)]] = sc.broadcast(patCoorList)
                val candiList : Array[Array[Int]] = indexRDD.mapPartitions(l => mapSearchWithIndex(l, myBaseSettings, bcPatCoor))
                                                            .glom()
                                                            .collect()

                val patCandiList : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                for (i <- 0 to (candiList.length - 1)){
                    for (j <- 0 to (candiList(i).length - 1)){
                        patCandiList += (myBaseSettings.totalTrajNums * i + candiList(i)(j))
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

    private def mapSearchWithIndex(iterator : Iterator[(ThreeDimRTree, MBR)], myBaseSettings : BaseSettings, 
                            bcPatCoor : Broadcast[Array[(Int, Double, Double)]]) : Iterator[Int] = {
        
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        
        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / partitionsNum * partitionID + myBaseSettings.beginSecond
        val stopSnap    : Int = totalSnap / partitionsNum * (partitionID + 1) + myBaseSettings.beginSecond
        val patCoorList : Array[(Int, Double, Double)] = bcPatCoor.value
        val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(startSnap)._2))
        val latDiff     : Double = delta / 111111
        var temMap      : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        while(iterator.hasNext){
            val root     : ThreeDimRTree = iterator.next()
            var currSnap : Int = startSnap
            while(currSnap < stopSnap){
                val minOffset : Int    = patCoorList(currSnap)._1
                val maxOffset : Int    = patCoorList(currSnap)._1
                val minLon    : Double = patCoorList(currSnap)._2 - lonDiff
                val maxLon    : Double = patCoorList(currSnap)._2 + lonDiff
                val minLat    : Double = patCoorList(currSnap)._3 - latDiff
                val maxLat    : Double = patCoorList(currSnap)._3 + latDiff
                val myMBB     : MBB = new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
                val temList   : ArrayBuffer[Int] = root.search(myMBB)
                temMap += currSnap -> temList
                currSnap += contiSnap
            }
        }
        val currSnap : Int = startSnap
        while((currSnap + contiSnap) < stopSnap){
            val firList : ArrayBuffer[Int] = temMap.get(currSnap).get
            val secList : ArrayBuffer[Int] = temMap.get(currSnap + contiSnap).get
            returnList ++= firList intersect secList
            currSnap += contiSnap
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