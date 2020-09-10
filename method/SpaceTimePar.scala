package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel



import scala.math._
import scala.collection.mutable.ArrayBuffer

import src.main.scala.dataFormat._
import src.main.scala.selfPartitioner._
import src.main.scala.index.ThreeDimRTree


object SpaceTimePar{


    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .appName("SpaceTimePar")
                    .getOrCreate()
        val sc = spark.sparkContext

        val myBaseSettings : BaseSettings = new BaseSettings
        myBaseSettings.setDelta(500)
        myBaseSettings.setContiSnap(360)
        myBaseSettings.setTotalSnap(17280)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setRootPath("file:///mnt/disk_data_hdd/changzhihao/random_traj_data/101Day0/")
        myBaseSettings.setTimePartitionsNum(48)
        myBaseSettings.setSpacePartitionsNum(2)
        myBaseSettings.setTotalTrajNums(100)
        myBaseSettings.setPatIDList(21)

        val inputFilePath : String = myBaseSettings.rootPath.concat("trajectory*.txt")
        val mapFilePath   : String = myBaseSettings.rootPath.concat("map*.txt")
        
        
        val mapOfIDToZvalue : Map[Int, Array[Array[Int]]] = getMapValue(sc, mapFilePath)

        val recordRDD : RDD[Record] = readRDDAndMapToRecord(sc, inputFilePath, myBaseSettings, mapOfIDToZvalue)
        val indexRDD  : RDD[(ThreeDimRTree, MBR)] = setIndexOnPartition(recordRDD, myBaseSettings)
        doSearchEntry(sc, myBaseSettings, indexRDD)
    }

    /**
    * 目的是得到每个时间分区中的空间分区状况
    * 比如说一个时间分区中有100条轨迹
    * 这100条轨迹对应的z-value分别是50， 43， 49， 79， ...， 60
    * 则该时间分区对应的map文件为：轨迹id， 该轨迹被分配的空间分区id
    * 即: 20， 0
    *     29， 0
    *     10， 1
    *     ...
    */
    def getMapValue(sc : SparkContext, mapFilePath : String) : Map[Int, Array[Array[Int]]] = {
        val inputRDD   : RDD[String] = sc.textFile(mapFilePath)
        val arrayValue : Array[Array[String]] = inputRDD.glom()
                                                        .collect()
        var mapOfIDToZvalue : Map[Int, Array[Array[Int]]] = Map[Int, Array[Array[Int]]]()
        for (i <- 0 to arrayValue.length - 1){
            val eachValue : Array[Array[Int]] = Array.ofDim(arrayValue(i).length, 2)
            for (j <- 0 to arrayValue(i).length - 1){
                val line  : String = arrayValue(i)(j)
                val words : Array[String] = line.split("\t")
                eachValue(j)(0) = words(0).toInt
                eachValue(j)(1) = words(1).toInt
            }
            mapOfIDToZvalue += i -> eachValue
        }
        mapOfIDToZvalue
    }

    def readRDDAndMapToRecord(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSettings, 
                                mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : RDD[Record] = {
        val totalPartitionsNum : Int = myBaseSettings.timePartitionsNum * myBaseSettings.spacePartitionsNum
        val inputRDD  : RDD[String] = sc.textFile(inputFilePath)
        val recordRDD : RDD[Record] = inputRDD.map(l => l.split("\t"))
                                              .filter(l => l.length == 5)
                                              .map(l => new Record(l(0).toInt, l(1), l(2), l(3).toDouble, l(4).toDouble))
                                              .map(l => (l, 1))
                                              .partitionBy(new PartitionerByAnyMinute(myBaseSettings.timePartitionsNum))
                                              .map(l => l._1)
                                              .mapPartitions(l => getPartitionID(l, myBaseSettings.spacePartitionsNum, mapOfIDToZvalue))
                                              .partitionBy(new PartitionerBySpecifyID(totalPartitionsNum))
                                              .map(l => l._2)
        recordRDD
    }

    def getPartitionID(iterator : Iterator[Record], spacePartitionsNum : Int, mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : Iterator[(Int, Record)] = {
        val thisPartitionID : Int = TaskContext.get.partitionId
        val zMapValue   : Array[Array[Int]] = mapOfIDToZvalue.get(thisPartitionID)
        var result      : ArrayBuffer[(Int, Record)] = new ArrayBuffer[(Int, Record)]()
        while(iterator.hasNext){
            val record   : Record = iterator.next()
            val id       : Int = record.id
            val returnID : Int = (thisPartitionID * spacePartitionsNum) + zMapValue(id)(1)
            result += ((returnID, record))
        }
        result.iterator
    }

    def setIndexOnPartition(inputRDD : RDD[Record], myBaseSettings : BaseSettings) : RDD[(ThreeDimRTree, MBR)] = {
        val indexRDD : RDD[(ThreeDimRTree, MBR)] = inputRDD.mapPartitions(l => mapToThreeDimRTree(l, myBaseSettings))..persist(StorageLevel.MEMORY_AND_DISK)
        indexRDD
    }

    /**
    * 在每个分区内部建立R-Tree
    * 处理时间维度时
    * 1、首先将具体的时间转换为snapshot
    * 2、之后将具体的snapshot与本分区开始的snapshot之间的offset作为时间维度插入R-Tree
    */
    def mapToThreeDimRTree(iterator : Iterator[Record], myBaseSettings : BaseSettings) : Iterator[(ThreeDimRTree, MBR)] = {
        val totalSnap          : Int = myBaseSettings.totalSnap
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val timeInterval       : Int = myBaseSettings.timeInterval
        
        val minCap      : Int = 10
        val maxCap      : Int = 30
        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / timePartitionsNum * (partitionID / spacePartitionsNum)
        var root        : ThreeDimRTree = new ThreeDimRTree(null, 1, minCap, maxCap, null, 1, -1)
        var minLon : Double = Double.MaxValue
        var maxLon : Double = Double.MinValue
        var minLat : Double = Double.MaxValue
        var maxLat : Double = Double.MinValue
        while(iterator.hasNext){
            val record : Record = iterator.next()
            val offset : Int = PublicFunc.changeTimeToSnap(record.time, timeInterval) - startSnap
            val myMBB : MBB = new MBB(offset, offset, record.lon, record.lon, record.lat, record.lat)
            var node : ThreeDimRTree = new ThreeDimRTree(myMBB, 0, minCap, maxCap, null, 0, record.id)
            root = ThreeDimRTree.insert(root, node)
            minLon = min(minLon, record.lon)
            maxLon = max(maxLon, record.lon)
            minLat = min(minLat, record.lat)
            maxLat = max(maxLat, record.lat)
        }
        val myMBR : MBR = new MBR(minLon, maxLon, minLat, maxLat)
        Array((root, myMBR)).iterator
    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSettings, indexRDD : indexRDD : RDD[(ThreeDimRTree, MBR)]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach{patID => {
            val patPath : String = myBaseSettings.rootPath + patID.toString + "..."
            val patCoorList : Array[(Double, Double)] = sc.textFile(patPath)
                                                              .map(l => l.split("\t"))
                                                              .map(l => (l(3).toDouble, l(4).toDouble))
                                                              .collect()
            val bcPatCoor : Broadcast[Array[(Double, Double)]] = sc.broadcast(patCoorList)
            val candiList : Array[Array[Int]] = indexRDD.mapPartitions(l => mapSearchWithIndex(l, myBaseSettings, bcPatCoor))
                                                            .glom()
                                                            .collect()
            var partitionIDList : ArrayBuffer[Int] = new ArrayBuffer[Int]()
            var partitionIDMap  : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            for (i <- 0 to candiList.length - 1){
                if (candiList(i).length != 1){
                    var temBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    for (j <- 1 to candiList(i).length - 1){
                        temBuffer += candiList(i)(j)
                    }
                    partitionIDMap += candiList(i)(0) -> temBuffer
                    partitionIDList += candiList(i)(0)
                }
            }
            val inputFilePath : String = getInputFilePath(myBaseSettings, partitionIDList)
            val inputRDD : RDD[String] = sc.textFile(inputFilePath, partitionIDList.length)
        }
        }
    }

    def mapSearchWithIndex(iterator : Iterator[(ThreeDimRTree, MBR)], myBaseSettings : BaseSettings, bcPatCoor : Broadcast[Array[(Double, Double)]]) : Iterator[Int] = {
        val timePartitionsNum : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap : Int = myBaseSettings.contiSnap
        val totalSnap : Int = myBaseSettings.totalSnap
        val delta : Double = myBaseSettings.delta

        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / timePartitionsNum * (partitionID / spacePartitionsNum)
        val stopSnap    : Int = totalSnap / timePartitionsNum * (partitionID / spacePartitionsNum + 1)
        val patCoorList : Array[(Double, Double)] = bcPatCoor.value
        val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(0)._2))
        val latDiff     : Double = delta / 111111
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        resultList += partitionID
        while(iterator.hasNext){
            val line : Tuple2[ThreeDimRTree, MBR] = iterator.next()
            val root : ThreeDimRTree = line._1
            val myMBR : MBR = line._2
            if ((myMBR, startSnap, stopSnap, patCoorList, delta)){
                val currSnap : Int = startSnap
                while(currSnap < stopSnap){
                    val minOffset : Int = currSnap - startSnap
                    val maxOffset : Int = currSnap - startSnap
                    val minLon : Double = patCoorList(currSnap)._1 - lonDiff
                    val maxLon : Double = patCoorList(currSnap)._1 + lonDiff
                    val minLat : Double = patCoorList(currSnap)._2 - latDiff
                    val maxLat : Double = patCoorList(currSnap)._2 + latDiff
                    val myMBB : MBB = new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
                    resultList ++= root.search(myMBB)
                    currSnap += contiSnap
                }
            }
        }
        resultList.iterator
    }

    def ifIntersect(myMBR : MBR, startSnap : Int, stopSnap : Int, patCoorList : Array[(Double, Double)], delta : Double) : Boolean = {
        var minLon : Double = Double.MaxValue
        var maxLon : Double = Double.MinValue
        var minLat : Double = Double.MaxValue
        var maxLat : Double = Double.MinValue
        val lonDiff : Double = abs(delta / 111111 / cos(patCoorList(startSnap)._2))
        val latDiff : Double = delta / 111111
        var currSnap : Int = startSnap
        while(currSnap < stopSnap){
            minLon = min(minLon, patCoorList(currSnap)._1)
            maxLon = max(maxLon, patCoorList(currSnap)._1)
            minLat = min(minLat, patCoorList(currSnap)._2)
            maxLat = max(maxLat, patCoorList(currSnap)._2)
        }
        minLon = minLon - lonDiff
        maxLon = maxLon + lonDiff
        minLat = minLat - latDiff
        maxLat = maxLat + latDiff
        val widthLon1 : Double = myMBR.maxLon - myMBR.minLon
        val heightLat1 : Double = myMBR.maxLat - myMBR.minLat
        val cenLon1 : Double = (myMBR.maxLon + myMBR.minLon) / 2
        val cenLat1 Double = (myMBR.maxLat + myMBR.minLat) / 2
        val widthLon2 : Double = maxLon - minLon
        val heightLat2 : Double = maxLat - minLat
        val cenLon2 : Double = (maxLon + minLon) / 2
        val cenLat2 Double = (maxLat + minLat) / 2
        if ((widthLon1 / 2 + widthLon2 / 2) >= abs(cenLon1 - cenLon2) && (heightLat1 / 2 + heightLat2 / 2) >= abs(cenLat1 - cenLat2)){
            true
        }else{
            false
        }
    }

    def getInputFilePath(myBaseSettings : BaseSettings, idList : ArrayBuffer[Int]) : String = {
        val pathPrefix         : String = myBaseSettings.rootPath
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        var path : String = pathPrefix + "{"
        for (i <- 0 to idList.length - 1){
            val id : Int = idList(i)
            val timeID : Int = id / spacePartitionsNum
            val spaceID : Int = id % spacePartitionsNum
            val name : String = "partition" + timeID.toString + "zorder" + spaceID.toString
            path = path + name
            if (i != idList.length - 1){
                path = path + ","
            }
        }
        path = path + "}.txt"
        path
    }

    def mapSearchWithRefine(iterator : Iterator[String], myBaseSettings : BaseSettings, partitionIDList : ArrayBuffer[Int], partitionIDMap : Map[Int, ArrayBuffer[Int]], bcPatCoor : Broadcast[Array[(Double, Double)]]) : Iterator[Array[Array[Int]]] = {
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int = myBaseSettings.contiSnap
        val totalSnap          : Int = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        val timeInterval       : Int = myBaseSettings.timeInterval

        val partitionID : Int = TaskContext.get.partitionId
        val mapKey : Int = partitionIDList(partitionID)
        val candiList : ArrayBuffer[Int] = partitionIDMap.get(mapKey)
        val patCoorList : Array[(Double, Double)] = bcPatCoor.value
        val startSnap : Int = totalSnap / timePartitionsNum * (mapKey / spacePartitionsNum)
        var result : Array[Array[Int]] = Array.ofDim(candiList.length, totalSnap / timePartitionsNum)
        val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(0)._2))
        val latDiff     : Double = delta / 111111
        var candiListMap : Map[Integer, Integer] = Map[Integer, Integer]()
        for (i <- 0 to candiList.length - 1){
            candiListMap ++= candiList(i) -> i
        }
        while(iterator.hasNext){
            val line : String = iterator.next()
            val words : Array[String] = line.split("\t")
            val id : Int = words(0).toInt
            val time : String = words(2)
            val snapshot : Int = PublicFunc.changeTimeToSnap(time, timeInterval)
            val lon : Double = words(3).toDouble
            val lat : Double = words(4).toDouble
            val minLon : Double = patCoorList(snapshot)._1 - lonDiff
            val maxLon : Double = patCoorList(snapshot)._1 + lonDiff
            val minLat : Double = patCoorList(snapshot)._2 - latDiff
            val maxLat : Double = patCoorList(snapshot)._2 + latDiff
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                result(candiListMap.get(id))(snapshot - startSnap) = 1
            }else{
                result(candiListMap.get(id))(snapshot - startSnap) = 0
            }
        }
        result.iterator
    }

}