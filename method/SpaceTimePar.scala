package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel



import scala.math._
import scala.collection.mutable.ArrayBuffer

import java.nio.ByteBuffer

import src.main.scala.dataFormat.RecordWithSnap
import src.main.scala.dataFormat.MBB
import src.main.scala.dataFormat.MBR
import src.main.scala.dataFormat.BaseSetting
import src.main.scala.index.ThreeDimRTree
import src.main.scala.selfPartitioner.PartitionByTime
import src.main.scala.selfPartitioner.PartitionerBySpecifyID
import src.main.scala.util.PublicFunc



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
        myBaseSettings.setBeginSecond(0)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setRootPath("file:///mnt/disk_data_hdd/changzhihao/random_traj_data/101Day0/")
        myBaseSettings.setTimePartitionsNum(48)
        myBaseSettings.setSpacePartitionsNum(2)
        myBaseSettings.setTotalTrajNums(100)
        myBaseSettings.setPatIDList(21)

        val inputFilePath : String = myBaseSettings.rootPath.concat("trajectory*.txt")
        val mapFilePath   : String = myBaseSettings.rootPath.concat("map*.txt")
        
        
        val mapOfIDToZvalue : Map[Int, Array[Array[Int]]] = getMapValue(sc, mapFilePath)

        val recordRDD : RDD[RecordWithSnap] = readRDDAndMapToRecord(sc, inputFilePath, myBaseSettings, mapOfIDToZvalue)
        
        val indexRDD  : RDD[(ThreeDimRTree, MBR)] = setIndexOnPartition(recordRDD, myBaseSettings)
        
        doSearchEntry(sc, myBaseSettings, indexRDD)
        
        sc.stop()
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
                                mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : RDD[RecordWithSnap] = {
        val totalPartitionsNum : Int = myBaseSettings.timePartitionsNum * myBaseSettings.spacePartitionsNum
        val inputRDD  : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, 24)
        val recordRDD : RDD[RecordWithSnap] = inputRDD.map(l => new RecordWithSnap(ByteBuffer.wrap(l.slice(0, 4)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(4, 8)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(8, 16)).getDouble, 
                                                                                    ByteBuffer.wrap(l.slice(16, 24)).getDouble))
                                                      .map(l => (l, 1))
                                                      .partitionBy(new PartitionByTime(myBaseSettings.timePartitionsNum, myBaseSettings))
                                                      .map(l => l._1)
                                                      .mapPartitions(l => getPartitionID(l, myBaseSettings.spacePartitionsNum, mapOfIDToZvalue))
                                                      .partitionBy(new PartitionerBySpecifyID(totalPartitionsNum))
                                                      .map(l => l._2)
        recordRDD
    }

    def getPartitionID(iterator : Iterator[RecordWithSnap], spacePartitionsNum : Int, 
                        mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : Iterator[(Int, RecordWithSnap)] = {
        val thisPartitionID : Int = TaskContext.get.partitionId
        val zMapValue   : Array[Array[Int]] = mapOfIDToZvalue.get(thisPartitionID)
        var result      : ArrayBuffer[(Int, RecordWithSnap)] = new ArrayBuffer[(Int, RecordWithSnap)]()
        while(iterator.hasNext){
            val record   : RecordWithSnap = iterator.next()
            val id       : Int = record.id
            val returnID : Int = (thisPartitionID * spacePartitionsNum) + zMapValue(id)(1)
            result += ((returnID, record))
        }
        result.iterator
    }

    def setIndexOnPartition(inputRDD : RDD[RecordWithSnap], myBaseSettings : BaseSettings) : RDD[(ThreeDimRTree, MBR)] = {
        val indexRDD : RDD[(ThreeDimRTree, MBR)] = inputRDD.mapPartitions(l => mapToThreeDimRTree(l, myBaseSettings)).persist(StorageLevel.MEMORY_AND_DISK)
        indexRDD
    }

    /**
    * 在每个分区内部建立R-Tree
    * 处理时间维度时
    * 1、首先将具体的时间转换为snapshot
    * 2、之后将具体的snapshot与本分区开始的snapshot之间的offset作为时间维度插入R-Tree
    */
    def mapToThreeDimRTree(iterator : Iterator[RecordWithSnap], myBaseSettings : BaseSettings) : Iterator[(ThreeDimRTree, MBR)] = {
        val minCap : Int = 10
        val maxCap : Int = 30
        var root   : ThreeDimRTree = new ThreeDimRTree(null, 1, minCap, maxCap, null, 1, -1)
        var minLon : Double = Double.MaxValue
        var maxLon : Double = Double.MinValue
        var minLat : Double = Double.MaxValue
        var maxLat : Double = Double.MinValue
        for (record <- iterator){
            val myMBB : MBB = new MBB(record.currSec, record.currSec, record.lon, record.lon, record.lat, record.lat)
            var node  : ThreeDimRTree = new ThreeDimRTree(myMBB, 0, minCap, maxCap, null, 0, record.id)
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
            var partitionIDList : ArrayBuffer[Int] = new ArrayBuffer[Int]()
            var partitionIDMap  : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            for (i <- 0 to candiList.length - 1){
                if (candiList(i).length != 1){
                    partitionIDList += candiList(i)(0)
                    var temBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    for (j <- 1 to candiList(i).length - 1){
                        temBuffer += candiList(i)(j)
                    }
                    partitionIDMap += candiList(i)(0) -> temBuffer
                }
            }
            val inputFilePath : String = getInputFilePath(myBaseSettings, partitionIDList)
            val inputRDD : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, 24)
            // TODO
        }
        }
    }

    def mapSearchWithIndex(iterator : Iterator[(ThreeDimRTree, MBR)], myBaseSettings : BaseSettings, 
                            bcPatCoor : Broadcast[Array[(Int, Double, Double)]]) : Iterator[Int] = {
        
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta

        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / timePartitionsNum * (partitionID / spacePartitionsNum) + myBaseSettings.beginSecond
        val stopSnap    : Int = totalSnap / timePartitionsNum * (partitionID / spacePartitionsNum + 1) + myBaseSettings.beginSecond
        val patCoorList : Array[(Int, Double, Double)] = bcPatCoor.value
        val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(startSnap)._2))
        val latDiff     : Double = delta / 111111
        var temMap      : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        resultList += partitionID
        
        while(iterator.hasNext){
            val line  : Tuple2[ThreeDimRTree, MBR] = iterator.next()
            val root  : ThreeDimRTree = line._1
            val myMBR : MBR = line._2
            if (ifIntersect(myMBR, startSnap, stopSnap, patCoorList, delta)){
                val currSnap : Int = startSnap
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

    def ifIntersect(myMBR : MBR, startSnap : Int, stopSnap : Int, patCoorList : Array[(Double, Double)], delta : Double) : Boolean = {
        var minLon   : Double = Double.MaxValue
        var maxLon   : Double = Double.MinValue
        var minLat   : Double = Double.MaxValue
        var maxLat   : Double = Double.MinValue
        val lonDiff  : Double = abs(delta / 111111 / cos(patCoorList(startSnap)._2))
        val latDiff  : Double = delta / 111111
        var currSnap : Int = startSnap
        while(currSnap < stopSnap){
            minLon = min(minLon, patCoorList(currSnap)._1)
            maxLon = max(maxLon, patCoorList(currSnap)._1)
            minLat = min(minLat, patCoorList(currSnap)._2)
            maxLat = max(maxLat, patCoorList(currSnap)._2)
            currSnap += 1
        }
        minLon = minLon - lonDiff
        maxLon = maxLon + lonDiff
        minLat = minLat - latDiff
        maxLat = maxLat + latDiff
        val widthLon1  : Double = myMBR.maxLon - myMBR.minLon
        val heightLat1 : Double = myMBR.maxLat - myMBR.minLat
        val cenLon1    : Double = (myMBR.maxLon + myMBR.minLon) / 2
        val cenLat1    : Double = (myMBR.maxLat + myMBR.minLat) / 2
        val widthLon2  : Double = maxLon - minLon
        val heightLat2 : Double = maxLat - minLat
        val cenLon2    : Double = (maxLon + minLon) / 2
        val cenLat2    : Double = (maxLat + minLat) / 2
        val flag1 : Boolean = (widthLon1 / 2 + widthLon2 / 2) >= abs(cenLon1 - cenLon2)
        val flag2 : Boolean = (heightLat1 / 2 + heightLat2 / 2) >= abs(cenLat1 - cenLat2)
        if (flag1 && flag2){
            true
        }else{
            false
        }
    }

    def getInputFilePath(myBaseSettings : BaseSettings, idList : ArrayBuffer[Int]) : String = {
        val pathPrefix         : String = myBaseSettings.rootPath
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        var path : String = pathPrefix + "{"
        for (i <- 0 to idList.length - 1){
            val id      : Int = idList(i)
            val timeID  : Int = id / spacePartitionsNum
            val spaceID : Int = id % spacePartitionsNum
            val name    : String = "partition" + timeID.toString + "zorder" + spaceID.toString
            path = path + name
            if (i != idList.length - 1){
                path = path + ","
            }
        }
        path = path + "}.txt"
        path
    }

    def mapSearchWithRefine(iterator : Iterator[Array[Byte]], myBaseSettings : BaseSettings, partitionIDList : ArrayBuffer[Int], 
                            partitionIDMap : Map[Int, ArrayBuffer[Int]], bcPatCoor : Broadcast[Array[(Int, Double, Double)]]) : Iterator[Array[Array[Int]]] = {
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int = myBaseSettings.contiSnap
        val totalSnap          : Int = myBaseSettings.totalSnap
        val timeInterval       : Int = myBaseSettings.timeInterval
        val delta              : Double = myBaseSettings.delta
        

        val partitionID  : Int = TaskContext.get.partitionId
        val mapKey       : Int = partitionIDList(partitionID)
        val candiList    : ArrayBuffer[Int] = partitionIDMap.get(mapKey)
        val patCoorList  : Array[(Int, Double, Double)] = bcPatCoor.value
        val startSnap    : Int = totalSnap / timePartitionsNum * (mapKey / spacePartitionsNum) + myBaseSettings.beginSecond
        var result       : Array[Array[Int]] = Array.ofDim(candiList.length, totalSnap / timePartitionsNum)
        val lonDiff      : Double = abs(delta / 111111 / cos(patCoorList(0)._2))
        val latDiff      : Double = delta / 111111
        var candiListMap : Map[Integer, Integer] = Map[Integer, Integer]()
        
        for (i <- 0 to candiList.length - 1){
            candiListMap ++= candiList(i) -> i
        }
        while(iterator.hasNext){
            val line : Array[Byte] = iterator.next()
            val id   : Int = ByteBuffer.wrap(line.slice(0, 4)).getInt
            if (!candiList.constains(id)){
                continue
            }
            val currSnap : Int = ByteBuffer.wrap(line.slice(4, 8)).getInt
            val snapShot : Int = (currSnap - myBaseSettings.beginSecond) / timeInterval
            val lon      : Double = ByteBuffer.wrap(l.slice(8, 16)).getDouble
            val lat      : Double = ByteBuffer.wrap(l.slice(16, 24)).getDouble
            val minLon   : Double = patCoorList(snapshot)._2 - lonDiff
            val maxLon   : Double = patCoorList(snapshot)._2 + lonDiff
            val minLat   : Double = patCoorList(snapshot)._3 - latDiff
            val maxLat   : Double = patCoorList(snapshot)._3 + latDiff
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                result(candiListMap.get(id))(snapshot - startSnap) = 1
            }else{
                result(candiListMap.get(id))(snapshot - startSnap) = 0
            }
        }
        result.iterator
    }

}