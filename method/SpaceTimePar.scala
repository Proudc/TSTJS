package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

import java.nio.ByteBuffer

import src.main.scala.dataFormat.RecordWithSnap
import src.main.scala.dataFormat.MBB
import src.main.scala.dataFormat.MBR
import src.main.scala.dataFormat.BaseSetting
import src.main.scala.dataFormat.Point
 

import src.main.scala.index.RTree

import src.main.scala.selfPartitioner.PartitionerByTime
import src.main.scala.selfPartitioner.PartitionerBySpecifyID

import src.main.scala.util.PublicFunc



object SpaceTimePar{

    def main(args : Array[String]) : Unit = {
        val conf = new SparkConf()
                       .setAppName("SpaceTimePar")
                       .set("spark.driver.host", "192.168.1.74")
                       .set("spark.driver.port", "8123")
        val sc = new SparkContext(conf)
        // val spark = SparkSession
        //             .builder()
        //             .appName("SpaceTimePar")
        //             .getOrCreate()
        // val sc = spark.sparkContext

        val myBaseSettings : BaseSetting = new BaseSetting
        myBaseSettings.setDelta(500)
        myBaseSettings.setContiSnap(360)
        myBaseSettings.setTotalSnap(17280)
        myBaseSettings.setBeginSecond(0)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setRootPath("hdfs:///changzhihao/1000Day0Zorder/1000Day0Zorder/")
        myBaseSettings.setTimePartitionsNum(48)
        myBaseSettings.setSpacePartitionsNum(10)
        myBaseSettings.setTotalTrajNums(1000)
        myBaseSettings.setPatIDList(11)
        myBaseSettings.setRecordLength(10)

        val inputFilePath : String = myBaseSettings.rootPath.concat("par*.zhihao")
        val mapFilePath   : String = myBaseSettings.rootPath.concat("par_map*.txt")
        
        
        val mapOfIDToZvalue : Map[Int, Array[Array[Int]]] = getMapValue(sc, mapFilePath)

        // val recordRDD : RDD[RecordWithSnap] = readRDDAndMapToRecord(sc, inputFilePath, myBaseSettings, mapOfIDToZvalue)
        val recordRDD : RDD[RecordWithSnap] = readRDD(sc, inputFilePath, myBaseSettings, mapOfIDToZvalue)
        
        val indexRDD  : RDD[(RTree, MBR)] = setIndexOnPartition(recordRDD, myBaseSettings)
        
        doSearchEntry(sc, myBaseSettings, indexRDD)
        
        sc.stop()
    }

    /**
    * The purpose is to get the mapping status of the space partition in each time partition
    * Example:
    * trajectory id------corresponding space partition id------corresponding position in the space partition
    * 20    0   0
    * 16    0   1
    * 80    0   2
    * 37    1   0
    * 79    1   1
    * 69    1   2
    * 30    2   0
    * ...
    */
    def getMapValue(sc : SparkContext, mapFilePath : String) : Map[Int, Array[Array[Int]]] = {
        val inputRDD   : RDD[String] = sc.textFile(mapFilePath)
        val arrayValue : Array[Array[String]] = inputRDD.glom()
                                                        .collect()
        var mapOfIDToZvalue : Map[Int, Array[Array[Int]]] = Map[Int, Array[Array[Int]]]()
        for (i <- 0 to arrayValue.length - 1){
            val eachValue : Array[Array[Int]] = Array.ofDim(arrayValue(i).length, 3)
            for (j <- 0 to arrayValue(i).length - 1){
                val line  : String = arrayValue(i)(j)
                val words : Array[String] = line.split("\t")
                eachValue(j)(0) = words(0).toInt // trajectory id
                eachValue(j)(1) = words(1).toInt // corresponding space partition id
                eachValue(j)(2) = words(2).toInt // corresponding position in the space partition
            }
            mapOfIDToZvalue += i -> eachValue
        }
        mapOfIDToZvalue
    }

    def readRDDAndMapToRecord(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSetting, 
                                mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : RDD[RecordWithSnap] = {
        val totalPartitionsNum : Int = myBaseSettings.timePartitionsNum * myBaseSettings.spacePartitionsNum
        val inputRDD  : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, 10)
        val recordRDD : RDD[RecordWithSnap] = inputRDD.map(l => new RecordWithSnap(ByteBuffer.wrap(l.slice(0, 4)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(4, 8)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(8, 16)).getDouble, 
                                                                                    ByteBuffer.wrap(l.slice(16, 24)).getDouble))
                                                      .map(l => (l, 1))
                                                      .partitionBy(new PartitionerByTime(myBaseSettings.timePartitionsNum, myBaseSettings))
                                                      .map(l => l._1)
                                                      .mapPartitions(l => getPartitionID(l, myBaseSettings.spacePartitionsNum, mapOfIDToZvalue))
                                                      .partitionBy(new PartitionerBySpecifyID(totalPartitionsNum))
                                                      .map(l => l._2)
        recordRDD
    }

    def readRDD(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSetting, 
                mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : RDD[RecordWithSnap] = {
        val inputRDD  : RDD[Array[Byte]]    = sc.binaryRecords(inputFilePath, myBaseSettings.recordLength)
        val recordRDD : RDD[RecordWithSnap] = inputRDD.mapPartitions(l => fromFileGetID(l, myBaseSettings, mapOfIDToZvalue))
        recordRDD
    }

    def fromFileGetID(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, 
                        mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : Iterator[RecordWithSnap] = {
        val partitionID : Int = TaskContext.get.partitionId
        val timeMapData : Array[Array[Int]] = mapOfIDToZvalue.get(partitionID / myBaseSettings.spacePartitionsNum).get
        var resultArray : ArrayBuffer[RecordWithSnap] = ArrayBuffer[RecordWithSnap]()
        val startSnap   : Int = (partitionID % myBaseSettings.spacePartitionsNum) * (myBaseSettings.totalTrajNums / myBaseSettings.spacePartitionsNum)
        var currSnap    : Int = startSnap
        var count       : Int = 0
        for (record <- iter){
            val time : Short = ByteBuffer.wrap(record.slice(0, 2)).getShort
            val lon  : Float = ByteBuffer.wrap(record.slice(2, 6)).getFloat
            val lat  : Float = ByteBuffer.wrap(record.slice(6, 10)).getFloat
            resultArray += new RecordWithSnap(timeMapData(currSnap)(0), time.toInt, lon.toDouble, lat.toDouble)
            count += 1
            if (count == myBaseSettings.totalSnap / myBaseSettings.timePartitionsNum){
                currSnap += 1
                count = 0
            }
        }
        resultArray.iterator
    }

    def getPartitionID(iterator : Iterator[RecordWithSnap], spacePartitionsNum : Int, 
                        mapOfIDToZvalue : Map[Int, Array[Array[Int]]]) : Iterator[(Int, RecordWithSnap)] = {
        val thisPartitionID : Int = TaskContext.get.partitionId
        val zMapValue   : Array[Array[Int]] = mapOfIDToZvalue.get(thisPartitionID).get
        var result      : ArrayBuffer[(Int, RecordWithSnap)] = new ArrayBuffer[(Int, RecordWithSnap)]()
        while(iterator.hasNext){
            val record   : RecordWithSnap = iterator.next()
            val id       : Int = record.id
            val returnID : Int = (thisPartitionID * spacePartitionsNum) + zMapValue(id)(1)
            result += ((returnID, record))
        }
        result.iterator
    }

    def setIndexOnPartition(inputRDD : RDD[RecordWithSnap], myBaseSettings : BaseSetting) : RDD[(RTree, MBR)] = {
        val indexRDD : RDD[(RTree, MBR)] = inputRDD.mapPartitions(l => mapToRTree(l, myBaseSettings)).persist(StorageLevel.MEMORY_AND_DISK)
        val time1 : Long = System.currentTimeMillis
        println(indexRDD.count())
        val time2 : Long = System.currentTimeMillis
        println("Index build time: " + ((time2 - time1) / 1000.0).toDouble)
        indexRDD
    }

    /**
    * Build RTree inside each partition
    */
    def mapToRTree(iterator : Iterator[RecordWithSnap], myBaseSettings : BaseSetting) : Iterator[(RTree, MBR)] = {
        println("------" + TaskContext.get.partitionId + "------")
        val minCap : Int = 30
        val maxCap : Int = 50
        var root   : RTree = new RTree(null, 1, minCap, maxCap, null, 1)
        var minLon : Double = Double.MaxValue
        var maxLon : Double = Double.MinValue
        var minLat : Double = Double.MaxValue
        var maxLat : Double = Double.MinValue
        for (record <- iterator){
            val myPoint : Point = new Point(record.id, record.currSec.toShort, record.lon.toFloat, record.lat.toFloat)
            root = RTree.insert(root, myPoint)
            minLon = min(minLon, record.lon)
            maxLon = max(maxLon, record.lon)
            minLat = min(minLat, record.lat)
            maxLat = max(maxLat, record.lat)
        }
        val myMBR : MBR = new MBR(minLon.toFloat, maxLon.toFloat, minLat.toFloat, maxLat.toFloat)
        Array((root, myMBR)).iterator
    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSetting, indexRDD : RDD[(RTree, MBR)]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach{patID => {
            val patPath : String = myBaseSettings.rootPath + "query/trajectory"  + patID.toString + ".zhihao"
            val patCoorList : Array[(Int, Double, Double)] = sc.binaryRecords(patPath, myBaseSettings.recordLength)
                                                               .map(l => (ByteBuffer.wrap(l.slice(0, 2)).getShort.toInt, 
                                                                            ByteBuffer.wrap(l.slice(2, 6)).getFloat.toDouble, 
                                                                                ByteBuffer.wrap(l.slice(6, 10)).getFloat.toDouble))
                                                               .collect()
            val bcPatCoor : Broadcast[Array[(Int, Double, Double)]] = sc.broadcast(patCoorList)
            
            val time1 : Long = System.currentTimeMillis
            val candiList : Array[Array[Int]] = indexRDD.mapPartitions(l => mapSearchWithIndex(l, myBaseSettings, bcPatCoor))
                                                        .glom()
                                                        .collect()
            val time2 : Long = System.currentTimeMillis
            println("Query time on the index: " + ((time2 - time1) / 1000.0).toDouble)
            
            var partitionIDList : ArrayBuffer[Int] = new ArrayBuffer[Int]()
            var partitionIDMap  : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            var candidateNum : Int = 0
            for (i <- 0 to candiList.length - 1){
                if (candiList(i).length != 1){
                    partitionIDList += candiList(i)(0)
                    var temBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    for (j <- 1 to candiList(i).length - 1){
                        temBuffer += candiList(i)(j)
                        candidateNum += 1
                    }
                    partitionIDMap += candiList(i)(0) -> temBuffer
                }
            }
            println("Number of partitions with results: " + partitionIDList.size)
            println("Number of candidate trajectories: " + candidateNum)
            
            val inputFilePath : String = getInputFilePath(myBaseSettings, partitionIDList)
            val inputRDD : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, 24)
            // TODO
            val finalResult = inputRDD.mapPartitions(l => mapSearchWithRefine(l, myBaseSettings, partitionIDList, partitionIDMap, bcPatCoor))
                                      .collect()
            println(inputFilePath)
        }
        }
    }

    def mapSearchWithIndex(iterator : Iterator[(RTree, MBR)], myBaseSettings : BaseSetting, 
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
            val line  : Tuple2[RTree, MBR] = iterator.next()
            val root  : RTree = line._1
            val myMBR : MBR = line._2
            if (ifIntersect(myMBR, startSnap, stopSnap, patCoorList, delta)){
                var currSnap : Int = startSnap
                while(currSnap < stopSnap){
                    val minOffset : Short = patCoorList(currSnap)._1.toShort
                    val maxOffset : Short = patCoorList(currSnap)._1.toShort
                    val minLon    : Float = (patCoorList(currSnap)._2 - lonDiff).toFloat
                    val maxLon    : Float = (patCoorList(currSnap)._2 + lonDiff).toFloat
                    val minLat    : Float = (patCoorList(currSnap)._3 - latDiff).toFloat
                    val maxLat    : Float = (patCoorList(currSnap)._3 + latDiff).toFloat
                    val myMBB     : MBB = new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
                    val temList   : ArrayBuffer[Int] = root.search(myMBB)
                    temMap += currSnap -> temList
                    currSnap += (contiSnap / 2)
                }
            }
        }
        var currSnap : Int = startSnap
        while((currSnap + contiSnap) < stopSnap){
            val firList : ArrayBuffer[Int] = temMap.get(currSnap).get
            val secList : ArrayBuffer[Int] = temMap.get(currSnap + contiSnap).get
            resultList ++= firList intersect secList
            currSnap += contiSnap
        }
        resultList.iterator
    }

    def ifIntersect(myMBR : MBR, startSnap : Int, stopSnap : Int, patCoorList : Array[(Int, Double, Double)], delta : Double) : Boolean = {
        var minLon   : Double = Double.MaxValue
        var maxLon   : Double = Double.MinValue
        var minLat   : Double = Double.MaxValue
        var maxLat   : Double = Double.MinValue
        val lonDiff  : Double = abs(delta / 111111 / cos(patCoorList(startSnap)._3))
        val latDiff  : Double = delta / 111111
        var currSnap : Int = startSnap
        while(currSnap < stopSnap){
            minLon = min(minLon, patCoorList(currSnap)._2)
            maxLon = max(maxLon, patCoorList(currSnap)._2)
            minLat = min(minLat, patCoorList(currSnap)._3)
            maxLat = max(maxLat, patCoorList(currSnap)._3)
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

    def getInputFilePath(myBaseSettings : BaseSetting, idList : ArrayBuffer[Int]) : String = {
        val pathPrefix         : String = myBaseSettings.rootPath
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        var path : String = pathPrefix + "{"
        for (i <- 0 to idList.length - 1){
            val id      : Int = idList(i)
            val timeID  : Int = id / spacePartitionsNum
            val spaceID : Int = id % spacePartitionsNum
            val name    : String = "par" + timeID.toString + "zorder" + spaceID.toString
            path = path + name
            if (i != idList.length - 1){
                path = path + ","
            }
        }
        path = path + "}.txt"
        path
    }

    def mapSearchWithRefine(iterator : Iterator[Array[Byte]], myBaseSettings : BaseSetting, partitionIDList : ArrayBuffer[Int], 
                            partitionIDMap : Map[Int, ArrayBuffer[Int]], bcPatCoor : Broadcast[Array[(Int, Double, Double)]]) : Iterator[Array[Int]] = {
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int = myBaseSettings.contiSnap
        val totalSnap          : Int = myBaseSettings.totalSnap
        val timeInterval       : Int = myBaseSettings.timeInterval
        val delta              : Double = myBaseSettings.delta
        

        val partitionID  : Int = TaskContext.get.partitionId
        val mapKey       : Int = partitionIDList(partitionID)
        val candiList    : ArrayBuffer[Int] = partitionIDMap.get(mapKey).get
        val patCoorList  : Array[(Int, Double, Double)] = bcPatCoor.value
        val startSnap    : Int = totalSnap / timePartitionsNum * (mapKey / spacePartitionsNum) + myBaseSettings.beginSecond
        var result       : Array[Array[Int]] = Array.ofDim(candiList.length, totalSnap / timePartitionsNum)
        val lonDiff      : Double = abs(delta / 111111 / cos(patCoorList(0)._2))
        val latDiff      : Double = delta / 111111
        var candiListMap : Map[Int, Int] = Map[Int, Int]()
        
        for (i <- 0 to candiList.length - 1){
            candiListMap += (candiList(i)) -> i
        }
        var loop = new Breaks
        while(iterator.hasNext){
            val line : Array[Byte] = iterator.next()
            val id   : Int = ByteBuffer.wrap(line.slice(0, 4)).getInt
            loop.breakable{
            if (!candiList.contains(id)){
                loop.break
            }
            val currSnap : Int = ByteBuffer.wrap(line.slice(4, 8)).getInt
            val snapShot : Int = (currSnap - myBaseSettings.beginSecond) / timeInterval
            val lon      : Double = ByteBuffer.wrap(line.slice(8, 16)).getDouble
            val lat      : Double = ByteBuffer.wrap(line.slice(16, 24)).getDouble
            val minLon   : Double = patCoorList(snapShot)._2 - lonDiff
            val maxLon   : Double = patCoorList(snapShot)._2 + lonDiff
            val minLat   : Double = patCoorList(snapShot)._3 - latDiff
            val maxLat   : Double = patCoorList(snapShot)._3 + latDiff
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                result(candiListMap.get(id).get)(snapShot - startSnap) = 1
            }else{
                result(candiListMap.get(id).get)(snapShot - startSnap) = 0
            }
            }
        }
        result.iterator
    }

}