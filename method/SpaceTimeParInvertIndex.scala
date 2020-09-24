package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import scala.collection.mutable.Set

import java.nio.ByteBuffer

import src.main.scala.dataFormat.BaseSetting


object SpaceTimeParInvertIndex{
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .appName("SpaceTimeParInvertIndex")
                    .getOrCreate()
        val sc = spark.sparkContext

        val myBaseSettings : BaseSetting = new BaseSetting
        myBaseSettings.setDelta(500)
        myBaseSettings.setContiSnap(360)
        myBaseSettings.setTotalSnap(17280)
        myBaseSettings.setBeginSecond(0)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setRootPath("hdfs:///changzhihao/1000Day0Zorder/1000Day0Zorder/")
        myBaseSettings.setTimePartitionsNum(48)
        myBaseSettings.setSpacePartitionsNum(10)
        myBaseSettings.settrajNumEachSpace()
        myBaseSettings.setTotalTrajNums(1000)
        myBaseSettings.setPatIDList(11)
        myBaseSettings.setRecordLength(10)

        myBaseSettings.setIndexSnapInterval();
        myBaseSettings.setOneParSnapNum();
        myBaseSettings.lonGridNum();
        myBaseSettings.latGridNum();
        myBaseSettings.setMINLON();
        myBaseSettings.setMINLAT();
        myBaseSettings.setLonGridLength();
        myBaseSettings.setLatGridLength();

        val inputFilePath     : String = myBaseSettings.rootPath.concat("par*.zhihao")
        val lookupTableFilePath : String = myBaseSettings.rootPath.concat("par_map*.txt")

        val lookupTable : Array[Int] = getLookupTable(sc, lookupTableFilePath)
        val bcLookupTable : Broadcast[Array[Int]] = sc.broadcast(lookupTable)

        val indexRDD : RDD[Array[InvertedIndex]] = setIndex(sc, inputFilePath, myBaseSettings, bcLookupTable)

        doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
    }

    def getLookupTable(sc : SparkContext, lookupTableFilePath : String, myBaseSettings : BaseSetting) : Array[Int] = {
        val inputByte : Array[Array[Array[Byte]]] = sc.binaryRecords(lookupTableFilePath, 3)
                                                      .glom()
                                                      .collect()
        var lookupTable : Array[Int] = new Array[Int](myBaseSettings.timePartitionsNum * myBaseSettings.totalTrajNums * 3)
        for (i <- 0 until inputByte.length){
            for (j <- 0 until inputByte(i).length){
                val line : Array[Byte] = inputByte(i)(j)
                val firPos : Int = i * myBaseSettings.totalTrajNums * 3 + j * 3
                val spaceID : Int = ByteBuffer.wrap(line.slice(0, 1)).toInt
                val spaceOrder : Int = ByteBuffer.wrap(line.slice(1, 3)).toInt
                lookupTable(firPos)     = spaceID // corresponding space partition id
                lookupTable(firPos + 1) = spaceOrder // corresponding position in the space partition
                val secPos : Int = i * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + spaceOrder * 3 + 2
                lookupTable(secPos) = j // corresponding traj id
            }
        }
        lookupTable
    }

    def setIndex(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSetting, 
                    bcLookupTable : Broadcast[Array[Int]]) : RDD[Array[InvertedIndex]] = {
        val inputRDD : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, myBaseSettings.recordLength)
                                            .coalesce(myBaseSettings.timePartitionsNum)
        val indexRDD : RDD[Array[InvertedIndex]] = inputRDD.mapPartitions(l => mapToInvertedIndex(l, myBaseSettings, bcLookupTable))
    }

    def mapToInvertedIndex(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, 
                            bcLookupTable : Broadcast[Array[Int]]) : Iterator[Array[InvertedIndex]] = {
        val partitionID : Int = TaskContext.get.partitionId
        // val timeMapData : Array[Array[Int]] = lookTable.get(partitionID / myBaseSettings.spacePartitionsNum).get
        // val timeMapData : Array[Array[Int]] = lookTable.get(partitionID).get
        val lookupTable : Array[Int] = bcLookupTable.value
        val indexSnapInterval : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum
        val lonGridNum : Int = myBaseSettings.lonGridNum
        val latGridNum : Int = myBaseSettings.latGridNum
        val MINLON : Float =  myBaseSettings.MINLON
        val MINLAT : Float = myBaseSettings.MINLAT
        val lonGridLength : Float = myBaseSettings.lonGridLength
        val latGridLength : Float = myBaseSettings.latGridLength

        
        var temIndex : Array[Array[ArrayBuffer[Int]]] = Array.ofDim(lonGridNum * lonGridNum, oneParSnapNum / indexSnapInterval)
        var temPos : Int = 0
        var parSeq : Int = 0
        for (record <- iter){
            if (temPos % oneParSnapNum == 0){
                parSeq = temPos / oneParSnapNum
            }
            if (temPos % indexSnapInterval == 0){
                val timePos : Int = temPos / indexSnapInterval
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                val lonGridID : Int = ceil((lon - MINLON) / lonGridLength)
                val latGridID : Int = ceil((lat - MINLAT) / latGridLength)
                val gridID : Int = latGridID * lonGridNum + lonGridID
                // val trajID : Int = timeMapData(partitionID * myBaseSettings.spacePartitionsNum + parSeq)(2)
                val trajID : Int = lookupTable(parSeq * 3 + 2)
                if (temIndex(gridID)(timePos) == null){
                    var temArray : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    temArray += trajID
                    temIndex(gridID)(timePos) = temArray
                }else{
                    temIndex(gridID)(timePos) += trajID
                }
            }
            temPos += 1
        }
        var invertedIndex : Array[InvertedIndex] = new Array[InvertedIndex](oneParSnapNum / indexSnapInterval)
        for (i <- 0 until invertedIndex.length){
            var oneIndex : InvertedIndex = new InvertedIndex(myBaseSettings.totalTrajNums, lonGridNum * latGridNum)
            invertedIndex(i) = oneIndex
        }
        for (i <- 0 until temIndex.length){
            val temArray : Array[ArrayBuffer[Int]] = temIndex(i)
            for (j <- 0 until temArray.length){
                val myArrayBuffer : ArrayBuffer[Int] = temArray(j)
                val length : Int = temArray(j).length
                if (i == 0){
                    invertedIndex(j).index(0) = 0
                    invertedIndex(j).index(1) = length
                    invertedIndex(j).indexPos = 2
                    var temPos = invertedIndex(j).idArrayPos
                    for (k <- 0 until length){
                        invertedIndex(j).idArray(temPos) = myArrayBuffer(k)
                        temPos += 1
                    }
                    invertedIndex(j).idArrayPos = temPos
                }else{
                    var temPos = invertedIndex(j).indexPos
                    invertedIndex(j).index(temPos) = invertedIndex(j).index(temPos - 2) + invertedIndex(j).index(temPos - 1)
                    invertedIndex(j).index(temPos + 1) = length
                    invertedIndex(j).indexPos += 2
                    var temPos = invertedIndex(j).idArrayPos
                    for (k <- 0 to length - 1){
                        invertedIndex(j).idArray(temPos) = myArrayBuffer(k)
                        temPos += 1
                    }
                    invertedIndex(j).idArrayPos = temPos
                }
            }
        }
        Array(invertedIndex).iterator
    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSetting, indexRDD : RDD[Array[InvertedIndex]], 
                        lookupTable : Array[Int]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach{patID => {
            val patPath : String = myBaseSettings.rootPath + "query/trajectory"  + patID.toString + ".zhihao"
            val patCoorList : Array[(Float, Float)] = sc.binaryRecords(patPath, myBaseSettings.recordLength)
                                                        .map(l => (ByteBuffer.wrap(l.slice(0, 4)).getFloat, 
                                                                    ByteBuffer.wrap(l.slice(4, 8)).getFloat))
                                                        .collect()
            val bcPatCoor : Broadcast[Array[(Float, Float)]] = sc.broadcast(patCoorList)
            val candiList : Array[Array[Int]] = indexRDD.mapPartitions(l => mapSearchWithIndex(l, myBaseSettings, bcPatCoor))
                                                        .glom()
                                                        .collect()
            var mapOfParToTraj : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            val inputFilePath : String = getInputFilePath(myBaseSettings, candiList, lookupTable, mapOfParToTraj)
            
        }
        }
    }

    def mapSearchWithIndex(iter : Iterator[Array[InvertedIndex]], myBaseSettings : BaseSetting, 
                            bcPatCoor : Broadcast[(Float, Float)]) : Iterator[Int] = {
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        
        val indexSnapInterval : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum     : Int = myBaseSettings.oneParSnapNum
        val lonGridNum        : Int = myBaseSettings.lonGridNum
        val latGridNum        : Int = myBaseSettings.latGridNum
        val MINLON            : Float =  myBaseSettings.MINLON
        val MINLAT            : Float = myBaseSettings.MINLAT
        val lonGridLength     : Float = myBaseSettings.lonGridLength
        val latGridLength     : Float = myBaseSettings.latGridLength

        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = totalSnap / timePartitionsNum * partitionID
        val stopSnap    : Int = totalSnap / timePartitionsNum * (partitionID + 1)
        val patCoorList : Array[(Float, Float)] = bcPatCoor.value
        val lonDiff     : Double = abs(delta / 111111 / cos(patCoorList(startSnap)._2))
        val latDiff     : Double = delta / 111111
        var temMap      : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        
        val invertedIndex : Array[InvertedIndex] = iter.next()

        var currSnap : Int = startSnap
        while(currSnap < stopSnap){
            val myInvertedIndex : InvertedIndex = invertedIndex(currSnap / indexSnapInterval)
            val minLon : Float = (patCoorList(currSnap)._1 - lonDiff)
            val maxLon : Float = (patCoorList(currSnap)._1 + lonDiff)
            val minLat : Float = (patCoorList(currSnap)._2 - latDiff)
            val maxLat : Float = (patCoorList(currSnap)._2 + latDiff)
            val minLonGridID : Int = ceil((minLon - MINLON) / lonGridLength)
            val maxLonGridID : Int = ceil((maxLon - MINLON) / lonGridLength)
            val minLatGridID : Int = ceil((minLat - MINLAT) / latGridLength)
            val maxLatGridID : Int = ceil((maxLat - MINLAT) / latGridLength)
            var temList : ArrayBuffer[Int] = ArrayBuffer[Int]()
            for (lonGridID <- minLonGridID to maxLonGridID){
                for (latGridID <- minLatGridID to maxLatGridID){
                    val gridID   : Int = latGridID * lonGridNum + lonGridID
                    val beginPos : Int = myInvertedIndex.index(gridID * 2)
                    val length   : Int = myInvertedIndex.index(gridID * 2 + 1);
                    for (i <- 0 to length - 1){
                        temList += myInvertedIndex.idArray(beginPos + i)
                    }
                }
            }
            temMap += currSnap -> temList
            currSnap += indexSnapInterval
        }
        var currSnap : Int = startSnap
        resultList ++= temMap.get(currSnap).get
        val stepNum : Int = ceil(contiSnap.toDouble / indexSnapInterval)
        while((currSnap + (stepNum - 1) * indexSnapInterval) < stopSnap){
            var temList : ArrayBuffer[Int] = temMap.get(currSnap).get
            for (i <- 0 to stepNum - 1){
                var firList : ArrayBuffer[Int] = temMap.get(currSnap + i * indexSnapInterval).get
                temList = temList intersect firList
            }
            resultList ++= temList
        }
        resultList.iterator
    }

    def getInputFilePath(myBaseSettings : BaseSetting, candiList : Array[Array[Int]]
                            lookupTable : Array[Int], mapOfParToTraj : Map[Int, ArrayBuffer[Int]]) : String = {
        val pathPrefix         : String = myBaseSettings.rootPath
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        var path : String = pathPrefix + "{"
        var totalPos : Int = 0
        for (i <- 0 until candiList.length){
            val timeID : Int = i
            var spaceSet : Set[Int] = Set[Int]()
            var spaceMap : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            for (j <- 0 until candiList(i).length){
                val trajID : Int = candiList(i)(j)
                val spaceID : Int = lookupTable(i * myBaseSettings + trajID * 3)
                if (spaceSet.contains(spaceID)){
                    var tem : ArrayBuffer[Int] = spaceMap.get(spaceID).get
                    tem += trajID
                    spaceMap += spaceID -> tem
                }else{
                    spaceSet.add(spaceID)
                    var tem : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    tem += timeID
                    tem += spaceID
                    tem += trajID
                    spaceMap += spaceID -> tem
                }
            }
            var spaceArray : Array[Int] = spaceSet.toArray
            for (j <- 0 until spaceArray.length){
                val spaceID : Int = spaceArray(j)
                val name : String = "par" + timeID.toString + "zorder" + spaceID.toString
                mapOfParToTraj += totalPos -> spaceMap.get(spaceID).get
                totalPos += 1
                if ((i == candiList.length - 1) && (j == spaceArray.length - 1)){
                    path = path + name
                }else{
                    path = path + name + ","
                }
            }
        }
        path = path + "}.txt"
        path
    }

    def mapSearchWithRefine(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, mapOfParToTraj : Map[Int, ArrayBuffer[Int]], 
                            bcPatCoor : Broadcast[Array[(Float, Float)]], bcLookupTable : Broadcast[Array[Int]]) : Iterator[Int] = {
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int = myBaseSettings.contiSnap
        val totalSnap          : Int = myBaseSettings.totalSnap
        val timeInterval       : Int = myBaseSettings.timeInterval
        val delta              : Double = myBaseSettings.delta


        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum

        val lookupTable : Array[Int] = bcLookupTable.value
        
        
        

        val partitionID  : Int = TaskContext.get.partitionId
        var candiList : ArrayBuffer[Int] = mapOfParToTraj.get(partitionID).get
        val timeID : Int = candiList(0)
        val spaceID : Int = candiList(1)
        candiList = candiList.slice(2, candiList.length)


        val patCoorList  : Array[(Float, Float)] = bcPatCoor.value
        val startSnap : Int = timeID * oneParSnapNum
        val lonDiff : Float = abs(float(myBaseSettings.delta) / 111111 / math.cos(patCoorList(startSnap)._2)).toFloat
        val latDiff : Float = float(myBaseSettings.delta)/111111

        var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var rearResult : ArrayBuffer[Int] = ArrayBuffer[Int]()

        var totalPos : Int = 0
        while(iter.hasNext){
            var record : Array[Byte] = iter.next()
            if (totalPos % oneParSnapNum == 0){
                val parSeq = totalPos / oneParSnapNum
                val temPos : Int = timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + parSeq * 3 + 2
                val trajID : Int = lookupTable(temPos)
                if (candiList.contains(trajID)){
                    var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
                    var flag : Int = 0
                    var temConti : Int = 0
                    var maxConti : Int = 0
                    var pos : Int = 0
                    while(pos < oneParSnapNum){
                        val testLon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                        val testLat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                        val lon : Float = patCoorList(startSnap + pos)._1
                        val lat : Float = patCoorList(startSnap + pos)._2
                        val minLon : Float = lon - lonDiff
                        val maxLon : Float = lon + lonDiff
                        val minLat : Float = lat - latDiff
                        val maxLat : Float = lat + latDiff
                        if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, testLon, testLat)){
                            flagArray[pos] = 1
                            if (flag == 0){
                                temConti = 1
                            }else{
                                temConti += 1
                            }
                        }else{
                            if (flag != 0){
                                flag = 0
                                if (maxConti < temConti){
                                    maxConti = temConti
                                }
                            }
                        }
                        record = iter.next()
                        pos += 1
                        totalPos += 1
                    }
                    if (maxConti >= contiSnap){
                        finalResult += trajID
                    }else{
                        if (flagArray[0] == 1){
                            var count : Int = 0
                            var loop = new Breaks
                            loop.breakable{
                            for (i <- 0 to oneParSnapNum - 1){
                                if (flagArray(i) == 1){
                                    count += 1
                                }else{
                                    loop.break()
                                }
                            }
                            }
                            frontResult += trajID
                            frontResult += count
                        }
                        if (flagArray[oneParSnapNum - 1] == 1){
                            var count : Int = 0
                            var loop = new Breaks
                            loop.breakable{
                            for (i <- oneParSnapNum - 1 to 0 by -1){
                                if (flagArray(i) == 1){
                                    count += 1
                                }else{
                                    loop.break()
                                }
                            }
                            }
                            rearResult += trajID
                            rearResult += count
                        }
                    }
                }
            }
            totalPos += 1
        }
        var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
        result += finalResult.length
        for (i <- 0 until finalResult.length){
            result += finalResult(i)
        }
        result += frontResult.length
        for (i <- 0 until frontResult.length){
            result += frontResult(i)
        }
        result += rearResult.length
        for (i <- 0 until rearResult.length){
            result += rearResult(i)
        }
        result.iterator

    }
}