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

import java.nio.ByteBuffer

import src.main.scala.dataFormat.BaseSetting


object SpaceTimeParInvertIndex{
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .appName("SpaceTimePar")
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
        myBaseSettings.setTotalTrajNums(1000)
        myBaseSettings.setPatIDList(11)
        myBaseSettings.setRecordLength(10)

        val inputFilePath : String = myBaseSettings.rootPath.concat("par*.zhihao")
        val mapFilePath   : String = myBaseSettings.rootPath.concat("par_map*.txt")

        val lookTable : Map[Int, Array[Array[Int]]] = getLookTable(sc, mapFilePath)

        val indexRDD : = setIndex(sc, inputFilePath, myBaseSettings, lookTable)
    }

    def getLookTable(sc : SparkContext, mapFilePath : String, myBaseSettings : BaseSetting) : Map[Int, Array[Array[Int]]] = {
        val inputByte : Array[Array[Array[Byte]]] = sc.textFile(mapFilePath, 3)
                                                      .glom()
                                                      .collect()
        var lookTable : Map[Int, Array[Array[Int]]] = Map[Int, Array[Array[Int]]]()
        for (i <- 0 to inputByte.length - 1){
            val eachValue : Array[Array[Int]] = Array.ofDim(inputByte(i).length, 3)
            for (j <- 0 to inputByte(i).length - 1){
                val line : Array[Byte] = inputByte(i)(j)
                eachValue(j)(0) = ByteBuffer.wrap(line.slice(0, 1)).toInt // corresponding space partition id
                eachValue(j)(1) = ByteBuffer.wrap(line.slice(1, 3)).toInt // corresponding position in the space partition
            }
            for (j <- 0 to inputByte(i).length - 1){
                val pos = myBaseSettings.spacePartitionsNum * eachValue(j)(0) + eachValue(j)(1)
                eachValue(pos)(2) = j
            }
            lookTable += i -> eachValue

        }
        lookTable
    }

    def readRDD(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSetting, 
                lookTable : Map[Int, Array[Array[Int]]]) : RDD[RecordWithSnap] = {
        val inputRDD  : RDD[Array[Byte]]    = sc.binaryRecords(inputFilePath, myBaseSettings.recordLength)
        val recordRDD : RDD[RecordWithSnap] = inputRDD.mapPartitions(l => getID(l, myBaseSettings, lookTable))
        recordRDD
    }

    def getID(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, 
                lookTable : Map[Int, Array[Array[Int]]]) : Iterator[RecordWithSnap] = {
        
    }

    def setIndex(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSetting, 
                    lookTable : Map[Int, Array[Array[Int]]]) : RDD[InvertedIndex] = {
        val inputRDD : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, myBaseSettings.recordLength)
        val indexRDD : RDD[InvertedIndex] = inputRDD.mapPartitions(l => mapToInvertedIndex(l, myBaseSettings, lookTable))
    }

    def mapToInvertedIndex(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, 
                            lookTable : Map[Int, Array[Array[Int]]]) : Iterator[InvertedIndex] = {
        val partitionID : Int = TaskContext.get.partitionId
        val timeMapData : Array[Array[Int]] = mapOfIDToZvalue.get(partitionID / myBaseSettings.spacePartitionsNum).get
        val indexSnapInterval : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum
        val lonGridNum : Int = myBaseSettings.lonGridNum
        val latGridNum : Int = myBaseSettings.latGridNum
        val MINLON : Float =  myBaseSettings.MINLON
        val MINLAT : Float = myBaseSettings.MINLAT
        val lonGridLength : Float = myBaseSettings.lonGridLength
        val latGridLength : Float = myBaseSettings.latGridLength

        
        var temIndex : Array[Array[ArrayBuffer[Int]]] = Array.ofDim(lonGridNum * lonGridNum, oneParSnapNum / indexSnapInterval)
        var temData : Array[] = 
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
                val trajID : Int = timeMapData(partitionID * myBaseSettings.spacePartitionsNum + parSeq)(2)
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
        var invertedIndex : Array[InvertedIndex] = Array[InvertedIndex](oneParSnapNum / indexSnapInterval)
        for (i <- 0 to invertedIndex.length - 1){
            var oneIndex : InvertedIndex = new InvertedIndex(myBaseSettings.totalTrajNums, lonGridNum * latGridNum)
            invertedIndex(i) = oneIndex
        }
        for (i <- 0 to temIndex.length - 1){
            val temArray : Array[ArrayBuffer[Int]] = temIndex(i)
            for (j <- 0 to temArray.length - 1){
                val myArrayBuffer : ArrayBuffer[Int] = temArray(j)
                val length : Int = temArray(j).length
                if (i == 0){
                    invertedIndex(j).index(0) = 0
                    invertedIndex(j).index(1) = length
                    invertedIndex(j).indexPos = 2
                    var temPos = invertedIndex(j).idArrayPos
                    for (k <- 0 to length - 1){
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
        invertedIndex.iterator
    }



}