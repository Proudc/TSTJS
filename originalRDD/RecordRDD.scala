package src.main.scala.OriginalRDD

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext


import scala.math._
import scala.collection.mutable.ArrayBuffer

import src.main.scala.index._
import src.main.scala.dataFormat._
import src.main.scala.util.PublicFunc
import src.main.scala.index.ThreeDimRTree

class RecordRDD{

    var recordRDD : RDD[Record] = null

    var indexRDD : RDD[ThreeDimRTree] = null

    var partitionsNum : Int = _

    def this(sc : SparkContext, inputFilePath : String, partitionsNum : Int){
        this()
        var temRDD : RDD[String] = sc.textFile(inputFilePath, partitionsNum)
        this.setRecordRDD(temRDD.mapPartitions(mapToRecord))
        this.partitionsNum = this.recordRDD.getNumPartitions
    }

    private def mapToRecord(iterator : Iterator[String]) : Iterator[Record] = {
        var temList : ArrayBuffer[Record] = ArrayBuffer[Record]()
        while(iterator.hasNext){
            val line : String = iterator.next()
            val words : Array[String] = line.split("\t")
            var record : Record = new Record(words(0).toInt, words(1), words(2), words(3).toDouble, words(4).toDouble)
            temList += record
        }
        temList.iterator
    }

    private def setRecordRDD(recordRDD : RDD[Record]){
        this.recordRDD = recordRDD
    }

    def setIndexOnPartition(indexType : String) = indexType match{
        case "ThreeDimRTree" => this.indexRDD = this.recordRDD.mapPartitions(mapToThreeDimRTree)
    }

    private def mapToThreeDimRTree(iterator : Iterator[Record]) : Iterator[ThreeDimRTree] = {
        val minCap : Int = 10
        val maxCap : Int = 30
        var root : ThreeDimRTree = new ThreeDimRTree(null, 1, minCap, maxCap, null, 1, -1)
        while(iterator.hasNext){
            val record : Record = iterator.next()
            val snap : Int = PublicFunc.changeTimeToSnap(record.time, 5)
            var MBB : Map[String, Double] = Map[String, Double]()
            MBB += "minOffset" -> snap.toDouble
            MBB += "maxOffset" -> snap.toDouble
            MBB += "minLon" -> record.lon
            MBB += "maxLon" -> record.lon
            MBB += "minLat" -> record.lat
            MBB += "maxLat" -> record.lat
            var node : ThreeDimRTree = new ThreeDimRTree(null, 0, minCap, maxCap, null, 0, record.id)
            root = ThreeDimRTree.insert(root, node)
        }
        val rootList : Array[ThreeDimRTree] = Array(root)
        rootList.iterator 
    }

    def search(patientList : Array[Record], indexType : String, delta : Double, contiSnap : Int) : Array[Int] = indexType match{
        case "ThreeDimRTree" => {
            val resultList : Array[Int] = this.indexRDD.mapPartitions(l => mapSearchWithThreeDimRTree(l, patientList, delta, contiSnap))
                                                        .collect()
            resultList
        }
    }

    def mapSearchWithThreeDimRTree(iterator : Iterator[ThreeDimRTree], patientList : Array[Record], delta : Double, contiSnap : Int) : Iterator[Int] = {
        val partitionID : Int = TaskContext.get.partitionId
        val startSnap   : Int = patientList.length / this.partitionsNum * partitionID
        val stopSnap    : Int = patientList.length / this.partitionsNum * (partitionID + 1)
        var currSnap    : Int = startSnap
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        val lonDiff     : Double = abs(delta / 111111 / cos(patientList(0).lat))
        val latDiff     : Double = delta / 111111
        while(iterator.hasNext){
            val root : ThreeDimRTree = iterator.next()
            while(currSnap < stopSnap){
                var MBB : Map[String, Double] = Map[String, Double]()
                MBB += "minOffset" -> (currSnap - startSnap).toDouble
                MBB += "maxOffset" -> (currSnap - startSnap).toDouble
                MBB += "minLon" -> (patientList(currSnap).lon - lonDiff)
                MBB += "maxLon" -> (patientList(currSnap).lon + lonDiff)
                MBB += "minLat" -> (patientList(currSnap).lat - latDiff)
                MBB += "maxLat" -> (patientList(currSnap).lat + latDiff)
                resultList ++= root.search(MBB)
                currSnap += contiSnap
            }
        }
        resultList.iterator
    }

}