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

object Baseline{
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .appName("Baseline")
                    .getOrCreate()
        val sc = spark.sparkContext

        val myBaseSettings : BaseSettings = new BaseSettings
        myBaseSettings.setDelta(500)
        myBaseSettings.setContiSnap(360)
        myBaseSettings.setTotalSnap(17280)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setRootPath("file:///mnt/disk_data_hdd/changzhihao/random_traj_data/101Day0/")
        myBaseSettings.setTotalTrajNums(100)
        myBaseSettings.setPatIDList(21)
        
        val inputFilePath : String = myBaseSettings.rootPath.concat("trajectory*.txt")
        val recordRDD : RDD[Record] = readRDDAndMapToRecord(sc, inputFilePath, myBaseSettings)
        deSearchEntry(sc, myBaseSettings, recordRDD)
    }

    def readRDDAndMapToRecord(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSettings) : RDD[Record] = {
        val inputRDD  : RDD[String] = sc.textFile(inputFilePath, myBaseSettings.totalTrajNums)
        val recordRDD : RDD[Record] = inputRDD.map(l => l.split("\t"))
                                              .filter(l => l.length == 5)
                                              .map(l => new Record(l(0).toInt, l(1), l(2), l(3).toDouble, l(4).toDouble))
                                              .persist(StorageLevel.MEMORY_AND_DISK)
        recordRDD
        
    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSettings, recordRDD : RDD[Record]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach{patID => {
            // TODO
            val patPath : String = myBaseSettings.rootPath + patID.toString + "..."
            val patCoorList : Array[(Double, Double)] = sc.textFile(patPath)
                                                              .map(l => l.split("\t"))
                                                              .map(l => (l(3).toDouble, l(4).toDouble))
                                                              .collect()
            val bcPatCoor : Broadcast[Array[(Double, Double)]] = sc.broadcast(patCoorList)
            val returnList : Array[Int] = recordRDD.mapPartitions(l => mapSearchWithTraverse(l, myBaseSettings, bcPatCoor))
                                                   .collect()
            println(patID, returnList)
        }

        }
    }

    def mapSearchWithTraverse(iter : Iterator[Record], myBaseSettings : BaseSettings, bcPatCoor : Broadcast[Array[(Double, Double)]]) : Iterator[Int] = {
        var temCount : Int = 0
        var pos      : Int = 0
        var flag     : Int = 0
        val patCoor  : Array[(Double, Double)] = bcPatCoor.value
        var maxCount : Int = -1
        var id : Int = 0
        while(iter.hasNext){
            val record : Record = iter.next()
            id = record.id
            val testLon : Double = record.lon
            val testLat : Double = record.lat
            val lon     : Double = patCoor(pos)._1
            val lat     : Double = patCoor(pos)._2
            val lonDiff : Double = abs(float(myBaseSettings.delta) / 111111 / math.cos(lat))
            val latDiff : Double = float(myBaseSettings.delta)/111111
            val minLon  : Double = lon - lonDiff
            val maxLon  : Double = lon + lonDiff
            val minLat  : Double = lat - latDiff
            val maxLat  : Double = lat + latDiff
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                if (flag == 0){
                    temCount = 1
                    flag = 1
                }else{
                    temCount += 1
                    maxCount = max(temCount, maxCount)
                }
            }else{
                if (flag != 0){
                    flag = 0
                }
            }
        }
        var reList   : ArrayBuffer[Int] = ArrayBuffer[Int]()
        if (maxCount >= myBaseSettings.contiSnap){
            reList += id
        }
        reList.iterator
    }


}