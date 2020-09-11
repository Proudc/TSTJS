package src.main.scala.method

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel



import scala.math._
import scala.collection.mutable.ArrayBuffer

import src.main.scala.dataFormat.RecordWithSnap
import src.main.scala.dataFormat.BaseSetting
import src.main.scala.util.PublicFunc


/**
* The most violent solution algorithm, only guarantees correctness
* This algorithm is used to verify the correctness of other algorithms
*/
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
        
        val recordRDD : RDD[RecordWithSnap] = readRDDAndMapToRecord(sc, inputFilePath, myBaseSettings)
        
        deSearchEntry(sc, myBaseSettings, recordRDD)
        
        sc.stop()
    }

    def readRDDAndMapToRecord(sc : SparkContext, inputFilePath : String, myBaseSettings : BaseSettings) : RDD[RecordWithSnap] = {
        val inputRDD  : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, 24)
        val recordRDD : RDD[RecordWithSnap] = inputRDD.map(l => new RecordWithSnap(ByteBuffer.wrap(l.slice(0, 4)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(4, 8)).getInt, 
                                                                                    ByteBuffer.wrap(l.slice(8, 16)).getDouble, 
                                                                                    ByteBuffer.wrap(l.slice(16, 24)).getDouble))
                                                      .persist(StorageLevel.MEMORY_AND_DISK)
        recordRDD
        
    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSettings, recordRDD : RDD[RecordWithSnap]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach{patID => {
            // TODO
            val patPath     : String = myBaseSettings.rootPath + patID.toString + "..."
            val patCoorList : Array[(Int, Double, Double)] = sc.binaryRecords(patPath)
                                                               .map(l => (ByteBuffer.wrap(l.slice(4, 8)).getInt, 
                                                                            ByteBuffer.wrap(l.slice(8, 16)).getDouble, 
                                                                                ByteBuffer.wrap(l.slice(16, 24)).getDouble))
                                                               .collect()
            val bcPatCoor  : Broadcast[Array[(Int, Double, Double)]] = sc.broadcast(patCoorList)
            val returnList : Array[Int] = recordRDD.mapPartitions(l => mapSearchWithTraverse(l, myBaseSettings, bcPatCoor))
                                                   .collect()
            println(patID, returnList)
        }

        }
    }

    def mapSearchWithTraverse(iter : Iterator[RecordWithSnap], myBaseSettings : BaseSettings, 
                                bcPatCoor : Broadcast[Array[(Int, Double, Double)]]) : Iterator[Int] = {
        
        var temCount  : Int = 0
        var pos       : Int = 0
        var contiFlag : Int = 0
        var maxCount  : Int = -1
        var id        : Int = -1
        val patCoor   : Array[(Int, Double, Double)] = bcPatCoor.value
        
        for (record <- iter){
            id = record.id
            val testLon : Double = record.lon
            val testLat : Double = record.lat
            val lon     : Double = patCoor(pos)._2
            val lat     : Double = patCoor(pos)._3
            val lonDiff : Double = abs(float(myBaseSettings.delta) / 111111 / math.cos(lat))
            val latDiff : Double = float(myBaseSettings.delta)/111111
            val minLon  : Double = lon - lonDiff
            val maxLon  : Double = lon + lonDiff
            val minLat  : Double = lat - latDiff
            val maxLat  : Double = lat + latDiff
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                if (contiFlag == 0){
                    temCount = 1
                    contiFlag = 1
                }else{
                    temCount += 1
                    maxCount = max(temCount, maxCount)
                }
            }else{
                if (contiFlag != 0){
                    contiFlag = 0
                }
            }
        }
        var reList : ArrayBuffer[Int] = ArrayBuffer[Int]()
        if (maxCount >= myBaseSettings.contiSnap){
            reList += id
        }
        reList.iterator
    }


}