package src.main.scala.method.oldVersion

import org.apache.spark.rdd.RDD

import scala.math._

import src.main.scala.dataFormat._
import src.main.scala.util.PublicFunc

object IndexRDD{
    
    def seqMBRForPartition(iterator : Iterator[Record], numObjects : Int, timeDiv : Float, totalTime : Float) : Iterator[Array[RecordIndex]] = {
        var numOfSecDiv : Int = (totalTime / timeDiv).toInt
        var partitionTime : String = "00:00:00"
        var minLonArray : Array[Double] = new Array[Double](numObjects)
        var maxLonArray : Array[Double] = new Array[Double](numObjects)
        var minLatArray : Array[Double] = new Array[Double](numObjects)
        var maxLatArray : Array[Double] = new Array[Double](numObjects)
        var minLonTimeDivArray : Array[Array[Double]] = Array.ofDim(numObjects, numOfSecDiv)
        var maxLonTimeDivArray : Array[Array[Double]] = Array.ofDim(numObjects, numOfSecDiv)
        var minLatTimeDivArray : Array[Array[Double]] = Array.ofDim(numObjects, numOfSecDiv)
        var maxLatTimeDivArray : Array[Array[Double]] = Array.ofDim(numObjects, numOfSecDiv)
        for (i <- 0 to (numObjects - 1)){
            minLonArray(i) = Double.MaxValue
            maxLonArray(i) = Double.MinValue
            minLatArray(i) = Double.MaxValue
            maxLatArray(i) = Double.MinValue
        }
        for (i <- 0 to (numObjects - 1)){
            for (j <- 0 to (numOfSecDiv - 1)){
                minLonTimeDivArray(i)(j) = Double.MaxValue
                maxLonTimeDivArray(i)(j) = Double.MinValue
                minLatTimeDivArray(i)(j) = Double.MaxValue
                maxLatTimeDivArray(i)(j) = Double.MinValue
            }
        }
        for (l <- iterator){
            var id : Int = l.id
            partitionTime = l.time
            var secDiePos : Int = partitionTime.substring(3, 5).toInt / 10
            var lon : Double = l.lon
            var lat : Double = l.lat
            if (minLonTimeDivArray(id)(secDiePos) > lon){
                minLonTimeDivArray(id)(secDiePos) = lon
            }
            if (maxLonTimeDivArray(id)(secDiePos) < lon){
                maxLonTimeDivArray(id)(secDiePos) = lon
            }
            if (minLatTimeDivArray(id)(secDiePos) > lat){
                minLatTimeDivArray(id)(secDiePos) = lat
            }
            if (maxLatTimeDivArray(id)(secDiePos) < lat){
                maxLatTimeDivArray(id)(secDiePos) = lat
            }
            if (minLonArray(id) > lon){
                minLonArray(id) = lon
            }
            if (maxLonArray(id) < lon){
                maxLonArray(id) = lon
            }
            if (minLatArray(id) > lat){
                minLatArray(id) = lat
            }
            if (maxLatArray(id) < lat){
                maxLatArray(id) = lat
            }
        }
        var indexRecord = Array.ofDim[RecordIndex](numObjects, numOfSecDiv+1)
        for (i <- 0 to (numObjects - 1)){
            var cenLon = (minLonArray(i) + maxLonArray(i)) / 2
            var cenLat = (minLatArray(i) + maxLatArray(i)) / 2
            var widthLon = maxLonArray(i) - minLonArray(i)
            var heightLat = maxLatArray(i) - minLatArray(i)
            var temIndexRecord : RecordIndex= new RecordIndex(i, minLonArray(i), maxLonArray(i), minLatArray(i), maxLatArray(i), cenLon, cenLat, widthLon, heightLat, partitionTime)
            indexRecord(i)(0) = temIndexRecord
            for (j <- 0 to (numOfSecDiv - 1)){
                var cenTimeDivLon = (minLonTimeDivArray(i)(j) + maxLonTimeDivArray(i)(j)) / 2
                var cenTimeDivLat = (minLatTimeDivArray(i)(j) + maxLatTimeDivArray(i)(j)) / 2
                var widthTimeDivLon = maxLonTimeDivArray(i)(j) - minLonTimeDivArray(i)(j)
                var heightTimeDivLat = maxLatTimeDivArray(i)(j) - minLatTimeDivArray(i)(j)
                var temTimeDivIndexRecord : RecordIndex = new RecordIndex(i, minLonTimeDivArray(i)(j), maxLonTimeDivArray(i)(j), minLatTimeDivArray(i)(j), maxLatTimeDivArray(i)(j), cenTimeDivLon, cenTimeDivLat, widthTimeDivLon, heightTimeDivLat, partitionTime)
                indexRecord(i)(j + 1) = temTimeDivIndexRecord
            }
        }
        indexRecord.iterator
    }

    def seqMBRAndComBinaryTreeForPartition(iterator : Iterator[Record], numObjects : Int, totalTime : Float, treeHeight : Int, numParts : Int) : Iterator[Array[MBRBoundary]] = {
        var totalSnap       : Int = ((totalTime * 60).toInt) / 5
        var miniumLength    : Int = totalSnap / (pow(2, treeHeight - 1).toInt)
        var secDimLength    : Int = (pow(2, treeHeight).toInt) - 1
        var thisPartitionID : Int = 0
        var totalData       : Array[Array[Record]]      = Array.ofDim(numObjects, totalSnap)
        var indexArray      : Array[Array[MBRBoundary]] = Array.ofDim(numObjects, secDimLength)
        for (l <- iterator){
            var id     : Int      = l.id
            var time   : String   = l.time
            var secPos : Int      = PublicFunc.changeTimeToSnap(time) % totalSnap
            thisPartitionID       = PublicFunc.getPartitionID(time, numParts)
            totalData(id)(secPos) = l
        }
        for (i <- 0 to (numObjects - 1)){
            var temCount : Int = 0
            var temPos   : Int = 0
            var minLon   : Double = Double.MaxValue
            var maxLon   : Double = Double.MinValue
            var minLat   : Double = Double.MaxValue
            var maxLat   : Double = Double.MinValue
            for (j <- 0 to (totalSnap - 1)){
                var longitude : Double = totalData(i)(j).lon
                var latitude  : Double = totalData(i)(j).lat
                temCount += 1;
                if (minLon > longitude){
                    minLon = longitude
                }
                if (maxLon < longitude){
                    maxLon = longitude
                }
                if (minLat > latitude){
                    minLat = latitude
                }
                if (maxLat < latitude){
                    maxLat = latitude
                }
                if (temCount == miniumLength){
                    var temMBR : MBRBoundary = new MBRBoundary(i, thisPartitionID, temCount, minLon, maxLon, minLat, maxLat)
                    indexArray(i)(temPos)    = temMBR
                    minLon = Double.MaxValue
                    maxLon = Double.MinValue
                    minLat = Double.MaxValue
                    maxLat = Double.MinValue
                    temCount = 0
                    temPos += 1
                }
            }
            var beginPos : Int = pow(2, treeHeight-1).toInt - 2
            for (j <- 0 to beginPos){
                var pos      : Int = beginPos - j
                var posLeft  : Int = pos * 2 + 1
                var posRight : Int = pos * 2 + 2
                var leftMBR  = indexArray(i)(posLeft)
                var rightMBR = indexArray(i)(posRight)
                minLon = min(leftMBR.minLon, rightMBR.minLon)
                maxLon = max(leftMBR.maxLon, rightMBR.maxLon)
                minLat = min(leftMBR.minLat, rightMBR.minLat)
                maxLat = max(leftMBR.maxLat, rightMBR.maxLat)
                var temMBR : MBRBoundary = new MBRBoundary(i, thisPartitionID, leftMBR.duraTime + rightMBR.duraTime, minLon, maxLon, minLat, maxLat)
                indexArray(i)(pos) = temMBR
            }
        }
        indexArray.iterator
    }
}