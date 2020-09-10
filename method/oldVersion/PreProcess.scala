package src.main.scala.method.oldVersion

import org.apache.spark.rdd.RDD

import scala.math._
import scala.collection.mutable

import src.main.scala.dataFormat._
import src.main.scala.util.PublicFunc

object PreProcess{

    def timeTrajMapFunc(iterator : Iterator[Array[RecordIndex]], patientID : Int, delta : Int, numObjects : Int) : Iterator[(Int, Int, Int)] = {
        var totalData = Array.ofDim[RecordIndex](numObjects, 7)
        var pos : Int = 0
        var patientInfo = new Array[RecordIndex](7)
        for (l <- iterator){
            totalData(pos) = l
            pos += 1
        }
        for (l <- totalData){
            if (l(0).id == patientID){
                patientInfo = l
            }
        }
        var cenLon : Double = patientInfo(0).cenLon
        var cenLat : Double = patientInfo(0).cenLat
        var widthLon : Double = patientInfo(0).widthLon
        var heightLat : Double = patientInfo(0).heightLat
        var lonDiff : Double = abs(delta.toFloat / 111111 / cos(cenLat).toFloat)
        var latDiff : Double = delta.toFloat / 111111
        widthLon += (lonDiff * 2)
        heightLat += (latDiff * 2)
        var minLon : Double = cenLon - (widthLon) / 2
        var maxLon : Double = cenLon + (widthLon) / 2
        var minLat : Double = cenLat - (heightLat) / 2
        var maxLat : Double = cenLat + (heightLat) / 2
        var resultList = List[(Int, Int, Int)]()
        for (l <- totalData){
            if ((widthLon / 2 + l(0).widthLon / 2) >= abs(cenLon - l(0).cenLon) && (heightLat / 2 + l(0).heightLat / 2) >= abs(cenLat - l(0).cenLat)){
                var partitionID = l(0).partitionTime.substring(0, 2).toInt
                var count = 720
                for (i <- 1 to 6){
                    var cenLonTime = patientInfo(i).cenLon
                    var cenLatTime = patientInfo(i).cenLat
                    var widthLonTime = patientInfo(i).widthLon
                    var heightLatTime = patientInfo(i).heightLat
                    widthLonTime += (lonDiff * 2)
                    heightLatTime += (latDiff * 2)
                    if (!((widthLonTime / 2 + l(i).widthLon / 2) >= abs(cenLonTime - l(i).cenLon) && (heightLatTime / 2 + l(i).heightLat / 2) >= abs(cenLatTime - l(i).cenLat))){
                        count -=120
                    }
                }
                resultList.::=(l(0).id, l(0).partitionTime.substring(0, 2).toInt, count)
            }
        }
        resultList.iterator
    }

    def timeTrajMapFuncBinaryTree(iterator : Iterator[Array[MBRBoundary]], patientID : Int, delta : Int, numObjects : Int, treeHeight : Int, totalTime : Float) : Iterator[(Int, Int, Int)] = {
        var totalSnap : Int = ((totalTime * 60).toInt) / 5
        var miniumLength : Int = totalSnap / (pow(2, treeHeight - 1).toInt)
        var secDimLength : Int = (pow(2, treeHeight).toInt) - 1
        var indexArray : Array[Array[MBRBoundary]] = Array.ofDim(numObjects, secDimLength)
        var patientMBR : Array[MBRBoundary] = new Array(secDimLength)
        var resultList = List[(Int, Int, Int)]()
        for (l <- iterator){
            var id : Int = l(0).id
            indexArray(id) = l
        }
        var patientInfo = indexArray(patientID)
        var lonDiff : Float = abs(delta.toFloat / 111111 / cos(patientInfo(0).cenLat).toFloat)
        var latDiff : Float = delta.toFloat / 111111
        for (l <- indexArray){
            var totalInsecTime : Int = 0
            var testInfo = l
            var pos : Int = 0
            var queue = new mutable.Queue[Int]
            queue += 0
            while(queue.length != 0){
                pos = queue.dequeue()
                var patientCenLon : Double = patientInfo(pos).cenLon
                var patientCenLat : Double = patientInfo(pos).cenLat
                var patientWidthLon : Double = patientInfo(pos).widthLon
                var patientHeigthLat : Double = patientInfo(pos).heightLat
                var testCenLon : Double = testInfo(pos).cenLon
                var testCenLat : Double = testInfo(pos).cenLat
                var testWidthLon : Double = testInfo(pos).widthLon
                var testHeigthLat : Double = testInfo(pos).heightLat
                patientWidthLon += (lonDiff * 2)
                patientHeigthLat += (latDiff * 2)
                if (PublicFunc.ifIntersectOfMBR(patientCenLon, patientCenLat, patientWidthLon, patientHeigthLat, testCenLon, testCenLat, testWidthLon, testHeigthLat)){
                    if (patientInfo(pos).duraTime <= miniumLength){
                        totalInsecTime += patientInfo(pos).duraTime
                    }
                    if ((pos * 2 + 1) < patientMBR.length){
                        queue += (pos * 2 + 1)
                        queue += (pos * 2 + 2)
                    }
                }
            }
            if (totalInsecTime != 0){
                resultList.::=(testInfo(0).id, testInfo(0).parID, totalInsecTime)
            }
        }
        resultList.iterator
    }
}