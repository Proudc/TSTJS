package src.main.scala.method.oldVersion

import org.apache.spark.SparkContext

import scala.util.control.Breaks._
import scala.math._

import src.main.scala.util.PublicFunc
import src.main.scala.selfPartitioner._
import src.main.scala.dataFormat._


object Refine{
    def refineFilter(sc : SparkContext, patientPath : String, personIDList : Array[Int], delta : Int, rootPath : String, contiSnap : Int, totalSnap : Int) : Array[Int] = {
        var patientList = sc.textFile(patientPath)
                           .map(l => l.split("\t"))
                           .filter(l => l.length == 5)
                           .map(l => Array(l(3).toFloat, l(4).toFloat))
                           .collect()
        var minLonArray : Array[Float] = new Array[Float](totalSnap)
        var maxLonArray : Array[Float] = new Array[Float](totalSnap)
        var minLatArray : Array[Float] = new Array[Float](totalSnap)
        var maxLatArray : Array[Float] = new Array[Float](totalSnap)
        for (i <- 0 to (totalSnap - 1)){
            var lon : Float = patientList(i)(0)
            var lat : Float = patientList(i)(1)
            var lonDiff : Float = abs(delta.toFloat / 111111 / cos(lat).toFloat)
            var latDiff : Float = delta.toFloat / 111111
            minLonArray(i) = (lon - lonDiff)
            maxLonArray(i) = (lon + lonDiff)
            minLatArray(i) = (lat - latDiff)
            maxLatArray(i) = (lat + latDiff)
        }
        if (personIDList.length != 0){
            var temPath : String =  "{" + personIDList(0).toString
            for (id <- 1 to (personIDList.length - 1)){
                temPath = temPath + "," + personIDList(id).toString
            }
            temPath = temPath + "}"
            var personPath : String = rootPath + temPath + ".txt"
            // println(personPath)
            val startTime1 : Long = System.currentTimeMillis
            var finalResultRDD = sc.textFile(personPath)
            finalResultRDD.count()
            val endTime1 : Long = System.currentTimeMillis
            println("读文件时间为: " + (endTime1 - startTime1).toString)
            // println(finalResultRDD.getNumPartitions)
            var finalResultList = finalResultRDD.mapPartitions(l => getFinalResult(l, contiSnap, minLonArray, maxLonArray, minLatArray, maxLatArray))
                                                .collect()
            println(finalResultList.length)
            finalResultList
        }else{
            var finalResultList = Array(-1)
            finalResultList
        }
    }

    def getFinalResult(iterator : Iterator[String], contiSnap : Int, minLonArray : Array[Float], maxLonArray : Array[Float], minLatArray : Array[Float], maxLatArray : Array[Float]) : Iterator[Int] = {
        var finalResultList = List[Int]()
        var pos : Int = 0
        var flag : Int = 0
        var temConti : Int = 0
        var temID : Int = 0
        for (l <- iterator){
            // println(l)
            var line : Array[String] = l.split("\t")
            // for (each <- line){
            //     println(each)
            // }
            var id : Int = line(0).toInt
            temID = id
            // println(id)
            var lon : Float = line(3).toFloat
            var lat : Float = line(4).toFloat
            var minLon : Float = minLonArray(pos)
            var maxLon : Float = maxLonArray(pos)
            var minLat : Float = minLatArray(pos)
            var maxLat : Float = maxLatArray(pos)
            pos += 1
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                if (flag == 0){
                    temConti = 1
                    flag = 1
                }else{
                    temConti += 1
                    if (temConti >= contiSnap){
                        // println("id = " + id)
                        finalResultList = finalResultList.+:(id)
                        return finalResultList.iterator
                        break
                    }
                }
            }else{
                if (flag != 0){
                    flag = 0
                }
            }
        }
        // println(temID)
        finalResultList.iterator
    }

    def refineFilterPar(sc : SparkContext, patientPath : String, personIDListOrder : Array[Int], delta : Int, rootPath : String, contiSnap : Int, totalSnap : Int, personMap : Map[Int, List[(Int, Int)]]) : Array[Int] = {
        var patientList = sc.textFile(patientPath)
                            .map(l => l.split("\t"))
                            .filter(l => l.length == 5)
                            .map(l => Array(l(3).toFloat, l(4).toFloat))
                            .collect()
        var minLonArray : Array[Float] = new Array[Float](totalSnap)
        var maxLonArray : Array[Float] = new Array[Float](totalSnap)
        var minLatArray : Array[Float] = new Array[Float](totalSnap)
        var maxLatArray : Array[Float] = new Array[Float](totalSnap)
        for (i <- 0 to (totalSnap - 1)){
            var lon : Float = patientList(i)(0)
            var lat : Float = patientList(i)(1)
            var lonDiff : Float = abs(delta.toFloat / 111111 / cos(lat).toFloat)
            var latDiff : Float = delta.toFloat / 111111
            minLonArray(i) = (lon - lonDiff)
            maxLonArray(i) = (lon + lonDiff)
            minLatArray(i) = (lat - latDiff)
            maxLatArray(i) = (lat + latDiff)
        }

        var mapPos : Map[Int, Int] = Map()
        for (i <- 0 to (personIDListOrder.length - 1)){
            mapPos += personIDListOrder(i) -> i
        }
        println("###")
        if (personIDListOrder.length != 0){
            var temPath : String =  "{"
            var localPath : String = ""
            var beginFlag : Int = 0
            for (id <- 0 to (personIDListOrder.length - 1)){
                var eachPersonList = personMap.get(personIDListOrder(id))
                for (each <- eachPersonList){
                    var start : Int = each(0)._1
                    var stop : Int = each(0)._2
                    for (i <- start to stop){
                        localPath = personIDListOrder(id).toString + "par" + i.toString
                        if (beginFlag == 0){
                            temPath = temPath + localPath
                            beginFlag = 1
                        }else{
                            temPath = temPath + "," + localPath
                        }
                    }
                }
            }
            temPath = temPath + "}"
            var personPath : String = rootPath + temPath + ".txt"
            println("***")
            var finalResultList = sc.wholeTextFiles(personPath)
                                    .map(l => firstMapFuncWTF(l, mapPos))
                                    .partitionBy(new PartitionerOfWTF(personIDListOrder.length))
                                    .map(l => l._2)
                                    .mapPartitions(l => getFinalResultPar(l, contiSnap, minLonArray, maxLonArray, minLatArray, maxLatArray))
                                    .collect()
            finalResultList
        }else{
            var finalResultList = Array(-1)
            finalResultList
        }
    }

    def getTrajID(trajPath : String) : Int = {
        var temString : String = "trajectory"
        var beginPos : Int = trajPath.indexOf(temString, 42).toInt + 10
        var i : Int = beginPos
        var endPos : Int = 0
        // println(trajPath)
        while (i < trajPath.length()){
            // println("&&&")
            // println(i)
            // println(trajPath.charAt(i))
            if ((trajPath.charAt(i) >= 48) && (trajPath.charAt(i) <= 57)){
                i += 1
            }else{
                endPos = i
                // println(endPos)
                var finalValue : Int = trajPath.substring(beginPos, endPos).toInt
                // println(finalValue)
                return finalValue
                // break
            }
        }
        var finalValue : Int = trajPath.substring(beginPos, endPos).toInt
        return finalValue
    }

    def firstMapFuncWTF(data : Tuple2[String, String], mapPos : Map[Int, Int]) : Tuple2[Int, (String, String)] = {
        // println("@@@")
        var finalValue = getTrajID(data._1)
        
        var returnTupleID = mapPos.get(finalValue)
        // println("$$$")
        // println(returnTupleID)
        var returnTuple = (returnTupleID.get, data)
        // println(finalValue, returnTupleID)
        returnTuple
    }

    def getFinalResultPar(iterator : Iterator[(String, String)], contiSnap : Int, minLonArray : Array[Float], maxLonArray : Array[Float], minLatArray : Array[Float], maxLatArray : Array[Float]) : Iterator[Int] = {
        // println("getFinalResultPar")
        var finalResultList = List[Int]()
        var totalData = List[(String, String)]()
        var totalRecordData = List[String]()
        for (l <- iterator){
            totalData = totalData.+:(l)
        }
        totalData.sorted
        // println(totalData.length)
        // println("finish first loop")
        for (l <- totalData){
            var recordData : String = l._2;
            var recordDataList : Array[String] = recordData.split("\n")
            for (line <- recordDataList){
                totalRecordData = totalRecordData.+:(line)
            }
        }
        // println(totalRecordData.length)
        // println("finish second loop")
        if (totalRecordData.length == 0){
            return finalResultList.iterator
        }
        var firstLine : Array[String] = totalRecordData(0).split("\t")
        var firstTime : String = firstLine(2)
        var pos : Int = PublicFunc.changeTimeToSnap(firstTime)
        var flag : Int = 0
        var temConti : Int = 0
        // println("begin third loop")
        for (l <- totalRecordData){
            // println("pos = ", pos)
            var line : Array[String] = l.split("\t")
            var id : Int = line(0).toInt
            var lon : Float = line(3).toFloat
            var lat : Float = line(4).toFloat
            var minLon : Float = minLonArray(pos)
            var maxLon : Float = maxLonArray(pos)
            var minLat : Float = minLatArray(pos)
            var maxLat : Float = maxLatArray(pos)
            pos += 1
            if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, lon, lat)){
                if (flag == 0){
                    temConti = 1
                    flag = 1
                }else{
                    temConti += 1
                    if (temConti >= contiSnap){
                        // println(id)
                        finalResultList = finalResultList.+:(id)
                        break
                    }
                }
            }else{
                if (flag != 0){
                    flag = 0
                }
            }
        }
        // println(finalResultList)
        finalResultList.iterator
    }
}