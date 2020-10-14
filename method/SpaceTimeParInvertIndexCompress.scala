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
import scala.collection.mutable.Set
import scala.collection.mutable.Map

import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import src.main.scala.dataFormat.BaseSetting
import src.main.scala.index.InvertedIndex
import src.main.scala.util.PublicFunc
import src.main.scala.selfPartitioner.PartitionerByIndex
import src.main.scala.selfPartitioner.PartitionerByFilename
import src.main.scala.selfPartitioner.PartitionerByFilenamemap



object SpaceTimeParInvertIndexCompress{
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .appName("SpaceTimeParInvertIndex")
                    .getOrCreate()
        val sc = spark.sparkContext
        // val conf = new SparkConf()
        //                .setAppName("SpaceTimeParInvertIndexCompress")
        //                .set("spark.files.maxPartitionBytes", "819200")
        //                .set("spark.files.openCostInBytes", "168960")
        // val sc = new SparkContext(conf)

        val myBaseSettings : BaseSetting = new BaseSetting
        myBaseSettings.setDelta(500)
        myBaseSettings.setContiSnap(360)
        myBaseSettings.setTotalSnap(17280)
        myBaseSettings.setBeginSecond(0)
        myBaseSettings.setTimeInterval(5)
        // myBaseSettings.setRootPath("hdfs:///changzhihao/100000Day0Zorder/")
        // myBaseSettings.setRootPath("file:///home/changzhihao/test_data/100000Day0Zorder/")
        myBaseSettings.setRootPath("file:///home/changzhihao/test_data/100000Day0Zorder_gzip/")
        

        myBaseSettings.setTimePartitionsNum(48)

        myBaseSettings.setSpacePartitionsNum(100)
        myBaseSettings.setTrajNumEachSpace(1000)
        myBaseSettings.setTotalTrajNums(100000)
        
        myBaseSettings.setPatIDList(11)
        myBaseSettings.setRecordLength(8)

        myBaseSettings.setIndexSnapInterval(12);
        myBaseSettings.setOneParSnapNum(360);
        myBaseSettings.setLonGridNum(100);
        myBaseSettings.setLatGridNum(100);
        // beijing minLon=115.7438
        // beijing maxLon=117.0769
        // beijing minLat=39.30998
        // beijing maxLat=40.38032
        myBaseSettings.setMINLON((115.7438).toFloat);
        myBaseSettings.setMINLAT((39.30998).toFloat);
        myBaseSettings.setLonGridLength((0.013331).toFloat);
        myBaseSettings.setLatGridLength((0.0107034).toFloat);

        // val inputFilePath : Array[String] = getInitialInputFilePath(myBaseSettings, "par_ori", "zorder")
        val inputFilePath : Array[String] = getInitialInputFilePath(myBaseSettings, "par", "zorder")
        val lookupTableFilePath : Array[String] = getLookupTableFilePath(myBaseSettings, "par_map")

        val time1 : Long = System.currentTimeMillis
        val lookupTable : Array[Int] = getLookupTable(sc, lookupTableFilePath, myBaseSettings)
        
        val time2 : Long = System.currentTimeMillis
        println("The time to get the LookupTable is: " + ((time2 - time1) / 1000.0).toDouble)

        val bcLookupTable : Broadcast[Array[Int]] = sc.broadcast(lookupTable)

        val indexRDD : RDD[Array[InvertedIndex]] = setIndexUsingGz(sc, inputFilePath, myBaseSettings, bcLookupTable)


        doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
    }

    def getInitialInputFilePath(myBaseSettings : BaseSetting, prefix1 : String, prefix2 : String) : Array[String] = {
        var finalPath : Array[String] = new Array[String](myBaseSettings.timePartitionsNum * myBaseSettings.spacePartitionsNum)
        for (i <- 0 until myBaseSettings.timePartitionsNum){
            for (j <- 0 until myBaseSettings.spacePartitionsNum){
                finalPath(i * myBaseSettings.spacePartitionsNum + j) = myBaseSettings.rootPath.concat(prefix1).concat(i.toString).concat(prefix2).concat(j.toString).concat(".tstjs.gz")
            }
        }
        finalPath
    }

    def getLookupTableFilePath(myBaseSettings : BaseSetting, prefix : String) : Array[String] = {
        var finalPath : Array[String] = new Array[String](myBaseSettings.timePartitionsNum)
        for (i <- 0 until myBaseSettings.timePartitionsNum){
            finalPath(i) = myBaseSettings.rootPath.concat(prefix).concat(i.toString).concat(".tstjs")
        }
        finalPath
    }

    def getLookupTable(sc : SparkContext, lookupTableFilePath : Array[String], myBaseSettings : BaseSetting) : Array[Int] = {
        val inputByte : Array[Array[Array[Byte]]] = sc.binaryRecords(lookupTableFilePath.mkString(","), 3)
                                                      .glom()
                                                      .collect()
        var lookupTable : Array[Int] = new Array[Int](myBaseSettings.timePartitionsNum * myBaseSettings.totalTrajNums * 3)
        for (i <- 0 until inputByte.length){
            for (j <- 0 until inputByte(i).length){
                val line : Array[Byte] = inputByte(i)(j)
                val firPos : Int = i * myBaseSettings.totalTrajNums * 3 + j * 3
                val spaceID : Int = ByteBuffer.wrap(line.slice(0, 1)).get.toInt
                val spaceOrder : Int = ByteBuffer.wrap(line.slice(1, 3)).getShort.toInt
                lookupTable(firPos)     = spaceID // corresponding space partition id
                lookupTable(firPos + 1) = spaceOrder // corresponding position in the space partition
                val secPos : Int = i * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + spaceOrder * 3 + 2
                lookupTable(secPos) = j // corresponding traj id
            }
        }
        lookupTable
    }

    def setIndexUsingGz(sc : SparkContext, inputFilePath : Array[String], myBaseSettings : BaseSetting, 
                    bcLookupTable : Broadcast[Array[Int]]) : RDD[Array[InvertedIndex]] = {
        val inputRDD1 = sc.binaryFiles(inputFilePath.mkString(","))
        
                          
        // val x = inputRDD1.map(_._1).glom().collect()
        
        val inputRDD : RDD[Array[Byte]] = inputRDD1.partitionBy(new PartitionerByFilename(myBaseSettings.timePartitionsNum * myBaseSettings.spacePartitionsNum))
                                                   .map(_._2.toArray)
                                                   .map(unGzip)
                                                   .map(l => l.grouped(8).toArray)
                                                   .flatMap(_.iterator)
                                                   
        println(inputRDD.getNumPartitions)
        val indexRDD : RDD[Array[InvertedIndex]] = inputRDD.mapPartitions(l => mapSpaceParToInvertedIndex(l, myBaseSettings, bcLookupTable))
                                                           .partitionBy(new PartitionerByIndex(myBaseSettings.timePartitionsNum, myBaseSettings))
                                                           .mapPartitions(l => coalesceSpaceParIndex(l, myBaseSettings))
                                                           .persist(StorageLevel.MEMORY_AND_DISK)
        val time1 : Long = System.currentTimeMillis
        println(indexRDD.count())
        val time2 : Long = System.currentTimeMillis
        println("Index build time: " + ((time2 - time1) / 1000.0).toDouble)
        indexRDD
    }

    def unGzip(data : Array[Byte]) : Array[Byte] = {
        var bis  : ByteArrayInputStream = new ByteArrayInputStream(data)
        var gzip : GZIPInputStream = new GZIPInputStream(bis)
        var buf  : Array[Byte] = new Array[Byte](1024)
        var baos : ByteArrayOutputStream = new ByteArrayOutputStream()
        var num : Int = -1
        num = gzip.read(buf, 0, buf.length)
        while(num != -1){
        baos.write(buf, 0, num)
        num = gzip.read(buf, 0, buf.length)
        }
        val b = baos.toByteArray()
        baos.flush()
        baos.close()
        gzip.close()
        bis.close()
        b
    }

    def mapToTest(iter : Iterator[Array[Byte]]) : Iterator[Int] = {
        return Array(23).iterator
        val partitionID : Int = TaskContext.get.partitionId
        var total : Int = 0
        for (record <- iter){
            total += 1
        }
        return Array(46).iterator
    }

    def mapToInvertedIndex(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, 
                            bcLookupTable : Broadcast[Array[Int]]) : Iterator[Array[InvertedIndex]] = {
        
        println("Begin to build index...")
        val partitionID : Int = TaskContext.get.partitionId
        println("partitionID is: " + partitionID)
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

        var invertedIndex : Array[InvertedIndex] = new Array[InvertedIndex](oneParSnapNum / indexSnapInterval + 1)
        
        
        val x : Int = lonGridNum * lonGridNum
        val y : Int = oneParSnapNum / indexSnapInterval
        var temIndex : Array[Array[ArrayBuffer[Int]]] = Array.ofDim[ArrayBuffer[Int]](lonGridNum * lonGridNum, oneParSnapNum / indexSnapInterval + 1)
        // println(x + "\t" + y)
        // var temIndex : Array[Array[ArrayBuffer[Int]]] = Array.ofDim[ArrayBuffer[Int]](x, y)
        // return Array(invertedIndex).iterator

        var temPos : Int = 0
        var parSeq : Int = 0
        
        for (record <- iter){
            if (temPos == 72000){
                println("temPos is 72000\t" + ByteBuffer.wrap(record.slice(0, 4)).getFloat + "\t" + ByteBuffer.wrap(record.slice(4, 8)).getFloat + "\t" + partitionID)
            }
            if (temPos % oneParSnapNum == 0){
                parSeq = temPos / oneParSnapNum
            }
            if ((temPos % oneParSnapNum) % indexSnapInterval == 0){
                val timePos : Int = (temPos % oneParSnapNum) / indexSnapInterval
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                val lonGridID : Int = floor((lon - MINLON) / lonGridLength).toInt
                val latGridID : Int = floor((lat - MINLAT) / latGridLength).toInt
                val gridID : Int = latGridID * lonGridNum + lonGridID
                // val trajID : Int = timeMapData(partitionID * myBaseSettings.spacePartitionsNum + parSeq)(2)
                val trajID : Int = lookupTable(partitionID * myBaseSettings.totalTrajNums * 3 + parSeq * 3 + 2)
                if (trajID == 0){
                    println(lon + "\t" + lat + "\t" + lonGridID + "\t" + latGridID + "\t" + trajID + "\t" + partitionID + "\t" + parSeq + "\t" + timePos + "\t" + oneParSnapNum + "\t" + temPos + "\t" + indexSnapInterval)
                }
                if (temIndex(gridID)(timePos) == null){
                    var temArray : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    temArray += trajID
                    temIndex(gridID)(timePos) = temArray
                }else{
                    var temArray : ArrayBuffer[Int] = temIndex(gridID)(timePos)
                    temArray += trajID
                    temIndex(gridID)(timePos) = temArray
                }
            }
            if ((temPos + 1) % oneParSnapNum == 0){
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                val lonGridID : Int = floor((lon - MINLON) / lonGridLength).toInt
                val latGridID : Int = floor((lat - MINLAT) / latGridLength).toInt
                val gridID : Int = latGridID * lonGridNum + lonGridID
                val trajID : Int = lookupTable(partitionID * myBaseSettings.totalTrajNums * 3 + (temPos / oneParSnapNum) * 3 + 2)
                if (temIndex(gridID)(oneParSnapNum / indexSnapInterval) == null){
                    var temArray : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    temArray += trajID
                    temIndex(gridID)(oneParSnapNum / indexSnapInterval) = temArray
                }else{
                    var temArray : ArrayBuffer[Int] = temIndex(gridID)(oneParSnapNum / indexSnapInterval)
                    temArray += trajID
                    temIndex(gridID)(oneParSnapNum / indexSnapInterval) = temArray
                }
            }
            temPos += 1
        }
        
        for (i <- 0 until invertedIndex.length){
            var oneIndex : InvertedIndex = new InvertedIndex(myBaseSettings.totalTrajNums, lonGridNum * latGridNum)
            invertedIndex(i) = oneIndex
        }
        // println(temIndex)
        for (i <- 0 until temIndex.length){
            val temArray : Array[ArrayBuffer[Int]] = temIndex(i)
            for (j <- 0 until temArray.length){
                val myArrayBuffer : ArrayBuffer[Int] = temArray(j)
                var length : Int = 0
                if (myArrayBuffer != null){
                    length = myArrayBuffer.length
                }
                // val length : Int = temArray(j).length
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
                    temPos = invertedIndex(j).idArrayPos
                    for (k <- 0 until length){
                        invertedIndex(j).idArray(temPos) = myArrayBuffer(k)
                        temPos += 1
                    }
                    invertedIndex(j).idArrayPos = temPos
                }
            }
        }
        Array(invertedIndex).iterator
    }

    def mapSpaceParToInvertedIndex(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, 
                                    bcLookupTable : Broadcast[Array[Int]]) : Iterator[(Int, InvertedIndex)] = {
        val partitionID : Int = TaskContext.get.partitionId
        val timeID : Int = partitionID / (myBaseSettings.spacePartitionsNum)
        val spaceID : Int = partitionID % myBaseSettings.spacePartitionsNum
        
        val lookupTable : Array[Int] = bcLookupTable.value
        val indexSnapInterval : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum
        val lonGridNum : Int = myBaseSettings.lonGridNum
        val latGridNum : Int = myBaseSettings.latGridNum
        val MINLON : Float =  myBaseSettings.MINLON
        val MINLAT : Float = myBaseSettings.MINLAT
        val lonGridLength : Float = myBaseSettings.lonGridLength
        val latGridLength : Float = myBaseSettings.latGridLength

        var invertedIndex : Array[InvertedIndex] = new Array[InvertedIndex](oneParSnapNum / indexSnapInterval + 1)
        var temIndex : Array[Array[ArrayBuffer[Int]]] = Array.ofDim[ArrayBuffer[Int]](lonGridNum * lonGridNum, oneParSnapNum / indexSnapInterval + 1)

        var temPos : Int = 0
        var parSeq : Int = 0
        for (record <- iter){
            if (temPos % oneParSnapNum == 0){
                parSeq = temPos / oneParSnapNum
            }
            if ((temPos % oneParSnapNum) % indexSnapInterval == 0){
                val timePos : Int = (temPos % oneParSnapNum) / indexSnapInterval
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                val lonGridID : Int = floor((lon - MINLON) / lonGridLength).toInt
                val latGridID : Int = floor((lat - MINLAT) / latGridLength).toInt
                val gridID : Int = latGridID * lonGridNum + lonGridID
                val trajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + parSeq * 3 + 2)
                if (temIndex(gridID)(timePos) == null){
                    var temArray : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    temArray += trajID
                    temIndex(gridID)(timePos) = temArray
                }else{
                    var temArray : ArrayBuffer[Int] = temIndex(gridID)(timePos)
                    temArray += trajID
                    temIndex(gridID)(timePos) = temArray
                }
            }
            if ((temPos + 1) % oneParSnapNum == 0){
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                val lonGridID : Int = floor((lon - MINLON) / lonGridLength).toInt
                val latGridID : Int = floor((lat - MINLAT) / latGridLength).toInt
                val gridID : Int = latGridID * lonGridNum + lonGridID
                val trajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + (temPos / oneParSnapNum) * 3 + 2)
                if (temIndex(gridID)(oneParSnapNum / indexSnapInterval) == null){
                    var temArray : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    temArray += trajID
                    temIndex(gridID)(oneParSnapNum / indexSnapInterval) = temArray
                }else{
                    var temArray : ArrayBuffer[Int] = temIndex(gridID)(oneParSnapNum / indexSnapInterval)
                    temArray += trajID
                    temIndex(gridID)(oneParSnapNum / indexSnapInterval) = temArray
                }
            }
            temPos += 1
        }
        for (i <- 0 until invertedIndex.length){
            var oneIndex : InvertedIndex = new InvertedIndex(myBaseSettings.totalTrajNums / myBaseSettings.spacePartitionsNum, lonGridNum * latGridNum)
            invertedIndex(i) = oneIndex
        }
        for (i <- 0 until temIndex.length){
            val temArray : Array[ArrayBuffer[Int]] = temIndex(i)
            for (j <- 0 until temArray.length){
                val myArrayBuffer : ArrayBuffer[Int] = temArray(j)
                var length : Int = 0
                if (myArrayBuffer != null){
                    length = myArrayBuffer.length
                }
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
                    temPos = invertedIndex(j).idArrayPos
                    for (k <- 0 until length){
                        invertedIndex(j).idArray(temPos) = myArrayBuffer(k)
                        temPos += 1
                    }
                    invertedIndex(j).idArrayPos = temPos
                }
            }
        }
        var returnResult : Array[(Int, InvertedIndex)] = new Array[(Int, InvertedIndex)](oneParSnapNum / indexSnapInterval + 1)
        for (i <- 0 until returnResult.length){
            returnResult(i) = (timeID * (myBaseSettings.spacePartitionsNum * (oneParSnapNum / indexSnapInterval + 1))+ spaceID * (oneParSnapNum / indexSnapInterval + 1) + i, invertedIndex(i))
        }
        returnResult.iterator
    }

    def coalesceSpaceParIndex(iter : Iterator[(Int, InvertedIndex)], myBaseSettings : BaseSetting) : Iterator[Array[InvertedIndex]] = {
        val iterArray : Array[(Int, InvertedIndex)] = iter.toArray
        val sortArray : Array[(Int, InvertedIndex)] = iterArray.sortBy(l => (l._1))
        val indexSnapInterval : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum
        var invertedIndex : Array[InvertedIndex] = new Array[InvertedIndex](oneParSnapNum / indexSnapInterval + 1)
        val lonGridNum : Int = myBaseSettings.lonGridNum
        val latGridNum : Int = myBaseSettings.latGridNum
        for (i <- 0 until invertedIndex.length){
            var oneIndex : InvertedIndex = new InvertedIndex(myBaseSettings.totalTrajNums, lonGridNum * latGridNum)
            for (j <- 0 until (lonGridNum * latGridNum)){
                var pos : Int = i
                var temArray : ArrayBuffer[Int] = ArrayBuffer[Int]()
                while(pos < sortArray.length){
                    val temIndex : InvertedIndex = sortArray(pos)._2
                    val beginPos : Int = temIndex.index(j * 2)
                    val temLength : Int = temIndex.index(j * 2 + 1)
                    for (k <- 0 until temLength){
                        temArray += temIndex.idArray(beginPos + k)
                    }
                    pos += invertedIndex.length
                }
                if (j == 0){
                    oneIndex.index(0) = 0
                    oneIndex.index(1) = temArray.length
                    oneIndex.indexPos = 2
                    for (k <- 0 until temArray.length){
                        oneIndex.idArray(k) = temArray(k)
                    }
                    oneIndex.idArrayPos = temArray.length
                }else{
                    var temPos = oneIndex.indexPos
                    oneIndex.index(temPos) = oneIndex.index(temPos - 2) + oneIndex.index(temPos - 1)
                    oneIndex.index(temPos + 1) = temArray.length
                    oneIndex.indexPos += 2
                    temPos = oneIndex.idArrayPos
                    for (k <- 0 until temArray.length){
                        oneIndex.idArray(temPos) = temArray(k)
                        temPos += 1
                    }
                    oneIndex.idArrayPos = temPos
                }
            }
            
            
            
            invertedIndex(i) = oneIndex
        }
        Array(invertedIndex).iterator
    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSetting, indexRDD : RDD[Array[InvertedIndex]], 
                        lookupTable : Array[Int]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList
        patIDList.foreach{patID => {
            val patPath : String = myBaseSettings.rootPath + "query/trajectory"  + patID.toString + ".tstjs"
            val patCoorList : Array[(Float, Float)] = sc.binaryRecords(patPath, 14)
                                                        .map(l => (ByteBuffer.wrap(l.slice(6, 10)).getFloat, 
                                                                    ByteBuffer.wrap(l.slice(10, 14)).getFloat))
                                                        .collect()

            val bcPatCoor : Broadcast[Array[(Float, Float)]] = sc.broadcast(patCoorList)
            val bcLookupTable : Broadcast[Array[Int]] = sc.broadcast(lookupTable)

            val time1 : Long = System.currentTimeMillis
            val candiList : Array[Array[Int]] = indexRDD.mapPartitions(l => mapSearchWithIndex(l, myBaseSettings, bcPatCoor))
                                                        .glom()
                                                        .collect()
            val time2 : Long = System.currentTimeMillis
            println("----------------------------------------------------------")
            println("Query time on the index: " + ((time2 - time1) / 1000.0).toDouble)
            println("The length of candiList is: " + candiList.length)
            // for (i <- 0 until candiList.length){
            //     if (i == 0){
            //         for (j <- 0 until candiList(i).length){
            //             print(candiList(i)(j) + "\t")
            //         }
            //     }
            //     println("===")
            // }
            // println("----------------------------------------------------------")

            var mapOfParToTraj : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            val inputFilePath : String = getInputFilePath(myBaseSettings, candiList, patID, lookupTable, mapOfParToTraj)
            
            val time3 : Long = System.currentTimeMillis
            // val refineRDD : RDD[Array[Byte]] = sc.binaryRecords(inputFilePath, myBaseSettings.recordLength)
            val refineResult : Array[Array[Int]] = sc.binaryFiles(inputFilePath)
                                                     .map(_._2.toArray)
                                                     .map(unGzip)
                                                     .zipWithIndex()
                                                     .map(l => mapSearchWithRefine(l, myBaseSettings, mapOfParToTraj, bcPatCoor))
                                                     .collect()
            val time4 : Long = System.currentTimeMillis
            println("----------------------------------------------------------")
            println("Query time on the refine: " + ((time4 - time3) / 1000.0).toDouble)
            println("The length of inputFilePath is: " + inputFilePath.split(",").length)
            println("The length of refineResult is: " + refineResult.length)
            // println("The map is : ")
            // for (i <- 0 until inputFilePath.split(",").length){
            //     if (i < 3){
            //         var temMap = mapOfParToTraj.get(i).get
            //         for (j <- 0 until temMap.length){
            //             print(temMap(j) + "\t")
            //         }
            //         println("---")
            //     }
                
            // }
            println("The refineResult is: ")
            // for (i <- 0 until refineResult.length){
            //     for (j <- 0 until refineResult(i).length){
            //         print(refineResult(i)(j))
            //         print("\t")
            //     }
            //     println("===")
            // }
            // println("----------------------------------------------------------")

            val time5 : Long = System.currentTimeMillis
            val finalResult : Array[Int] = getFinalResult(refineResult, myBaseSettings, lookupTable)
            val time6 : Long = System.currentTimeMillis
            println("----------------------------------------------------------")
            println("The Time of get the final result is: " + ((time6 - time5) / 1000.0).toDouble)
            println("The number of close contacts of the " + patID + "th patient is: " + finalResult.length)
            for (i <- 0 until finalResult.length){
                print(finalResult(i) + "\t")
            }
            println("\n----------------------------------------------------------")
        }
        }
    }

    def mapSearchWithIndex(iter : Iterator[Array[InvertedIndex]], myBaseSettings : BaseSetting, 
                            bcPatCoor : Broadcast[Array[(Float, Float)]]) : Iterator[Int] = {
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        
        val indexSnapInterval : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum     : Int = myBaseSettings.oneParSnapNum
        val lonGridNum        : Int = myBaseSettings.lonGridNum
        val latGridNum        : Int = myBaseSettings.latGridNum
        val MINLON            : Float = myBaseSettings.MINLON
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
            val myInvertedIndex : InvertedIndex = invertedIndex((currSnap - startSnap) / indexSnapInterval)
            val minLon : Float = (patCoorList(currSnap)._1 - lonDiff).toFloat
            val maxLon : Float = (patCoorList(currSnap)._1 + lonDiff).toFloat
            val minLat : Float = (patCoorList(currSnap)._2 - latDiff).toFloat
            val maxLat : Float = (patCoorList(currSnap)._2 + latDiff).toFloat
            var minLonGridID : Int = floor((minLon - MINLON) / lonGridLength).toInt
            var maxLonGridID : Int = floor((maxLon - MINLON) / lonGridLength).toInt
            var minLatGridID : Int = floor((minLat - MINLAT) / latGridLength).toInt
            var maxLatGridID : Int = floor((maxLat - MINLAT) / latGridLength).toInt
            if (minLonGridID < 0){
                minLonGridID = 0
            }
            if (maxLonGridID >= lonGridNum){
                maxLonGridID = lonGridNum - 1
            }
            if (minLatGridID < 0){
                minLatGridID = 0
            }
            if (maxLatGridID >= latGridNum){
                maxLatGridID = latGridNum - 1
            }
            var temList : ArrayBuffer[Int] = ArrayBuffer[Int]()
            // println("?")
            // println(currSnap + "\t" + patCoorList(currSnap)._1 + "\t" + patCoorList(currSnap)._2 + "\t" + minLonGridID + "\t" + maxLonGridID + "\t" + minLatGridID + "\t" + maxLatGridID + "\t" + (currSnap - startSnap) / indexSnapInterval + "\t" + partitionID)
            
            for (lonGridID <- minLonGridID to maxLonGridID){
                for (latGridID <- minLatGridID to maxLatGridID){
                    val gridID   : Int = latGridID * lonGridNum + lonGridID
                    val beginPos : Int = myInvertedIndex.index(gridID * 2)
                    val length   : Int = myInvertedIndex.index(gridID * 2 + 1);
                    for (i <- 0 until length){
                        temList += myInvertedIndex.idArray(beginPos + i)
                        
                    }
                }
            }
            // temList += 1
            temMap += currSnap -> temList
            currSnap += indexSnapInterval
        }
        currSnap = startSnap
        resultList ++= temMap.get(currSnap).get
        val stepNum : Int = floor(contiSnap.toDouble / indexSnapInterval).toInt
        while((currSnap + (stepNum - 1) * indexSnapInterval) < stopSnap){
            var temList : ArrayBuffer[Int] = temMap.get(currSnap).get
            for (i <- 0 until stepNum){
                var firList : ArrayBuffer[Int] = temMap.get(currSnap + i * indexSnapInterval).get
                temList = temList intersect firList
                
            }
            resultList ++= temList
            currSnap += indexSnapInterval
        }

        currSnap = stopSnap - 1
        val myInvertedIndex : InvertedIndex = invertedIndex((currSnap - startSnap) / indexSnapInterval)
        val minLon : Float = (patCoorList(currSnap)._1 - lonDiff).toFloat
        val maxLon : Float = (patCoorList(currSnap)._1 + lonDiff).toFloat
        val minLat : Float = (patCoorList(currSnap)._2 - latDiff).toFloat
        val maxLat : Float = (patCoorList(currSnap)._2 + latDiff).toFloat
        var minLonGridID : Int = floor((minLon - MINLON) / lonGridLength).toInt
        var maxLonGridID : Int = floor((maxLon - MINLON) / lonGridLength).toInt
        var minLatGridID : Int = floor((minLat - MINLAT) / latGridLength).toInt
        var maxLatGridID : Int = floor((maxLat - MINLAT) / latGridLength).toInt
        if (minLonGridID < 0){
            minLonGridID = 0
        }
        if (maxLonGridID >= lonGridNum){
            maxLonGridID = lonGridNum - 1
        }
        if (minLatGridID < 0){
            minLatGridID = 0
        }
        if (maxLatGridID >= latGridNum){
            maxLatGridID = latGridNum - 1
        }
        var temList : ArrayBuffer[Int] = ArrayBuffer[Int]()
        for (lonGridID <- minLonGridID to maxLonGridID){
            for (latGridID <- minLatGridID to maxLatGridID){
                val gridID   : Int = latGridID * lonGridNum + lonGridID
                val beginPos : Int = myInvertedIndex.index(gridID * 2)
                val length   : Int = myInvertedIndex.index(gridID * 2 + 1);
                for (i <- 0 to length - 1){
                    temList += myInvertedIndex.idArray(beginPos + i)
                    // println(lonGridID + "\t" + latGridID + "\t" + gridID + "\t" + myInvertedIndex.idArray(beginPos + i))
                }
            }
        }
        // temList += 1
        resultList ++= temList

        resultList.toSet.iterator
    }

    def getInputFilePath(myBaseSettings : BaseSetting, candiList : Array[Array[Int]], patID : Int, 
                            lookupTable : Array[Int], mapOfParToTraj : Map[Int, ArrayBuffer[Int]]) : String = {
        var finalPath : ArrayBuffer[String] = ArrayBuffer[String]()
        val pathPrefix         : String = myBaseSettings.rootPath
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        var totalPos : Int = 0
        for (i <- 0 until candiList.length){
            val timeID : Int = i
            var spaceSet : Set[Int] = Set[Int]()
            var spaceMap : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
            for (j <- 0 until candiList(i).length){
                val trajID : Int = candiList(i)(j)
                if (trajID != patID){
                    val spaceID : Int = lookupTable(i * myBaseSettings.totalTrajNums * 3 + trajID * 3)
                    if (spaceSet.contains(spaceID)){
                        var tem : ArrayBuffer[Int] = spaceMap.get(spaceID).get
                        tem += trajID
                        spaceMap += spaceID -> tem
                    }else{
                        spaceSet.add(spaceID)
                        var tem : ArrayBuffer[Int] = ArrayBuffer[Int]()
                        tem += trajID
                        spaceMap += spaceID -> tem
                    }
                }
            }
            var spaceArray : Array[Int] = spaceSet.toArray
            for (j <- 0 until spaceArray.length){
                val spaceID : Int = spaceArray(j)
                val name : String = myBaseSettings.rootPath.concat("par").concat(timeID.toString).concat("zorder").concat(spaceID.toString).concat(".tstjs.gz")
                finalPath += name
                var temArrayBuffer : ArrayBuffer[Int] = spaceMap.get(spaceID).get
                var afterChangeBuffer : ArrayBuffer[Int] = ArrayBuffer[Int]()
                afterChangeBuffer += timeID
                afterChangeBuffer += spaceID
                for (k <- 0 until temArrayBuffer.length){
                    val myTrajID : Int = temArrayBuffer(k)
                    val myOrderPos : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + myTrajID * 3 + 1)
                    afterChangeBuffer += myOrderPos
                }
                mapOfParToTraj += totalPos -> afterChangeBuffer
                totalPos += 1
            }
        }
        println("The length of finalPath is: " + finalPath.length)
        finalPath.mkString(",")
    }

    def mapSearchWithRefine(iter : (Array[Byte], Long), myBaseSettings : BaseSetting, mapOfParToTraj : Map[Int, ArrayBuffer[Int]], 
                            bcPatCoor : Broadcast[Array[(Float, Float)]]) : Array[Int] = {
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int = myBaseSettings.contiSnap
        val totalSnap          : Int = myBaseSettings.totalSnap
        val timeInterval       : Int = myBaseSettings.timeInterval
        val delta              : Double = myBaseSettings.delta

        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum

        val partitionID : Int = iter._2.toInt
        var candiList : ArrayBuffer[Int] = mapOfParToTraj.get(partitionID).get
        val timeID : Int = candiList(0)
        val spaceID : Int = candiList(1)
        candiList = candiList.slice(2, candiList.length)
        

        val patCoorList : Array[(Float, Float)] = bcPatCoor.value
        val startSnap : Int = timeID * oneParSnapNum
        val lonDiff : Float = abs(delta / 111111 / math.cos(patCoorList(startSnap)._2)).toFloat
        val latDiff : Float = (delta/111111).toFloat

        var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var rearResult : ArrayBuffer[Int] = ArrayBuffer[Int]()

        var testArray : Array[Byte] = iter._1
        for (canPar <- candiList){
            // if (timeID == 0){
            //     println(timeID + "\t" + spaceID + "\t" + canPar)
            // }
            val beginPos : Int = oneParSnapNum * canPar * 8
            var nowPos : Int = beginPos
            var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
            var flag : Int = 0
            var temConti : Int = 0
            var maxConti : Int = 0
            var pos : Int = 0
            while(pos < oneParSnapNum){
                val testLon : Float = ByteBuffer.wrap(testArray.slice(nowPos, nowPos + 4)).getFloat
                val testLat : Float = ByteBuffer.wrap(testArray.slice(nowPos + 4, nowPos + 8)).getFloat
                nowPos += 8
                val lon : Float = patCoorList(startSnap + pos)._1
                val lat : Float = patCoorList(startSnap + pos)._2
                val minLon : Float = lon - lonDiff
                val maxLon : Float = lon + lonDiff
                val minLat : Float = lat - latDiff
                val maxLat : Float = lat + latDiff
                if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, testLon, testLat)){
                    flagArray(pos) = 1
                    if (flag == 0){
                        temConti = 1
                        flag = 1
                    }else{
                        temConti += 1
                    }
                }else{
                    flagArray(pos) = 0
                    if (flag != 0){
                        flag = 0
                        if (maxConti < temConti){
                            maxConti = temConti
                        }
                    }
                }
                pos += 1
            }
            if (maxConti < temConti){
                maxConti = temConti
            }
            if (maxConti >= contiSnap){
                finalResult += canPar
            }else{
                if (flagArray(0) == 1){
                    var count : Int = 0
                    var loop = new Breaks
                    loop.breakable{
                    for (i <- 0 until oneParSnapNum){
                        if (flagArray(i) == 1){
                            count += 1
                        }else{
                            loop.break()
                        }
                    }
                    }
                    frontResult += canPar
                    frontResult += count
                }
                if (flagArray(oneParSnapNum - 1) == 1){
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
                    rearResult += canPar
                    rearResult += count
                }
            }
        }

        // var totalPos : Int = 0
        // var subPos : Int = 0
        // while(subPos < iter._1.length){
        //     var record : Array[Byte] = iter._1(subPos)
        //     subPos += 1
        //     if (totalPos % oneParSnapNum == 0){
        //         val parSeq = totalPos / oneParSnapNum
        //         if (candiList.contains(parSeq)){
        //             var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
        //             var flag : Int = 0
        //             var temConti : Int = 0
        //             var maxConti : Int = 0
        //             var pos : Int = 0
        //             while(pos < oneParSnapNum){
        //                 val testLon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
        //                 val testLat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
        //                 val lon : Float = patCoorList(startSnap + pos)._1
        //                 val lat : Float = patCoorList(startSnap + pos)._2
        //                 val minLon : Float = lon - lonDiff
        //                 val maxLon : Float = lon + lonDiff
        //                 val minLat : Float = lat - latDiff
        //                 val maxLat : Float = lat + latDiff
        //                 if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, testLon, testLat)){
        //                     flagArray(pos) = 1
        //                     if (flag == 0){
        //                         temConti = 1
        //                         flag = 1
        //                     }else{
        //                         temConti += 1
        //                     }
        //                 }else{
        //                     flagArray(pos) = 0
        //                     if (flag != 0){
        //                         flag = 0
        //                         if (maxConti < temConti){
        //                             maxConti = temConti
        //                         }
        //                     }
        //                 }
        //                 if (subPos < iter._1.length){
        //                     record = iter._1(subPos)
        //                     subPos += 1
        //                 }
        //                 pos += 1
        //                 totalPos += 1
        //             }
        //             if (maxConti < temConti){
        //                 maxConti = temConti
        //             }
        //             if (maxConti >= contiSnap){
        //                 finalResult += parSeq
        //             }else{
        //                 if (flagArray(0) == 1){
        //                     var count : Int = 0
        //                     var loop = new Breaks
        //                     loop.breakable{
        //                     for (i <- 0 to oneParSnapNum - 1){
        //                         if (flagArray(i) == 1){
        //                             count += 1
        //                         }else{
        //                             loop.break()
        //                         }
        //                     }
        //                     }
        //                     frontResult += parSeq
        //                     frontResult += count
        //                 }
        //                 if (flagArray(oneParSnapNum - 1) == 1){
        //                     var count : Int = 0
        //                     var loop = new Breaks
        //                     loop.breakable{
        //                     for (i <- oneParSnapNum - 1 to 0 by -1){
        //                         if (flagArray(i) == 1){
        //                             count += 1
        //                         }else{
        //                             loop.break()
        //                         }
        //                     }
        //                     }
        //                     rearResult += parSeq
        //                     rearResult += count
        //                 }
        //             }
        //         }
        //     }
        //     totalPos += 1
        // }
        var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
        result += timeID
        result += spaceID
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
        result.toArray
    }

    def mapSearchWithRefine2(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, mapOfParToTraj : Map[Int, ArrayBuffer[Int]], 
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

        val patCoorList : Array[(Float, Float)] = bcPatCoor.value
        val startSnap : Int = timeID * oneParSnapNum
        val lonDiff : Float = abs(delta / 111111 / math.cos(patCoorList(startSnap)._2)).toFloat
        val latDiff : Float = (delta/111111).toFloat

        var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var rearResult : ArrayBuffer[Int] = ArrayBuffer[Int]()

        val testArray : Array[Array[Byte]] = iter.toArray
        for (trajID <- candiList){
            val firPos : Int = timeID * myBaseSettings.totalTrajNums * 3 + trajID * 3 + 1
            val thisOrder : Int = lookupTable(firPos)
            val beginPos : Int = oneParSnapNum * thisOrder
            var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
            var flag : Int = 0
            var temConti : Int = 0
            var maxConti : Int = 0
            var pos : Int = 0
            while(pos < contiSnap){
                val testLon : Float = ByteBuffer.wrap(testArray(beginPos + pos).slice(0, 4)).getFloat
                val testLat : Float = ByteBuffer.wrap(testArray(beginPos + pos).slice(4, 8)).getFloat
                val lon : Float = patCoorList(startSnap + pos)._1
                val lat : Float = patCoorList(startSnap + pos)._2
                val minLon : Float = lon - lonDiff
                val maxLon : Float = lon + lonDiff
                val minLat : Float = lat - latDiff
                val maxLat : Float = lat + latDiff
                if (PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat, maxLat, testLon, testLat)){
                    flagArray(pos) = 1
                    if (flag == 0){
                        temConti = 1
                        flag = 1
                    }else{
                        temConti += 1
                    }
                }else{
                    flagArray(pos) = 0
                    if (flag != 0){
                        flag = 0
                        if (maxConti < temConti){
                            // println("#")
                            maxConti = temConti
                        }
                    }
                }
                if (maxConti < temConti){
                    println("#")
                    maxConti = temConti
                }
                if (maxConti >= contiSnap){
                    finalResult += trajID
                }else{
                    println("!")
                    println(maxConti)
                    if (flagArray(0) == 1){
                        println("@")
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
                    if (flagArray(oneParSnapNum - 1) == 1){
                        println("-")
                        var count : Int = 0
                        var loop = new Breaks
                        loop.breakable{
                        for (i <- oneParSnapNum - 1 to 0 by -1){
                            if (flagArray(i) == 1){
                                println("/")
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
                pos += 1
            }
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

    def mapSearchWithRefine3(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, mapOfParToTraj : Map[Int, ArrayBuffer[Int]], 
                            bcPatCoor : Broadcast[Array[(Float, Float)]]) : Iterator[Int] = {
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int = myBaseSettings.contiSnap
        val totalSnap          : Int = myBaseSettings.totalSnap
        val timeInterval       : Int = myBaseSettings.timeInterval
        val delta              : Double = myBaseSettings.delta

        val oneParSnapNum : Int = myBaseSettings.oneParSnapNum

        // val lookupTable : Array[Int] = bcLookupTable.value
        // val lookupTable : Array[Int] = bcLookupTable
        
        val partitionID  : Int = TaskContext.get.partitionId
        //val partitionID : Int = 1
        var candiList : ArrayBuffer[Int] = mapOfParToTraj.get(partitionID).get

        // println("-----------")
        // for (i <- 0 until candiList.length){
        //     print(candiList(i))
        //     print("\t")
        // }
        // println("-----------")

        val timeID : Int = candiList(0)
        val spaceID : Int = candiList(1)
        candiList = candiList.slice(2, candiList.length)

        val patCoorList : Array[(Float, Float)] = bcPatCoor.value
        val startSnap : Int = timeID * oneParSnapNum
        val lonDiff : Float = abs(delta / 111111 / math.cos(patCoorList(startSnap)._2)).toFloat
        val latDiff : Float = (delta/111111).toFloat

        var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var rearResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        while(iter.hasNext){
            var record : Array[Byte] = iter.next()
        }
        Array(0, 0, 0, 0, 0).iterator
    }

    def mapSearchWithRefine4(iter : Iterator[Array[Byte]], myBaseSettings : BaseSetting, mapOfParToTraj : Map[Int, ArrayBuffer[Int]], 
                            bcPatCoor : Broadcast[Array[(Float, Float)]], bcLookupTable : Broadcast[Array[Int]]) : Iterator[Int] = {
        while(iter.hasNext){
            var record : Array[Byte] = iter.next()
        }
        Array(0, 0, 0).iterator
    }

    def getFinalResult(inputArray : Array[Array[Int]], myBaseSettings : BaseSetting, lookupTable : Array[Int]) : Array[Int] = {
        var finalResult : Set[Int] = Set[Int]()
        var temMap : Map[Int, Int] = Map[Int, Int]()
        for (i <- 0 until inputArray.length){
            var temArray : Array[Int] = inputArray(i)
            val timeID : Int = temArray(0)
            val spaceID : Int = temArray(1)
            temArray = temArray.slice(2, temArray.length)
            if (temArray.length != 0){
                var pos : Int = 0
                var length : Int = 0
                if (i == 0){
                    pos = 0
                    length = temArray(0)
                    var myArray : Array[Int] = temArray.slice(pos + 1, pos + length + 1)
                    for (j <- 0 until myArray.length){
                        val trajPos : Int = myArray(j)
                        val canTrajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                        if (!finalResult.contains(canTrajID)){
                            finalResult.add(myArray(j))
                        }
                    }
                    pos = pos + length + 1
                    length = temArray(pos)
                    pos = pos + length + 1
                    length = temArray(pos)
                    myArray = temArray.slice(pos + 1, pos + length + 1)
                    temMap.clear()
                    for (j <- 0 until myArray.length / 2){
                        val trajPos : Int = myArray(j * 2)
                        val canTrajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                        val snapNum : Int = myArray(j * 2 + 1)
                        temMap += canTrajID -> snapNum
                    }
                }else{
                    pos = 0
                    length = temArray(0)
                    var myArray : Array[Int] = temArray.slice(pos + 1, pos + length + 1)
                    for (j <- 0 until myArray.length){
                        val trajPos : Int = myArray(j)
                        val canTrajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                        if (!finalResult.contains(canTrajID)){
                            finalResult.add(myArray(j))
                        }
                    }
                    pos = pos + length + 1
                    length = temArray(pos)
                    myArray = temArray.slice(pos + 1, pos + length + 1)
                    for (j <- 0 until myArray.length / 2){
                        val trajPos : Int = myArray(j * 2)
                        val canTrajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                        val snapNum : Int = myArray(j * 2 + 1)
                        if (!finalResult.contains(canTrajID)){
                            if (temMap.contains(canTrajID)){
                                val lastSnapNum : Int = temMap.get(canTrajID).get
                                if (lastSnapNum + snapNum > myBaseSettings.contiSnap){
                                    finalResult.add(canTrajID)
                                }
                            }
                        }
                    }
                    pos = pos + length + 1
                    length = temArray(pos)
                    myArray = temArray.slice(pos + 1, pos + length + 1)
                    temMap.clear()
                    for (j <- 0 until myArray.length / 2){
                        val trajPos : Int = myArray(j * 2)
                        val canTrajID : Int = lookupTable(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                        val snapNum : Int = myArray(j * 2 + 1)
                        temMap += canTrajID -> snapNum
                    }
                }
            }
        }
        finalResult.toArray
    }
}