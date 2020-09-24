package src.main.scala.index

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class InvertedIndex{
    var idArray    : Array[Int] = null
    var index      : Array[Int] = null
    var idArrayPos : Int = -1
    var indexPos   : Int = -1

    def this(totalTrajNum : Int, gridNum : Int) = {
        this()
        this.idArray    = Array[Int](totalTrajNum)
        this.index      = Array[Int](gridNum)
        this.idArrayPos = 0
        this.indexPos   = 0
    }

}