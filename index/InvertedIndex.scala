package src.main.scala.index

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class InvertedIndex extends Serializable{
    var idArray    : Array[Int] = null
    var index      : Array[Int] = null
    var idArrayPos : Int = -1
    var indexPos   : Int = -1

    def this(totalTrajNum : Int, gridNum : Int) = {
        this()
        this.idArray    = new Array[Int](totalTrajNum)
        this.index      = new Array[Int](gridNum * 2)
        this.idArrayPos = 0
        this.indexPos   = 0
    }

}