package src.main.scala.dataFormat

/**
* Data format of raw data
*/
class RecordWithSnap extends Serializable{

    var id      : Int    = _
    var currSec : Int    = _
    var lon     : Double = _
    var lat     : Double = _

    def this(id : Int, currSec : Int, lon : Double, lat : Double){
        this()
        this.id      = id
        this.currSec = currSec
        this.lon     = lon
        this.lat     = lat
    }
}