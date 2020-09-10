package src.main.scala.dataFormat

/**
* Data format of raw data
*/
class Record extends Serializable{

    var id   : Int    = _
    var date : String = _
    var time : String = _
    var lon  : Double = _
    var lat  : Double = _

    def this(id : Int, date : String, time : String, lon : Double, lat : Double){
        this()
        this.id   = id
        this.date = date
        this.time = time
        this.lon  = lon
        this.lat  = lat
    }
}