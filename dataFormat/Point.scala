package src.main.scala.dataFormat


class Point extends Serializable{
    var id     : Int = _
    var offset : Short = _
    var lon    : Float = _
    var lat    : Float = _

    def this(id : Int, offset : Short, lon : Float, lat : Float) = {
        this()
        this.id     = id
        this.offset = offset
        this.lon    = lon
        this.lat    = lat
    }

}