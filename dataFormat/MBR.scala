package src.main.scala.dataFormat

class MBR extends Serializable{

    var minLon : Float = _
    var maxLon : Float = _
    var minLat : Float = _
    var maxLat : Float = _

    def this(minLon : Double, maxLon : Double, minLat : Double, maxLat : Double) = {
        this()
        this.minLon = minLon
        this.maxLon = maxLon
        this.minLat = minLat
        this.maxLat = maxLat
    }

    def getArea() : Double = {
        1.0 * (this.maxLon - this.minLon) * (this.maxLat - this.minLat)
    }
}