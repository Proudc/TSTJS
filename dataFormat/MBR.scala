package src.main.scala.dataFormat

class MBR extends Serializable{

    var minLon : Double = _
    var maxLon : Double = _
    var minLat : Double = _
    var maxLat : Double = _

    var area : Double = _

    var widthLon : Double = _
    var heightLat : Double = _
    var cenLon : Double = _
    var cenLat : Double = _

    def this(minLon : Double, maxLon : Double, minLat : Double, maxLat : Double) = {
        this()
        this.minLon = minLon
        this.maxLon = maxLon
        this.minLat = minLat
        this.maxLat = maxLat
        setArea()
        setCenData()
    }

    def setArea() : Unit = {
        this.area = 1.0 * (this.maxLon - this.minLon) * (this.maxLat - this.minLat)
    }

    def setCenData() : Unit = {
        this.widthLon  = this.maxLon - this.minLon
        this.heightLat = this.maxLat - this.minLat
        this.cenLon    = (this.maxLon + this.minLon) / 2
        this.cenLat    = (this.maxLat + this.minLat) / 2
    }

}