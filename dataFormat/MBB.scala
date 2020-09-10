package src.main.scala.dataFormat

class MBB extends Serializable{

    var minOffset : Int = _
    var maxOffset : Int = _
    var minLon : Double = _
    var maxLon : Double = _
    var minLat : Double = _
    var maxLat : Double = _

    var volume : Double = _

    var longOffset : Int = _
    var widthLon : Double = _
    var heightLat : Double = _
    var cenOffset : Double = _
    var cenLon : Double = _
    var cenLat : Double = _

    def this(minOffset : Int, maxOffset : Int, minLon : Double, maxLon : Double, minLat : Double, maxLat : Double) = {
        this()
        this.minOffset = minOffset
        this.maxOffset = maxOffset
        this.minLon = minLon
        this.maxLon = maxLon
        this.minLat = minLat
        this.maxLat = maxLat
        setVolume()
        setCenData()
    }

    def setVolume() : Unit = {
        this.volume = 1.0 * (this.maxOffset - this.minOffset) * (this.maxLon - this.minLon) * (this.maxLat - this.minLat)
    }

    def setCenData() : Unit = {
        this.longOffset = this.maxOffset - this.minOffset
        this.widthLon   = this.maxLon - this.minLon
        this.heightLat  = this.maxLat - this.minLat
        this.cenOffset  = (this.maxOffset + this.minOffset) / 2
        this.cenLon     = (this.maxLon + this.minLon) / 2
        this.cenLat     = (this.maxLat + this.minLat) / 2
    }

}