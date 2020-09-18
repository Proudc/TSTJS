package src.main.scala.dataFormat

class MBB extends Serializable{
    var minOffset : Short = _
    var maxOffset : Short = _
    var minLon    : Float = _
    var maxLon    : Float = _
    var minLat    : Float = _
    var maxLat    : Float = _

    def this(minOffset : Short, maxOffset : Short, minLon : Float, maxLon : Float, minLat : Float, maxLat : Float) = {
        this()
        this.minOffset = minOffset
        this.maxOffset = maxOffset
        this.minLon    = minLon
        this.maxLon    = maxLon
        this.minLat    = minLat
        this.maxLat    = maxLat
    }
    
    def getVolume() : Double = {
        1.0 * (this.maxOffset - this.minOffset) * (this.maxLon - this.minLon) * (this.maxLat - this.minLat)
    }

    def getLongOffset() : Int = {
        this.maxOffset - this.minOffset
    }

    def getWidthLon() : Float = {
        this.maxLon - this.minLon
    }

    def getHeightLat() : Float = {
        this.maxLat - this.minLat
    }

    def getCenOffset() : Float = {
        val cenOffset : Float = ((this.maxOffset + this.minOffset) / 2.0).toFloat
        cenOffset
    }

    def getCenLon() : Float = {
        (this.maxLon + this.minLon) / 2
    }

    def getCenLat() : Float = {
        (this.maxLat + this.minLat) / 2
    }

}