package src.main.scala.index

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

import src.main.scala.dataFormat.MBB
import src.main.scala.dataFormat.Point

class RTree{
    var leavesRtree : ArrayBuffer[RTree] = null
    var leavesPoint : ArrayBuffer[Point] = null
    var leavesFlag  : Int = -1
    var myMBB       : MBB = null
    var level       : Int = -1
    var minCap      : Int = -1
    var maxCap      : Int = -1
    var father      : RTree = null

    def this(myMBB : MBB, level : Int, minCap : Int, maxCap : Int, father : RTree, leavesFlag : Int) = {
        this()
        if (leavesFlag == 0){
            this.leavesRtree = ArrayBuffer[RTree]()
            this.leavesPoint = null
        }else{
            this.leavesPoint = ArrayBuffer[Point]()
            this.leavesRtree = null
        }
        this.myMBB  = myMBB
        this.level  = level
        this.minCap = minCap
        this.maxCap = maxCap
        this.father = father
    }

    def chooseLeaf(node : Point) : RTree = {
        if (this.level == 1){
            this
        }else{
            var minValue : Double = Double.MaxValue
            var pos : Int = -1
            for (i <- 0 to (this.leavesRtree.size - 1)){
                val volumeInc : Double = RTree.volumeIncrease(this.leavesRtree(i).myMBB, node)
                if (minValue > volumeInc){
                    minValue = volumeInc
                    pos = i
                }
            }
            this.leavesRtree(pos).chooseLeaf(node)
        }
    }

    def adjustTree() : Unit = {
        var p : RTree = null
        while(p != null){
            if (p.leavesFlag == 1){
                if (p.leavesPoint.size > p.maxCap){
                    p.splitNode()
                }else{
                    if (p.father != null){
                        p.father.myMBB = RTree.merge(p.father.myMBB, p.myMBB)
                    }
                }
                p = p.father
            }else{
                if (p.leavesRtree.size > p.maxCap){
                    p.splitNode()
                }else{
                    if (p.father != null){
                        p.father.myMBB = RTree.merge(p.father.myMBB, p.myMBB)
                    }
                }
                p = p.father
            }
        }
    }

    def splitNode() : Unit = {
        if (this.leavesFlag == 0){
            this.rtreeSplitNode()
        }else{
            this.pointSplitNode()
        }
    }

    def pointSplitNode() : Unit = {
        if (this.father == null){
            this.father = new RTree(null, (this.level + 1), this.minCap, this.maxCap, null, 0)
            this.leavesRtree += this
        }
        var leaf1 : RTree = new RTree(null, (this.level), this.minCap, this.maxCap, null, 1)
        var leaf2 : RTree = new RTree(null, (this.level), this.minCap, this.maxCap, null, 1)
        var loop = new Breaks
        this.pointPickSeeds(leaf1, leaf2)
        loop.breakable{
        while(this.leavesPoint.size > 0){
            if ((leaf1.leavesPoint.size > leaf2.leavesPoint.size) && (leaf2.leavesPoint.size + this.leavesPoint.size) == this.minCap){
                for (i <- 0 to (this.leavesPoint.size - 1)){
                    var leaf : Point = this.leavesPoint(i)
                    leaf2.myMBB = RTree.merge(leaf2.myMBB, leaf)
                    leaf2.leavesPoint += leaf
                }
                this.leavesPoint.clear()
                loop.break()
            }
            if ((leaf2.leavesPoint.size > leaf1.leavesPoint.size) && (leaf1.leavesPoint.size + this.leavesPoint.size) == this.minCap){
                for (i <- 0 to (this.leavesPoint.size - 1)){
                    var leaf : Point = this.leavesPoint(i)
                    leaf1.myMBB = RTree.merge(leaf1.myMBB, leaf)
                    leaf1.leavesPoint += leaf
                }
                this.leavesPoint.clear()
                loop.break()
            }
            this.pointPickNext(leaf1, leaf2)
        }
        }
        this.father.leavesRtree.remove(this.father.leavesRtree.indexOf(this))
        leaf1.father = this.father
        leaf2.father = this.father
        this.father.leavesRtree += leaf1
        this.father.leavesRtree += leaf2
        this.father.myMBB = RTree.merge(this.father.myMBB, leaf1.myMBB)
        this.father.myMBB = RTree.merge(this.father.myMBB, leaf2.myMBB)
    }

    def rtreeSplitNode() : Unit = {
        if (this.father == null){
            this.father = new RTree(null, (this.level + 1), this.minCap, this.maxCap, null, 0)
            this.leavesRtree += this
        }
        var leaf1 : RTree = new RTree(null, (this.level), this.minCap, this.maxCap, null, 0)
        var leaf2 : RTree = new RTree(null, (this.level), this.minCap, this.maxCap, null, 0)
        var loop = new Breaks
        this.rtreePickSeeds(leaf1, leaf2)
        loop.breakable{
        while(this.leavesRtree.size > 0){
            if ((leaf1.leavesRtree.size > leaf2.leavesRtree.size) && (leaf2.leavesRtree.size + this.leavesRtree.size) == this.minCap){
                for (i <- 0 to (this.leavesRtree.size - 1)){
                    var leaf : RTree = this.leavesRtree(i)
                    leaf2.myMBB = RTree.merge(leaf2.myMBB, leaf.myMBB)
                    leaf2.leavesRtree += leaf
                    leaf.father = leaf2
                }
                this.leavesRtree.clear()
                loop.break()
            }
            if ((leaf2.leavesRtree.size > leaf1.leavesRtree.size) && (leaf1.leavesRtree.size + this.leavesRtree.size) == this.minCap){
                for (i <- 0 to (this.leavesRtree.size - 1)){
                    var leaf : RTree = this.leavesRtree(i)
                    leaf1.myMBB = RTree.merge(leaf1.myMBB, leaf.myMBB)
                    leaf1.leavesRtree += leaf
                    leaf.father = leaf1
                }
                this.leavesRtree.clear()
                loop.break()
            }
            this.rtreePickNext(leaf1, leaf2)
        }
        }
        this.father.leavesRtree.remove(this.father.leavesRtree.indexOf(this))
        leaf1.father = this.father
        leaf2.father = this.father
        this.father.leavesRtree += leaf1
        this.father.leavesRtree += leaf2
        this.father.myMBB = RTree.merge(this.father.myMBB, leaf1.myMBB)
        this.father.myMBB = RTree.merge(this.father.myMBB, leaf2.myMBB)
    }

    def pointPickSeeds(leaf1 : RTree, leaf2 : RTree) : Unit = {
        var d : Double = 0.0
        var t1 : Int = 0
        var t2 : Int = 0
        for (i <- 0 to (this.leavesPoint.size - 1)){
            for (j <- (i + 1) to (this.leavesPoint.size - 1)){
                var MBBNew : MBB = RTree.merge(this.leavesPoint(i), this.leavesPoint(j))
                var sNew : Double = MBBNew.getVolume()
                if ((sNew) > d){
                    t1 = i
                    t2 = j
                    d = sNew
                }
            }
        }
        var n2 : Point = this.leavesPoint.remove(t2)
        leaf1.leavesPoint += n2
        leaf1.myMBB = new MBB(n2.offset, n2.offset, n2.lon, n2.lon, n2.lat, n2.lat)
        var n1 : Point = this.leavesPoint.remove(t1)
        leaf2.leavesPoint += n1
        leaf2.myMBB = new MBB(n1.offset, n1.offset, n1.lon, n1.lon, n1.lat, n1.lat)
    }

    def rtreePickSeeds(leaf1 : RTree, leaf2 : RTree) : Unit = {
        var d : Double = 0.0
        var t1 : Int = 0
        var t2 : Int = 0
        for (i <- 0 to (this.leavesRtree.size - 1)){
            for (j <- (i + 1) to (this.leavesRtree.size - 1)){
                var MBBNew : MBB = RTree.merge(this.leavesRtree(i).myMBB, this.leavesRtree(j).myMBB)
                var sNew : Double = MBBNew.getVolume()
                var s1 : Double = this.leavesRtree(i).myMBB.getVolume()
                var s2 : Double = this.leavesRtree(j).myMBB.getVolume()
                if ((sNew - s1 - s2) > d){
                    t1 = i
                    t2 = j
                    d = sNew - s1 - s2
                }
            }
        }
        var n2 : RTree = this.leavesRtree.remove(t2)
        n2.father = leaf1
        leaf1.leavesRtree += n2
        leaf1.myMBB = leaf1.leavesRtree(0).myMBB
        var n1 : RTree = this.leavesRtree.remove(t1)
        n1.father = leaf2
        leaf2.leavesRtree += n1
        leaf2.myMBB = leaf2.leavesRtree(0).myMBB
    }

    def pointPickNext(leaf1 : RTree, leaf2 : RTree) : Unit = {
        var d : Double = 0.0
        var t : Int = 0
        for (i <- 0 to (this.leavesPoint.size - 1)){
            var d1 : Double = RTree.volumeIncrease(RTree.merge(leaf1.myMBB, this.leavesPoint(i)), leaf1.myMBB)
            var d2 : Double = RTree.volumeIncrease(RTree.merge(leaf2.myMBB, this.leavesPoint(i)), leaf2.myMBB)
            if (abs(d1 - d2) > abs(d)){
                d = d1 - d2
                t = i
            }
        }
        if (d > 0){
            var target : Point = this.leavesPoint.remove(t)
            leaf2.myMBB = RTree.merge(leaf2.myMBB, target)
            leaf2.leavesPoint += target
        }else{
            var target : Point = this.leavesPoint.remove(t)
            leaf1.myMBB = RTree.merge(leaf1.myMBB, target)
            leaf1.leavesPoint += target
        }
    }

    def rtreePickNext(leaf1 : RTree, leaf2 : RTree) : Unit = {
        var d : Double = 0.0
        var t : Int = 0
        for (i <- 0 to (this.leavesRtree.size - 1)){
            var d1 : Double = RTree.volumeIncrease(RTree.merge(leaf1.myMBB, this.leavesRtree(i).myMBB), leaf1.myMBB)
            var d2 : Double = RTree.volumeIncrease(RTree.merge(leaf2.myMBB, this.leavesRtree(i).myMBB), leaf2.myMBB)
            if (abs(d1 - d2) > abs(d)){
                d = d1 - d2
                t = i
            }
        }
        if (d > 0){
            var target : RTree = this.leavesRtree.remove(t)
            leaf2.myMBB = RTree.merge(leaf2.myMBB, target.myMBB)
            target.father = leaf2
            leaf2.leavesRtree += target
        }else{
            var target : RTree = this.leavesRtree.remove(t)
            leaf1.myMBB = RTree.merge(leaf1.myMBB, target.myMBB)
            target.father = leaf1
            leaf1.leavesRtree += target
        }
    }

    def search(searchMBB : MBB) : ArrayBuffer[Int] = {
        var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
        if (this.level == 1){
            for (i <- 0 to (this.leavesPoint.size - 1)){
                var leaf : Point = this.leavesPoint(i)
                if (RTree.locate(searchMBB, leaf)){
                    result += (leaf.id)
                }
            }
            return result
        }else{
            for (i <- 0 to (this.leavesRtree.size - 1)){
                var leaf : RTree = this.leavesRtree(i)
                if (RTree.intersect(searchMBB, leaf.myMBB)){
                    result ++= leaf.search(searchMBB)
                }
            }
            return result
        }
    }

}

object RTree{

    def main(args : Array[String]) : Unit = {
        println("changzhihao")
        var root : RTree = new RTree(null, 1, 30, 50, null, 1)
        var myPointArray : ArrayBuffer[Point] = ArrayBuffer[Point]()
        val time1 : Long = System.currentTimeMillis
        for (i <- 0 to 999999){
            val id : Int = i
            val offset : Short = (i / 2).toShort
            val lon : Float = (new util.Random).nextInt(200).toFloat
            val lat : Float = (new util.Random).nextInt(200).toFloat
            val myPoint : Point = new Point(id, offset, lon, lat)
            myPointArray += (myPoint)
            root = insert(root, myPoint)
        }
        val time2 : Long = System.currentTimeMillis
        val result = root.search(merge(myPointArray(5), myPointArray(0)))
        val time3 : Long = System.currentTimeMillis
        println(((time2 - time1) / 1000.0).toDouble, ((time3 - time2) / 1000.0).toDouble)
        // print(result)
        print(result.size)
    }

    def insert(root : RTree, node : Point) : RTree = {
        var target : RTree = root.chooseLeaf(node)
        var returnRoot : RTree = null
        target.leavesPoint += node
        target.myMBB = merge(target.myMBB, node)
        target.adjustTree()
        if (root.father != null){
            returnRoot = root.father
        }else{
            returnRoot = root
        }
        returnRoot
    }

    def volumeIncrease(myMBB : MBB, node : Point) : Double = {
        val minOffset    : Int = min(myMBB.minOffset, node.offset)
        val maxOffset    : Int = max(myMBB.maxOffset, node.offset)
        val minLon       : Double = min(myMBB.minLon, node.lon)
        val maxLon       : Double = max(myMBB.maxLon, node.lon)
        val minLat       : Double = min(myMBB.minLat, node.lat)
        val maxLat       : Double = max(myMBB.maxLat, node.lat)
        var beforeVolume : Double = myMBB.getVolume()
        var afterVolume : Double = (maxOffset - minOffset) * (maxLon - minLon) * (maxLat - minLat)
        (afterVolume - beforeVolume) * 1.0
    }

    def volumeIncrease(MBB1 : MBB, MBB2 : MBB) : Double = {
        val minOffset    : Int = min(MBB1.minOffset, MBB2.minOffset)
        val maxOffset    : Int = max(MBB1.maxOffset, MBB2.maxOffset)
        val minLon       : Double = min(MBB1.minLon, MBB2.minLon)
        val maxLon       : Double = max(MBB1.maxLon, MBB2.maxLon)
        val minLat       : Double = min(MBB1.minLat, MBB2.minLat)
        val maxLat       : Double = max(MBB1.maxLat, MBB2.maxLat)
        val beforeVolume : Double = MBB1.getVolume()
        val afterVolume  : Double = (maxOffset - minOffset) * (maxLon - minLon) * (maxLat - minLat)
        (afterVolume - beforeVolume) * 1.0
    }

    def merge(myMBB : MBB, node : Point) : MBB = {
        if (myMBB == null){
            val minOffset : Short = node.offset
            val maxOffset : Short = node.offset
            val minLon    : Float = node.lon
            val maxLon    : Float = node.lon
            val minLat    : Float = node.lat
            val maxLat    : Float = node.lat
            new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
        }else{
            val minOffset : Short = min(myMBB.minOffset, node.offset).toShort
            val maxOffset : Short = max(myMBB.maxOffset, node.offset).toShort
            val minLon    : Float = min(myMBB.minLon, node.lon).toFloat
            val maxLon    : Float = max(myMBB.maxLon, node.lon).toFloat
            val minLat    : Float = min(myMBB.minLat, node.lat).toFloat
            val maxLat    : Float = max(myMBB.maxLat, node.lat).toFloat
            new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
        }
        
    }

    def merge(MBB1 : MBB, MBB2 : MBB) : MBB = {
        if (MBB1 == null){
            MBB2
        }else if (MBB2 == null){
            MBB1
        }else{
            val minOffset : Short = min(MBB1.minOffset, MBB2.minOffset).toShort
            val maxOffset : Short = max(MBB1.maxOffset, MBB2.maxOffset).toShort
            val minLon : Float = min(MBB1.minLon, MBB2.minLon).toFloat
            val maxLon : Float = max(MBB1.maxLon, MBB2.maxLon).toFloat
            val minLat : Float = min(MBB1.minLat, MBB2.minLat).toFloat
            val maxLat : Float = max(MBB1.maxLat, MBB2.maxLat).toFloat
            new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
        }
    }

    def merge(node1 : Point, node2 : Point) : MBB = {
        val minOffset : Short = min(node1.offset, node2.offset).toShort
        val maxOffset : Short = max(node1.offset, node2.offset).toShort
        val minLon : Float = min(node1.lon, node2.lon).toFloat
        val maxLon : Float = max(node1.lon, node2.lon).toFloat
        val minLat : Float = min(node1.lat, node2.lat).toFloat
        val maxLat : Float = max(node1.lat, node2.lat).toFloat
        new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
    }

    def locate(myMBB : MBB, node : Point) : Boolean = {
        if ((node.offset <= myMBB.maxOffset) && (node.offset >= myMBB.minOffset) && 
            (node.lon <= myMBB.maxLon) && (node.lat >= myMBB.minLon) && 
            (node.lat <= myMBB.maxLat) && (node.lat >= myMBB.minLat)){
            true
        }else{
            false
        }
    }

    def intersect(MBB1 : MBB, MBB2 : MBB) : Boolean = {
        val longOffset1 : Int = MBB1.maxOffset - MBB1.minOffset
        val widthLon1   : Float = MBB1.maxLon - MBB1.minLon
        val heightLat1  : Float = MBB1.maxLat - MBB1.minLat
        val cenOffset1  : Int = (MBB1.maxOffset + MBB1.minOffset) / 2
        var cenLon1     : Float = (MBB1.maxLon + MBB1.minLon) / 2
        val cenLat1     : Float = (MBB1.maxLat + MBB1.minLat) / 2
        val longOffset2 : Int = MBB2.maxOffset - MBB2.minOffset
        val widthLon2   : Float = MBB2.maxLon - MBB2.minLon
        val heightLat2  : Float = MBB2.maxLat - MBB2.minLat
        val cenOffset2  : Int = (MBB2.maxOffset + MBB2.minOffset) / 2
        var cenLon2     : Float = (MBB2.maxLon + MBB2.minLon) / 2
        val cenLat2     : Float = (MBB2.maxLat + MBB2.minLat) / 2
        if ((longOffset1 / 2 + longOffset2 / 2) >= abs(cenOffset1 - cenOffset2) && 
            (widthLon1 / 2 + widthLon2 / 2) >= abs(cenLon1 - cenLon2) && 
            (heightLat1 / 2 + heightLat2 / 2) >= abs(cenLat1 - cenLat2)){
            true
        }else{
            false
        }
    }

}
