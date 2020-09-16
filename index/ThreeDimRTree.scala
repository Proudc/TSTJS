package src.main.scala.index

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

// import src.main.scala.dataFormat.MBB


class MBB{

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


class ThreeDimRTree extends Serializable{

    var leaves : ArrayBuffer[ThreeDimRTree] = null
    var myMBB  : MBB = null
    var level  : Int = _
    var minCap : Int = _
    var maxCap : Int = _
    var father : ThreeDimRTree = _
    // Flag is 0 means the node is an rtree leaf node, the default is 1
    var flag   : Int = _
    // The trajectory id corresponding to the leaf node
    var index  : Int = _

    def this(myMBB : MBB, level : Int, minCap : Int, maxCap : Int, father : ThreeDimRTree, flag : Int, index : Int) = {
        this()
        this.leaves = ArrayBuffer[ThreeDimRTree]()
        this.myMBB  = myMBB
        this.level  = level
        this.minCap = minCap
        this.maxCap = maxCap
        this.father = father
        this.flag   = flag
        this.index  = index
    }

    /**
    * Choose a leaf node when inserting
    */
    def chooseLeaf(node : ThreeDimRTree) : ThreeDimRTree = {
        if (this.level == (node.level + 1)){
            this
        }else{
            var minValue : Double = Double.MaxValue
            var pos : Int = -1
            for (i <- 0 to (this.leaves.size - 1)){
                val volumeIncNum : Double = ThreeDimRTree.volumeIncrease(this.leaves(i).myMBB, node.myMBB)
                if (minValue > volumeIncNum){
                    minValue = volumeIncNum
                    pos = i
                }
            }
            this.leaves(pos).chooseLeaf(node)
        }
    }

    /**
    * Adjust rtree to balance it
    */
    def adjustTree() : Unit = {
        var p : ThreeDimRTree = this
        // Adjust rtree from bottom to up
        while(p != null){
            if ((p.leaves.size) > p.maxCap){
                // If the children of the current node are greater than the maximum capacity, then split
                p.splitNode()
            }else{
                if (p.father != null){
                    // Otherwise, merge the MBB of this node with the MBB of the father node
                    p.father.myMBB = ThreeDimRTree.merge(p.father.myMBB, p.myMBB)
                }
            }
            p = p.father
        }
    }

    /**
    * Split a node into two nodes
    */
    def splitNode() : Unit = {
        if (this.father == null){
            this.father = new ThreeDimRTree(null, (this.level + 1), this.minCap, this.maxCap, null, 1, -1)
            this.father.leaves += this
        }
        var leaf1 : ThreeDimRTree = new ThreeDimRTree(null, (this.level), this.minCap, this.maxCap, null, 1, -1)
        var leaf2 : ThreeDimRTree = new ThreeDimRTree(null, (this.level), this.minCap, this.maxCap, null, 1, -1)
        var loop = new Breaks
        this.pickSeeds(leaf1, leaf2)
        loop.breakable{
        while(this.leaves.size > 0){
            if ((leaf1.leaves.size > leaf2.leaves.size) && (leaf2.leaves.size + this.leaves.size) == this.minCap){
                for (i <- 0 to (this.leaves.size - 1)){
                    var leaf : ThreeDimRTree = this.leaves(i)
                    leaf2.myMBB = ThreeDimRTree.merge(leaf2.myMBB, leaf.myMBB)
                    leaf2.leaves += leaf
                    leaf.father = leaf2
                }
                this.leaves.clear()
                loop.break()
            }
            if ((leaf2.leaves.size > leaf1.leaves.size) && (leaf1.leaves.size + this.leaves.size) == this.minCap){
                for (i <- 0 to (this.leaves.size - 1)){
                    var leaf : ThreeDimRTree = this.leaves(i)
                    leaf1.myMBB = ThreeDimRTree.merge(leaf1.myMBB, leaf.myMBB)
                    leaf1.leaves += leaf
                    leaf.father = leaf1
                }
                this.leaves.clear()
                loop.break()
            }
            this.pickNext(leaf1, leaf2)
        }
        }
        this.father.leaves.remove(this.father.leaves.indexOf(this))
        leaf1.father = this.father
        leaf2.father = this.father
        this.father.leaves += leaf1
        this.father.leaves += leaf2
        this.father.myMBB = ThreeDimRTree.merge(this.father.myMBB, leaf1.myMBB)
        this.father.myMBB = ThreeDimRTree.merge(this.father.myMBB, leaf2.myMBB)
    }

    /**
    * Auxiliary function of splitnode() function
    */
    def pickSeeds(leaf1 : ThreeDimRTree, leaf2 : ThreeDimRTree) : Unit = {
        var d : Double = 0.0
        var t1 : Int = 0
        var t2 : Int = 0
        for (i <- 0 to (this.leaves.size - 1)){
            for (j <- (i + 1) to (this.leaves.size - 1)){
                var MBBNew : MBB = ThreeDimRTree.merge(this.leaves(i).myMBB, this.leaves(j).myMBB)
                var sNew : Double = 1.0 * (MBBNew.maxOffset - MBBNew.minOffset) * (MBBNew.maxLon - MBBNew.minLon) * (MBBNew.maxLat - MBBNew.minLat)
                var s1 : Double = 1.0 * (this.leaves(i).myMBB.maxOffset - this.leaves(i).myMBB.minOffset) * (
                                         this.leaves(i).myMBB.maxLon - this.leaves(i).myMBB.minLon) * (
                                         this.leaves(i).myMBB.maxLat - this.leaves(i).myMBB.minLat)
                var s2 : Double = 1.0 * (this.leaves(j).myMBB.maxOffset - this.leaves(j).myMBB.minOffset) * (
                                         this.leaves(j).myMBB.maxLon - this.leaves(j).myMBB.minLon) * (
                                         this.leaves(j).myMBB.maxLat - this.leaves(j).myMBB.minLat)
                if ((sNew - s1 - s2) > d){
                    t1 = i
                    t2 = j
                    d = sNew - s1 - s2
                }
            }
        }
        var n2 : ThreeDimRTree = this.leaves.remove(t2)
        n2.father = leaf1
        leaf1.leaves += n2
        leaf1.myMBB = leaf1.leaves(0).myMBB
        var n1 : ThreeDimRTree = this.leaves.remove(t1)
        n1.father = leaf2
        leaf2.leaves += n1
        leaf2.myMBB = leaf2.leaves(0).myMBB
    }

    
    /**
    * Auxiliary function of splitnode() function
    */
    def pickNext(leaf1 : ThreeDimRTree, leaf2 : ThreeDimRTree) : Unit = {
        var d : Double = 0.0
        var t : Int = 0
        for (i <- 0 to (this.leaves.size - 1)){
            var d1 : Double = ThreeDimRTree.volumeIncrease(ThreeDimRTree.merge(leaf1.myMBB, this.leaves(i).myMBB), leaf1.myMBB)
            var d2 : Double = ThreeDimRTree.volumeIncrease(ThreeDimRTree.merge(leaf2.myMBB, this.leaves(i).myMBB), leaf2.myMBB)
            if (abs(d1 - d2) > abs(d)){
                d = d1 - d2
                t = i
            }
        }
        if (d > 0){
            var target : ThreeDimRTree = this.leaves.remove(t)
            leaf2.myMBB = ThreeDimRTree.merge(leaf2.myMBB, target.myMBB)
            target.father = leaf2
            leaf2.leaves += target
        }else{
            var target : ThreeDimRTree = this.leaves.remove(t)
            leaf1.myMBB = ThreeDimRTree.merge(leaf1.myMBB, target.myMBB)
            target.father = leaf1
            leaf1.leaves += target
        }
    }


    /**
    * Search on rtree
    */
    def search(searchMBB : MBB) : ArrayBuffer[Int] = {
        var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
        if (this.level == 1){
            for (i <- 0 to (this.leaves.size - 1)){
                var leaf : ThreeDimRTree = this.leaves(i)
                if (ThreeDimRTree.intersect(searchMBB, leaf.myMBB)){
                    result += (leaf.index)
                }
            }
            return result
        }else{
            for (i <- 0 to (this.leaves.size - 1)){
                var leaf : ThreeDimRTree = this.leaves(i)
                if (ThreeDimRTree.intersect(searchMBB, leaf.myMBB)){
                    result ++= leaf.search(searchMBB)
                }
            }
            return result
        }
    }

}

object ThreeDimRTree{

    def main(args : Array[String]) : Unit = {
        println("changzhihao")
        var root : ThreeDimRTree = new ThreeDimRTree(null, 1, 10, 50, null, 1, -1)
        var myMBBArray : ArrayBuffer[MBB] = ArrayBuffer[MBB]()
        val time1 : Long = System.currentTimeMillis
        for (i <- 0 to 99999){
            val minOffset : Int = (new util.Random).nextInt(1000)
            val maxOffset : Int = minOffset
            val minLon : Double = (new util.Random).nextInt(200).toDouble
            val maxLon : Double = (new util.Random).nextInt(200).toDouble
            val minLat : Double = (new util.Random).nextInt(200).toDouble
            val maxLat : Double = (new util.Random).nextInt(200).toDouble
            val testMBB : MBB = new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
            myMBBArray += (testMBB)
            var node : ThreeDimRTree = new ThreeDimRTree(testMBB, 0, 10, 50, null, 0, i)
            root = insert(root, node)
        }
        val time2 : Long = System.currentTimeMillis
        val result = root.search(merge(myMBBArray(5), myMBBArray(0)))
        val time3 : Long = System.currentTimeMillis
        println(((time2 - time1) / 1000.0).toDouble, ((time3 - time2) / 1000.0).toDouble)
        print(result.size)
    }


    /**
    * Insert a node into rtree
    */
    def insert(root : ThreeDimRTree, node : ThreeDimRTree) : ThreeDimRTree = {
        var target : ThreeDimRTree = root.chooseLeaf(node)
        var returnRoot : ThreeDimRTree = null
        node.father = target
        target.leaves += node
        target.myMBB = merge(target.myMBB, node.myMBB)
        target.adjustTree()
        if (root.father != null){
            returnRoot = root.father
        }else{
            returnRoot = root
        }
        returnRoot
    }

    /**
    * Calculate the volume increase after the two MBBs are merged
    */
    def volumeIncrease(MBB1 : MBB, MBB2 : MBB) : Double = {
        val minOffset    : Double = min(MBB1.minOffset, MBB2.minOffset)
        val maxOffset    : Double = max(MBB1.maxOffset, MBB2.maxOffset)
        val minLon       : Double = min(MBB1.minLon, MBB2.minLon)
        val maxLon       : Double = max(MBB1.maxLon, MBB2.maxLon)
        val minLat       : Double = min(MBB1.minLat, MBB2.minLat)
        val maxLat       : Double = max(MBB1.maxLat, MBB2.maxLat)
        val beforeVolume : Double = MBB1.volume
        val afterVolume  : Double = (maxOffset - minOffset) * (maxLon - minLon) * (maxLat - minLat)
        (afterVolume - beforeVolume) * 1.0
    }

    /**
    * Merge two MBBs to one MBB
    */
    def merge(MBB1 : MBB, MBB2 : MBB) : MBB = {
        if (MBB1 == null){
            MBB2
        }else if (MBB2 == null){
            MBB1
        }else{
            val minOffset : Int = min(MBB1.minOffset, MBB2.minOffset)
            val maxOffset : Int = max(MBB1.maxOffset, MBB2.maxOffset)
            val minLon : Double = min(MBB1.minLon, MBB2.minLon)
            val maxLon : Double = max(MBB1.maxLon, MBB2.maxLon)
            val minLat : Double = min(MBB1.minLat, MBB2.minLat)
            val maxLat : Double = max(MBB1.maxLat, MBB2.maxLat)
            new MBB(minOffset, maxOffset, minLon, maxLon, minLat, maxLat)
        }
    }

    /**
    * Calculate whether two MBBs intersect.
    * Returns true if intersect, otherwise returns false.
    */
    def intersect(MBB1 : MBB, MBB2 : MBB) : Boolean = {
        val longOffset1 : Double = MBB1.longOffset
        val widthLon1   : Double = MBB1.widthLon
        val heightLat1  : Double = MBB1.heightLat
        val cenOffset1  : Double = MBB1.cenOffset
        var cenLon1     : Double = MBB1.cenLon
        val cenLat1     : Double = MBB1.cenLat
        val longOffset2 : Double = MBB2.longOffset
        val widthLon2   : Double = MBB2.widthLon
        val heightLat2  : Double = MBB2.heightLat
        val cenOffset2  : Double = MBB2.cenOffset
        var cenLon2     : Double = MBB2.cenLon
        val cenLat2     : Double = MBB2.cenLat
        if ((longOffset1 / 2 + longOffset2 / 2) >= abs(cenOffset1 - cenOffset2) && (widthLon1 / 2 + widthLon2 / 2) >= abs(cenLon1 - cenLon2) && (heightLat1 / 2 + heightLat2 / 2) >= abs(cenLat1 - cenLat2)){
            true
        }else{
            false
        }
    }


}