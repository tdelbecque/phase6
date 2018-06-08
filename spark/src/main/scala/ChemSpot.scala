package com.sodad.abstracts

import org.apache.spark.sql.{DataFrame, SparkSession}
import de.berlin.hu.chemspot.ChemSpotFactory
import Aliases._

class SerializableChemPlot extends java.io.Serializable {
  var workload : Any = null

  private def writeObject (out: java.io.ObjectOutputStream) {

  }

  private def readObject (in: java.io.ObjectInputStream) {
    workload = ChemSpotFactory.createChemSpot ("/home/thierry/chemspot-2.0/dict.zip", "/home/thierry/chemspot-2.0/ids.zip", /*"/home/thierry/chemspot-2.0/multiclass.bin"*/ "")
  }

  private def readObjectNoData () {

  }

  def getWorkload : de.berlin.hu.chemspot.ChemSpot = workload.asInstanceOf[de.berlin.hu.chemspot.ChemSpot]
}

case class ChemSpotTag (start: Int, end: Int, text: String, _type: String)

object ChemSpot {
  def extractChemicals (data: DataFrame) (implicit session: SparkSession) = {
    import session.implicits._
    val b = session.sparkContext.broadcast (new SerializableChemPlot)
    data mapPartitions { it =>
      val tagger = b.value.getWorkload 
        //ChemSpotFactory.createChemSpot ("/home/thierry/chemspot-2.0/dict.zip", "", "")
      it map { x =>
        val s = x getString 0
        val ts = tagger.tag (s)
        val tags = new Array[ChemSpotTag](ts size)
        for (i <- 0 until ts.size) {
          val m = ts.get (i)
          tags(i) = ChemSpotTag (m.getStart, m.getEnd, m.getText, m.getType.toString)
        }
        (s, tags)
      }
    }

  }
}
