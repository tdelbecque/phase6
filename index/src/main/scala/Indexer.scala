package indexer;

import java.nio.file.FileSystems
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{
  IndexWriter, IndexReader, IndexWriterConfig, 
  IndexOptions, DirectoryReader}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.document.{Document, Field, FieldType}
import org.apache.lucene.queryparser.classic.QueryParser

case class Element (
  pii: String = "",
  title: String = "",
  issn: String = "",
  journal: String = "",
  abstr: String = "" )
 
object Utils {
  def getFileList (dir: String) : Seq[String] = {
    import java.io.File
    new File (dir).
      listFiles.
      filter {_.isFile}.
      map {_.getPath}.
      toList
  }

  def using[A <: {def close () : Unit}, B](resource: A)(f: A => B) =
    try {
      f (resource)
    }
    finally {
      resource close
    }

  def usingFileLines[B](path: String)(f: String => B = {x : String => x}) : List[B] = 
    using(scala.io.Source.fromFile(path)) { source => {
      (for (l <- source.getLines) yield f (l)) toList
    }}
}

object Element {
  implicit def fromDocument (d: Document) = 
    Element (d.get ("pii"), d.get ("title"), d.get ("issn"), d.get ("journal"), d.get ("abstract"))
}

object Indexer /*extends App*/ {
  val indexPath = "/home/thierry/Elsevier/abstracts/data-hugues/index2"
  import IndexOptions._
  def addElementToIndex (w: IndexWriter, e: Element) {
    import e._
    val d = new Document

    val piiFieldType = new FieldType
    piiFieldType setIndexOptions DOCS
    piiFieldType setTokenized true
    piiFieldType setStored true
    d add new Field ("pii", pii, piiFieldType)

    val titleFieldType = new FieldType
    titleFieldType setIndexOptions DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS
    titleFieldType setStored true
    titleFieldType setStoreTermVectors true
    d add new Field ("title", title, titleFieldType)

    val issnFieldType = new FieldType
    issnFieldType setIndexOptions DOCS
    issnFieldType setTokenized false
    issnFieldType setStored true
    d add new Field ("issn", issn, issnFieldType)

    val journalFieldType = new FieldType
    journalFieldType setIndexOptions DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS
    journalFieldType setStored true
    journalFieldType setStoreTermVectors true
    d add new Field ("journal", journal, journalFieldType)

    val abstrFieldType = new FieldType
    abstrFieldType setIndexOptions DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS
    abstrFieldType setStored true
    abstrFieldType setStoreTermVectors true
    d add new Field ("abstr", abstr, abstrFieldType)

    w addDocument d
  }

  def getIndexDirectory : Directory = 
    FSDirectory open FileSystems.getDefault.getPath (indexPath)
 
  def numDocs : Int = {
    val indexDir = getIndexDirectory
    val reader = DirectoryReader.open (indexDir)
    val numDocs = reader.numDocs
    reader.close ()
    numDocs
  }

  def queryPII (pii: String) : Option[Element] = {
    val indexDir = getIndexDirectory
    val reader = DirectoryReader.open (indexDir)
    val searcher = new IndexSearcher (reader)
    val analyzer = new StandardAnalyzer
    val parser = new QueryParser ("pii", analyzer)
    val query = parser.parse (pii)
    val docs = searcher.search (query, 1)
    val hits = docs.scoreDocs
    if (docs.totalHits > 0) 
      Some (searcher.doc (hits(0).doc))
    else
      None
  }

  def queryTitle (title: String, n: Int) : (Long, Array[(Element, Float)]) = {
    val indexDir = getIndexDirectory
    val reader = DirectoryReader.open (indexDir)
    val searcher = new IndexSearcher (reader)
    val analyzer = new StandardAnalyzer
    val parser = new QueryParser ("title", analyzer)
    val query = parser.parse (title)
    val docs = searcher.search (query, n)
    val hits = docs.scoreDocs
    val m = Math.min (docs.totalHits, n).toInt
    val ret = Array.fill (m)((Element (), 0.toFloat))
    for (i <- 0 until m) 
      ret(i) = (searcher.doc (hits(i).doc), hits(i).score)
    (docs.totalHits, ret)
  }

  def main () {
    val indexDir = getIndexDirectory
    val defaultAnalyzer = new StandardAnalyzer
    val config = new IndexWriterConfig (defaultAnalyzer)
    config.setOpenMode (IndexWriterConfig.OpenMode.CREATE)
    val w = new IndexWriter (indexDir, config)
    try {
      val files = Utils.getFileList ("/home/thierry/Elsevier/abstracts/data-hugues/tsv")
      files.foreach { f => 
        println (f)
        val elements = Utils.usingFileLines (f) { l =>
          try {
            val Array (pii, title, _, issn, journal, _, abstr, _) = l split "\t"
            Element (pii, title, issn, journal, abstr)
          } catch {
            case e : MatchError => Element ("", "", "", "", "") 
            case e : Throwable => 
              println (s"==> $l <==")
              throw e
          }
        }
        elements.foreach {e => 
          if (e.pii.size > 0) addElementToIndex (w, e)}
      }
    } finally {
      w close
    }
  }
}
