package com.sodad.abstracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

case class HeadCategoriesOrig (
  journal: String,
  issn: String,
  category: String,
  dummy: String
)

case class JournalCategoriesRecord (
  categories: Array[String],
  hot : Array[Short]
)

object HeadCategories {
  //implicit def getSession : SparkSession = SparkSession.getDefaultSession.get

  val mainCategoriesOrigPath = X.storagePrefix + "/resources/JournalHeadCategories-main-orig.csv"
  val mainCategoriesPath = X.storagePrefix + "/resources/JournalHeadCategories-main"

  val subCategoriesOrigPath = X.storagePrefix + "/resources/JournalHeadCategories-sub-orig.csv"
  val subCategoriesPath = X.storagePrefix + "/resources/JournalHeadCategories-sub.csv"

  def reader (implicit session: SparkSession) : String => DataFrame = 
    session.sqlContext.read.
      option ("header", "true").
      option ("delimiter", "\t").
      csv _

  def createCategoriesDico (fileInPath: String) 
    (implicit session: SparkSession) : (Seq[String], Map[String, Int]) = {
    import session.implicits._
    val r = reader
    val categories = r (fileInPath).
      map (_.getString(2)).
      distinct.collect.sorted
    (categories, categories.zipWithIndex.toMap)
  }

  def createIssnToJournalDico (implicit session: SparkSession) : Map[String, String] = {
    import session.implicits._
    val r = reader
    r (mainCategoriesOrigPath).
      map { x => (x.getString(1) replaceFirst ("-", ""), x.getString(0)) }.
      distinct.collect.toMap
  }

  def createMainCategories (implicit session: SparkSession) = 
    createCategoriesDico (mainCategoriesOrigPath)

  def createCategoriesFile (fileInPath: String, fileOutPath: String) 
    (implicit session: SparkSession) = {
    import session.implicits._
    val (mainCategories, mainCategoriesToIndex) = createMainCategories
    val fileIn = session.sqlContext.read.
      option ("header", "true").
      option ("delimiter", "\t").
      csv (fileInPath).toDF ("journal", "issn", "category", "dummy").as[HeadCategoriesOrig].
      rdd. map { x => (x.issn replaceFirst ("-", ""), x.category) } .
      groupByKey .
      mapValues { categoriesIt => 
        val hot = Array.fill[Short](mainCategories size)(0)
        val categories = categoriesIt.toArray
        categoriesIt foreach { catStr =>  hot (mainCategoriesToIndex (catStr)) = 1 }
        (categories, hot)
      } . map { case (issn, (catStr, hot)) => (issn, catStr, hot) } .
      toDF ("issn", "categories", "hot").
      write.option ("path", fileOutPath).
      save
  }

  def createMainCategoriesFile (implicit session: SparkSession) = 
    createCategoriesFile (mainCategoriesOrigPath, mainCategoriesPath)

  def readMainCategories (implicit session: SparkSession) = {
    import session.implicits._
    session.read.option ("path", mainCategoriesPath).load.
      map { x => 
        (x getString 0,
          JournalCategoriesRecord (
            (x getSeq[String] 1) toArray,
            (x getSeq[Short] 2) toArray ))
      }.
      collect.toMap
  }
}
