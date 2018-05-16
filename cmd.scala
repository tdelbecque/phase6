import org.apache.spark.sql.{SparkSession, Dataset}
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import java.util.Properties
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

import Aliases._

class B (implicit session: SparkSession) {
  import session.implicits._
  import scala.collection.JavaConversions._

  def createClassifierData (data: Dataset[(String, Array[Double])], targetMap: Map[String, Int]) = {
    data map { case (pii, xs) => org.apache.spark.ml.feature.LabeledPoint (
      targetMap (pii slice (1,9)).toDouble,
      org.apache.spark.ml.linalg.Vectors dense (for (x <- xs.tail) yield x/xs.head))
    }
  }

  def foo (data: OrigDataSet) = {
    data mapPartitions { it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit")
      val pipeline = new StanfordCoreNLP (props)
      it flatMap {case OrigRecord (pii, abstr) => 
        val ann = new Annotation (abstr)
        pipeline annotate ann

        (for (s <- ann get classOf[SentencesAnnotation]) yield (
          for (t <- (s get classOf [TokensAnnotation]).map (_.originalText())) yield t
        ).toList).toList

      }
    }}
  }
  def saveTokensPerDoc (data: OrigDataSet) = {
    data mapPartitions (it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit")
      val pipeline = new StanfordCoreNLP (props)
      it flatMap {case OrigRecord (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (for (s <- ann get classOf[SentencesAnnotation];
            t <- (s get classOf[TokensAnnotation]).map (_.originalText()))
        yield t).toList map ((_, pii))
      }
    })
  }

}

/*
class B (implicit session: SparkSession) {
  def foo (a: A) = {
    import session.implicits._
    val data = a.data
    val floatStr = """(?:\+/-|\+-|\+|-)?\s*\d+(?:\.\d+)*"""
    val pattern = a.chemicalPatternStr (100000).r
    val pattern2 = """[^\s]+sodad""".r
    val pattern_pct = s"$floatStr\\s*%".r
    val pattern_numexpr1 = s"[<>=]+\\s*$floatStr".r
    val pattern_numexpr2 = s"$floatStr\\s*[<>=]+".r
    val pattern_num = s"$floatStr".r
    val pattern_paren = """\(.*?\)""".r
    data mapPartitions { it => 
      it map { x  =>
        val s1 = pattern replaceAllIn (x.abstr, "sodad_chemical")
        val s2 = pattern2 replaceAllIn (s1, "sodad")
        val s3 = pattern_pct replaceAllIn (s2, " sodad_pct ")
        val s4 = pattern_numexpr1 replaceAllIn (s3, " sodad_numexpr ")
        val s5 = pattern_numexpr1 replaceAllIn (s4, " sodad_numexpr ")
        val s6 = pattern_num replaceAllIn (s5, " sodad_num ")
        val s7 = pattern_paren replaceAllIn (s6, "sodad_paren")
        val s = s7
        OrigRecord (x pii, s)
      }
    }
  }
}

val b = new B
val z = b.foo (a).cache
using (new java.io.PrintWriter ("replacement")) (_ write (z.filter (_.abstr.indexOf("sodad") != -1).map (_.abstr).collect.mkString("\n")))
 */
