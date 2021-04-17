import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.{lang => jl, util => ju}
import java.util.concurrent.atomic.AtomicInteger
import java.util.Arrays
import scala.collection.JavaConverters._
import java.util.HashMap
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.temporal.ChronoUnit
// $example on$
import org.apache.spark.mllib.fpm.PrefixSpan2

object PrefixSpanExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PrefixSpanExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val currentDate = LocalDate.parse("2018-12-24")
    val laterDate = currentDate.plusDays(1)
    val currentTime = LocalDateTime.now

    val map1 = Map(('a', currentDate), ('b', laterDate), ('c', laterDate), ('d', laterDate), ('e', laterDate))
    val map2 = Map(('a', laterDate), ('b', laterDate), ('c', laterDate))
    val map3 = Map(('b', laterDate), ('c', laterDate), ('d', laterDate))
    val map4 = Map(('b', laterDate), ('c', laterDate), ('e', laterDate))
    val map5 = Map(('c', laterDate), ('d', laterDate), ('e', laterDate))

    val sequences = sc.parallelize(Seq(
      Array(map1),
      Array(map2),
      Array(map3),
      Array(map4),
      Array(map5))).cache()

    val prefixSpan2 = new PrefixSpan2()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)

    val mode2 = prefixSpan2.run(sequences)

    mode2.freqSequences.collect().foreach { freqSequence =>
      println(
        s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
          s" ${freqSequence.freq}")
    }
    sc.stop()
  }
}
