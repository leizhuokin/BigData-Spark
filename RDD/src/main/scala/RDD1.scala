

object RDD1 {
  def main(args: Array[String]): Unit ={

    scala.io.Source
      .fromFile("input/students_exam_1.data", "UTF-8")
      .getLines()
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(0), x(7).toInt))
      .toList
      .sortBy(-_._2)
      .take(3)
      .foreach(println(_))
  }
}
