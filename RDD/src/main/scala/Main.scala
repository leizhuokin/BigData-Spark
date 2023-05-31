import com.alibaba.fastjson2.JSON
import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source
import scala.util.matching.Regex

case class Answer(qid: Long, title: String, desc: String, topic: String, star: Int, content: String, answer_id: Long, answerer_tags: String)

object Main extends App {
  val source = Source.fromFile("D:\\data\\web_text_zh_testa.json", "UTF-8")
  val data = source.getLines()
    .toList
    .map(ele => JSON.parseObject(ele, classOf[Answer]))

  //1.热点问题
  val top1 = data.groupBy(_.qid)
    .map(ele => (ele._1, ele._2.size))
    .toList
    .sortBy(_._2)
  //println(top1.mkString("\n"))

  //2.热门标签
  val pattern = new Regex("[\\u4E00-\\u9FA5]+")
  val topTapCount = data.map(ele => (ele.answerer_tags))
    .flatMap(ele => pattern.findAllMatchIn(ele).map(ele => ele.matched))
    .map(ele => (ele, 1))
    .groupBy(_._1)
    .map(ele => (ele._1, ele._2.size))
    .toList
    .sortBy(-_._2)
  println(topTapCount.mkString("\n"))

  //高赞回复
  val topStar = data.map(e => (e.star, e))
    .sortBy(_._1)
  topStar.takeRight(10).reverse.map(x => s"${x._1}\t${x._2}").foreach(println)

      val fenci=data.map(_.content)
        .flatMap(
          //转换Java的List为Scala的List
          ToAnalysis.parse(_).getTerms.asScala.map(x=>(x.getName,1)))
        .groupBy(_._1)
        .map(x=>(x._1,x._2.size))
        .toList
        .filter(_._1.length>1)
        .sortWith((v1,v2)=>v1._2-v2._2 > 0)
        .take(10)
      println(fenci.mkString("\n"))

  //任务1平均长度
  val length = data.map(_.title.length)
  //println(length.sum/length.size)
  //任务2

  //任务3
  val topic = data.map(ele => (ele.topic, 1))
    .groupBy(_._1)
    .map(ele => (ele._1, ele._2.size))
    .toList
    .sortBy(-_._2)
    .take(10)
  //println(topic.mkString("\n"))

}



