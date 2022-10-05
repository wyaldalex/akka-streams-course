package part2_primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object FirstPrinciples extends App {

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()

  //sources -> produce stuff
  val source = Source(1 to 9)
  //sinks -> consume stuff
  val sink = Sink.foreach[Int](println)

  //Then to run:
  val graph =  source.to(sink)
  graph.run()

  //Use of flows for further middle path processing, and more natural approach
  val flow = Flow[Int].map(x => x * 100)
  //Then append this middle flow processing
  val graph2 = source.via(flow).to(sink)
  graph2.run()

  //Multiple flows can be used before it reaches the sink
  val flow2 = Flow[Int].map(x => x + 333)
  val graph3 = source.via(flow).via(flow2).to(sink)
  graph3.run()

  //various kinds of sources,sinks and flows
  val finitieSource = Source.single(1)
  val anotherFinitieSource = Source(List(1,2,3))
  val emptySource = Source.empty[Int]
  val infiniteSource = Source(Stream.from(1))
  import scala.concurrent.ExecutionContext.Implicits.global
  val futureSource = Source.fromFuture(Future(1212))

  val theMostBoringSink = Sink.ignore
  val forEachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int]
  val foldSink = Sink.fold[Int,Int](0)((a,b) => a+b)

  //flows - usually mapped to collector operators
  val mapFlow = Flow[Int].map(x => 2 * x)
  val takeFlow = Flow[Int].take(5)
  //drop and filter available
  //there is no flap map

  //Syntactic sugars
  val mapSource = Source(1 to 10).map(x => x * 2) // == Source(1 to 10).via(Flow[Int].map(x => x * 2))
  mapSource.runForeach(println) // == mapSource.to(Sink.foreach[Int](println)).run()
  //Exercise
  //Take only 2 names which length is bigger than 5
  val someListOfNames = List("xir","xer","Xorbasda","pur","Khyron","adjasdgasjdhjas","xor","xar")
  val namesSource = Source(someListOfNames)
  val namesSink = Sink.foreach[String](println)
  val flowName1 = Flow[String].filter(x => x.length >= 5)
  val flowName2 = Flow[String].take(2)
  val nameGraph = namesSource.via(flowName1).via(flowName2).to(namesSink)
  nameGraph.run()
  //shorthands
  namesSource.filter(x => x.length >= 5).take(2).to(Sink.foreach[String](println)).run()
  namesSource.filter(x => x.length >= 5).take(2).runForeach(println)
  //WARNING NUUUUUUULLLLLSLLSLS NOT ALLOWEEEOUDUED
}

object MaterializingIntro extends App {

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global


  val source = Source(1 to 9)
  val flow = Flow[Int].map(x => x * 100)
  val flow2 = Flow[Int].map(x => x + 1)
  //val sink = Sink.foreach[Int](println)
  val sink = Sink.reduce[Int]((a,b) => a + b)
  //val graph = source.to(sink)
  //val someMaterializedView = graph.run()
  //val graph = source.viaMat(flow)(Keep.right).toMat(sink)(Keep.right)
  //val graph = source.viaMat(flow)(Keep.left).toMat(sink)(Keep.right)
  val graph = source.viaMat(flow)(Keep.left).viaMat(flow2)(Keep.right).toMat(sink)(Keep.right)
  graph.run().onComplete{
    case Success(value) =>
      println(s"The result of the graph is $value")
    case Failure(ex) =>
      println("Failed to retrieve graph")
  }

  //sugar
  val sum = source.runWith(Sink.reduce[Int]((a,b) => a+ b))
  //val sum =  Source(1 to 10).runReduce(_ + _)
  sum.onComplete{
    case Success(value) =>
      println(s"The result of the graph is $value")
    case Failure(ex) =>
      println("Failed to retrieve graph")
  }

  //return the last element of a source
  //total word count of a stream of sentences

  val lastVal = Source(1 to 100).runWith(Sink.last)
  lastVal.onComplete{
    case Success(value) =>
      println(s"The last val is $value")
    case Failure(ex) =>
      println(s"The last val is")
  }

  val senteceSource = Source(List("fffff asdas box","ssssss asda total","4444 asda" , "imperial el 999999999 adas"))
  val sentenceFlow1 = Flow[String].map(sentence => sentence.split(" ").size) //flow returns a collection of number of words per sentence
  val sentenceFlow2 = Flow[Int].reduce((a,b) => a + b)
  val sinkSentence = Sink.head[Int]
  val totalWords = senteceSource.viaMat(sentenceFlow1)(Keep.right).
    viaMat(sentenceFlow2)(Keep.right).toMat(sinkSentence)(Keep.right)

  totalWords.run().onComplete{
    case Success(value) =>
      println(s"Total words $value")
    case Failure(ex) =>
      println("Failed to get total words")

  }
  //shorthand
  val sumWordSentences = senteceSource.viaMat(sentenceFlow1)(Keep.right).runWith(Sink.reduce[Int]((a,b) => a+ b))
  sumWordSentences.onComplete{
    case Success(value) =>
      println(s"Total words shorthand $value")
    case Failure(ex) =>
      println("Failed to get total words")
  }

}

object AsyncBoundariesAndOrder extends App {

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global


  val source = Source(1 to 9)
  val flow = Flow[Int].map(x => {
    Thread.sleep(2000)
    x * 100
  })
  val flow2 = Flow[Int].map(x => {
    Thread.sleep(2000)
    x + 1
  })
  val sink = Sink.foreach[Int](println)

  source.via(flow).via(flow2).to(sink).run() //all executed in one actor - takes longer
  source.via(flow).async// 1st actor
    .via(flow2).async //2nd actors
    .to(sink) //3rd actor
    .run() //faster execution

}
