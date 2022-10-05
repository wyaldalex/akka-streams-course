package part2_primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

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
