package part3_graphs

import akka.NotUsed
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}

object StreamGraphsIntro extends App {

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global
  val source = Source(1 to 1000)
  val incrementerFlow = Flow[Int].map(x => x + 1)
  val multiplierFlow = Flow[Int].map(x => x * 10)
  val outputSink = Sink.foreach[(Int,Int)](println) //A sink thhat takes two values

  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._ //bring requiered operators

      val broadcast = builder.add(Broadcast[Int](2)) //fan out operator - returns 2 values
      val zip = builder.add(Zip[Int,Int]) //fan in operator
      //step 3 typing components
      source ~> broadcast

      broadcast.out(0) ~> incrementerFlow ~> zip.in0
      broadcast.out(1) ~> multiplierFlow ~> zip.in1

      zip.out ~> outputSink
      //return a close Shape
      ClosedShape
    }
  )
  graph.run()

}

object AkkaStreamsExercise1 extends App {

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  //Single source to two sinks
  val source = Source(1 to 1000)
  val incrementerFlow = Flow[Int].map(x => x + 1)
  val multiplierFlow = Flow[Int].map(x => x * 10)
  val firstSink = Sink.foreach[Int](x => println(s"First sink: $x"))
  val secondSink = Sink.foreach[Int](x => println(s"Second sink: $x"))

  // step 1
  val sourceToTwoSinksGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2 - declaring the components
      val broadcast = builder.add(Broadcast[Int](2))

      // step 3 - tying up the components
      source ~> broadcast ~> firstSink // implicit port numbering
      broadcast ~> secondSink
      //      broadcast.out(0) ~> firstSink
      //      broadcast.out(1) ~> secondSink

      // step 4
      ClosedShape
    }
  )
  sourceToTwoSinksGraph.run()
}

object GrapshExercise2 extends App {
  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  import scala.concurrent.duration._

  val input = Source(1 to 1000)
  val fastSource = input.throttle(5, 1 second)
  val slowSource = input.throttle(2, 1 second)

  val sink1 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1 number of elements: $count")
    count + 1
  })

  val sink2 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 2 number of elements: $count")
    count + 1
  })

  // step 1
  val balanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._


      // step 2 -- declare components
      val merge = builder.add(Merge[Int](2))
      val balance = builder.add(Balance[Int](2))


      // step 3 -- tie them up
      fastSource ~> merge ~> balance ~> sink1
      slowSource ~> merge
      balance ~> sink2

      // step 4
      ClosedShape
    }
  )

  balanceGraph.run()
}