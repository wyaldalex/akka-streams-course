package part3_graphs

import akka.NotUsed
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, Zip}
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
