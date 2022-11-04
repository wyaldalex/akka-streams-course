package playground

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.util.{ByteString, Timeout}
import playground.CSVReaderTest.AccumulatorActorTest.OperationResponse

import scala.concurrent.duration._
import java.nio.file.Paths

object CSVReaderTest extends App {

  case class TaxiTripEntry(vendorID: Int, tpepPickupDatetime: String, tpepDropoffDatetime: String, passengerCount: Int,
                           tripDistance: Double, pickupLongitude: Double, pickupLatitude: Double, rateCodeID: Int,
                           storeAndFwdFlag: String, dropoffLongitude: Double, dropoffLatitude: Double,
                           paymentType: Int, fareAmount: Double, extra: Double, mtaTax: Double,
                           tipAmount: Double, tollsAmount: Double, improvementSurcharge: Double, totalAmount: Double)

  def fromCsvEntryToCaseClass(csvEntry : String): TaxiTripEntry ={
    val arrayString = csvEntry.split(",")
    println(s"Trying to parse $csvEntry")
    TaxiTripEntry(
      arrayString(0).toInt,
      arrayString(1).toString,
      arrayString(2).toString,
      arrayString(3).toInt,
      arrayString(4).toDouble,
      arrayString(5).toDouble,
      arrayString(6).toDouble,
      arrayString(7).toInt,
      arrayString(8).toString,
      arrayString(9).toDouble,
      arrayString(10).toDouble,
      arrayString(11).toInt,
      arrayString(12).toDouble,
      arrayString(13).toDouble,
      arrayString(14).toDouble,
      arrayString(15).toDouble,
      arrayString(16).toDouble,
      arrayString(17).toDouble,
      arrayString(18).toDouble
    )
  }

  val file = Paths.get("src/main/resources/1ksample.csv")

  implicit val system = ActorSystem("principleSystem")
  implicit val materializer = ActorMaterializer()

  object AccumulatorActorTest {
   def props: Props = Props(new AccumulatorActorTest)
   case class OperationResponse(id: Int)
   case object PrintResults
  }
  class AccumulatorActorTest extends Actor with ActorLogging {

    import AccumulatorActorTest._

    var totalAmount: Double = 0
    var totalEntries = 0

    override def receive: Receive = {
      case taxiEntry : TaxiTripEntry =>
        totalAmount += taxiEntry.totalAmount
        totalEntries += 1
        log.info(s"Processing taxi trip entry $totalEntries")
        sender() ! OperationResponse(totalEntries)
      case PrintResults =>
        log.info(s"Printing final results at ${self.path}")
      case _ =>
        log.info("Wrong messaged delivered")
    }
  }
  val accumulatorActorTest = system.actorOf(AccumulatorActorTest.props)
  val accumulatorActorTest2 = system.actorOf(AccumulatorActorTest.props)


  //sources -> produce stuff
  val source = FileIO
    .fromPath(file)

  //flow transform
//  val toStringFlow = Flow[ByteString].map(byteString => {
//    println("Converting to taxi entry")
//    val stringVal = byteString.utf8String
//    fromCsvEntryToCaseClass(stringVal)
//  })
  val toStringFlow = Framing.delimiter(ByteString("\n"), 256, true).map(_.utf8String)
  val toTaxiEntryFlow = Flow[String].map(fromCsvEntryToCaseClass)

  //sinks -> consume stuff
  //val sink = Sink.foreach[TaxiTripEntry](x => println(x.toString()))
  import AccumulatorActorTest._
  val sink = Sink.foreach[OperationResponse](x => println(x.toString()))

  implicit val timeout = Timeout(10 seconds)

  val runnableGraph = source.via(toStringFlow)
    .via(toTaxiEntryFlow).mapAsync(parallelism = 1)(event => (accumulatorActorTest ? event ).mapTo[OperationResponse])
    .to(sink)
  val runnableGraph2 = source.via(toStringFlow)
    .via(toTaxiEntryFlow).mapAsync(parallelism = 1)(event => (accumulatorActorTest2 ? event).mapTo[OperationResponse])
    .to(sink)
  runnableGraph.run()
  runnableGraph2.run()
  accumulatorActorTest ! PrintResults
  accumulatorActorTest2 ! PrintResults

  //val graph = source.to(sink)
  //graph.run()
}

object GraphDSLTest extends App {
  implicit val actorSystem = ActorSystem("NumberSystem")
  implicit val materializer = ActorMaterializer()

  val graph = RunnableGraph.fromGraph( GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val in = Source(1 to 10) //Fan-In Operator
      val out = Sink.foreach(println) //Fan
      //Here, bcast is the Broadcast function, which is a Fan-Out function,
      //and merge is Merge function, which is a Fan-In function.
      //Fanout Broadcast[T]: (1 input, N outputs) given an input element emits to each output
      val bcast = builder.add(Broadcast[Int](2))
      //Merge (N inputs, 1 output) This picks randomly from inputs pushing them one by one to the output
      val merge = builder.add(Merge[Int](2))
      val f1, f2, f3, f4 = Flow[Int].map(_ + 1)
      val f5 = Flow[Int].filter(x => x % 2 == 0)
      in ~> f1 ~> bcast ~> f2 ~>  merge ~> f4 ~> f5 ~> out
      bcast ~> f3 ~> merge

      ClosedShape
  })

  graph.run()
  actorSystem.terminate()

}
