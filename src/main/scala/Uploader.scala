import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{RequestEntity, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import spray.json.RootJsonFormat
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import scala.concurrent.Future

final case class Message(docType: String, content: String, ctxId: Long)

object Uploader {

  val host = "localhost"
  val port = 9200

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher


  val connectionFlow: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]] =
    Http().outgoingConnection(host, port)

  def send[T](packages: List[T], index: String, docType: String)
             (implicit format: RootJsonFormat[T]) = {

    val store = s"$index/$docType"

    def sendPack(p: T) = {

      Thread.sleep(100)

      val message = Marshal(p)
        .to[RequestEntity]

      message.flatMap { msg =>

        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = store,
          entity = msg
        )

        println(s"request: $request")

        val responseFuture =
          Source
            .single(request)
            .via(connectionFlow)
            .runWith(Sink.head)

        responseFuture map { res =>
          res.status match {
            case Created =>
              println("Created")
              println(res.entity)
            case _ =>
              println("ERROR")
              println(res)
          }
        }
      }
    }

    packages.foreach(sendPack)
  }
}
