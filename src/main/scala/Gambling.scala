import java.time.ZonedDateTime
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scala.util.Random

sealed trait GamblingActions

case class Gamble(amount: BigInt) extends GamblingActions

case class PayBack(amount: BigInt) extends GamblingActions

case class Borrow(amount: BigInt) extends GamblingActions

class Bank extends Actor {

  var counter = 0

  override def receive: Receive = {
    case Gamble(amount) =>

      counter = counter + 1
      println(s"---- gambling no: $counter")
      Thread.sleep(200)

      val win = Random.nextBoolean
      val paybackAmount: BigInt = if (win) 2 * amount else 0

      sender ! PayBack(paybackAmount)

    case Borrow(amount) =>
      sender ! Borrow(amount)
    case PayBack(amount) =>
      sender ! Gamble(1)
  }
}

class Gamer(name: String, cash: BigInt, bank: ActorRef) extends Actor {

  case class GamblingState(
                            user: String,
                            amount: BigInt,
                            borrowed: BigInt,
                            actualGamblingAmount: BigInt,
                            timestamp: String
                          )

  def logState() = {
    import spray.json.DefaultJsonProtocol._
    implicit val format = jsonFormat5(GamblingState)

    state = state.copy(timestamp = ZonedDateTime.now().toLocalDateTime.toString)
    Uploader.send(List(state), "gambling", "state")
    KafkaProducer.send(state)
    println(state)
  }

  var state = GamblingState(name, cash, 0, 0, ZonedDateTime.now().toLocalDateTime.toString)

  override def receive: Receive = {

    case PayBack(pbAmount) =>
      state = state.copy(amount = pbAmount + state.amount)

      val win = pbAmount != 0
      if (win) {

        val canPayBackBorrow = state.borrowed > 0 && (
          state.amount > cash + state.borrowed
          )
        if (canPayBackBorrow) {
          val borrowed = state.borrowed
          state = state.copy(amount = state.amount - state.borrowed, borrowed = 0)

          logState()
          bank ! PayBack(borrowed)
        } else {
          logState()
          self ! Gamble(1)
        }
      } else {
        state = state.copy(actualGamblingAmount = 2 * state.actualGamblingAmount)
        if (state.actualGamblingAmount < state.amount) {

          logState()
          self ! Gamble(state.actualGamblingAmount)
        } else {

          logState()
          bank ! Borrow(state.actualGamblingAmount - state.amount)
        }
      }

    case Gamble(pay) =>
      state = state.copy(actualGamblingAmount = pay, amount = state.amount - pay)

      logState()
      bank ! Gamble(pay)

    case Borrow(amount) =>
      state = state.copy(amount = state.amount + amount, borrowed = state.borrowed + amount)

      logState()
      self ! Gamble(state.actualGamblingAmount)
  }
}


object GamblingMain extends App {

  val system = ActorSystem("gambling")

  val bank = system.actorOf(Props[Bank], "bank")

  def actor(name: String, cash: BigInt) =
    system.actorOf(Props(classOf[Gamer], name, cash, bank), name)

  for {
    userId <- 1 to 5
  } yield {
    val user = actor("user_" + userId, BigInt(100) * userId)
    user ! Gamble(1)
  }

}


