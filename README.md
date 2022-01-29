<img align="right" src="https://github.com/gvonness/bengal-stm/blob/main/docs/assets/logo.svg?raw=true" height="200px" style="padding-left: 20px"/>

# Bengal STM
![Build Status](https://github.com/gvonness/bengal-stm/actions/workflows/build.yml/badge.svg)

Bengal STM is a library for writing composable concurrency operations based on in-memory transactions. The library itself handles locking, retries, semantic blocking and optimised scheduling of transactions. Generally, STM is a higher-level concurrency abstraction that provides a safe, efficient and composable alternate to locks, mutexes, etc. 

There are two aspects that differentiate Bengal from other STM implementations:
* The Bengal runtime uses a custom scheduler that is not blindly optimistic. Transactions are scheduled based on a fast, static analysis of the transaction variable domain to lower the chances of transactions needing to be retried.
* In addition to transactional variables, the implementation includes performant transactional maps as a core API data structure

### Example

```scala
import cats.effect.{IO, IOApp}

import scala.concurrent.duration._

import ai.entrolution.bengal.stm._

object Main extends IOApp.Simple {

  override def run: IO[Unit] = STM.runtime[IO].flatMap(run(_))

  def run(stm: STM[IO]): IO[Unit] = {
    import stm._

    def createAccount(name: String,
                      initialBalance: Int,
                      accounts: TxnVarMap[String, Int]): IO[Unit] =
      accounts.set(name, initialBalance).commit

    def transferFunds(accounts: TxnVarMap[String, Int], 
                      bankOpen: TxnVar[Boolean], 
                      to: String, 
                      from: String, 
                      amount: Int): IO[Unit] =
      (for {
        balance    <- accounts.get(from)
        isBankOpen <- bankOpen.get
        _          <- stm.waitFor(isBankOpen)
        _          <- stm.waitFor(balance.exists(_ >= amount))
        _          <- accounts.modify(from, _ - amount)
        _          <- accounts.modify(to, _ + amount)
      } yield ()).commit

    def openBank(bankOpen: TxnVar[Boolean]): IO[Unit] =
      for {
        _ <- IO.sleep(1000.millis)
        _ <- IO(println("Bank Open!"))
        _ <- bankOpen.set(true).commit
      } yield ()

    def printAccounts(accounts: TxnVarMap[String, Int]): IO[Unit] =
      for {
        accounts <- accounts.get.commit
        _ <- IO {
          accounts.toList.foreach { acc =>
            println(s"${acc._1}: ${acc._2}")
          }
        }
      } yield ()

    for {
      bankOpen <- TxnVar.of[Boolean](false)
      accounts <- TxnVarMap.of[String, Int](Map())
      _        <- createAccount("David", 100, accounts)
      _        <- createAccount("Sasha", 0, accounts)
      _        <- printAccounts(accounts)
      _        <- openBank(bankOpen).start
      _        <- transferFunds(accounts, bankOpen, "Sasha", "David", 100)
      _        <- printAccounts(accounts)
    } yield ()
  }
}
```

### More Information
[Beautiful Concurrency](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/beautiful.pdf) contains a nice writeup of the STM concept.
