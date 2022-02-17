<img align="right" src="https://github.com/gvonness/bengal-stm/blob/main/docs/assets/logo.svg?raw=true" height="200px" style="padding-left: 20px"/>

# Bengal STM
![Build Status](https://github.com/gvonness/bengal-stm/actions/workflows/build.yml/badge.svg)

Bengal STM is a library for writing composable concurrency operations based on in-memory transactions. The library itself handles all aspects of concurrency management including locking, retries, semantic blocking and optimised transaction scheduling. Generally, STM is a higher-level concurrency abstraction that provides a safe, efficient and composable alternate to locks, mutexes, etc. 

There are two aspects that differentiate Bengal from other STM implementations:
* ***Bengal runtime scheduler***: The Bengal runtime uses a custom scheduler that is not blindly optimistic. Transactions are scheduled based on a fast, static analysis of the transaction variable domain to lower the chances of transactions needing to be retried. This ensures _consistent_ performance, even for highly-contentious transactional variables/maps.
* ***Transactional Maps***: In addition to transactional variables, the implementation includes performant transactional maps as a core API data structure. This data structure provides performance benefits above wrapping an entire map in a transactional variable.

---

## Theory

As there are already many solid references on STM, I will not dive into STM theory here. However, I do highly recommend the writeup in [Beautiful Concurrency](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/beautiful.pdf) for those interested in learning about this concurrency pattern.

---

## API

Example | Description | Type Signature | Notes
:--- | --- | :--- | :---
`STM.runtime[IO].unsaferunsync()` | Creates a runtime whose transaction results can be lifted into a container `F[_]` via `commit` | `def runtime[F[_]: Concurrent: Temporal](retryMaxWait: FiniteDuration): F[STM[F]]` <br/>or<br/> `def runtime[F[_]: Concurrent: Temporal]: F[STM[F]]` (default `retryMaxWait`) | `retryMaxWait` is a backstop max amount of time to wait before retrying a transaction. <br/><br/>Default: `FiniteDuration(Long.MaxValue, NANOSECONDS)` <br/><br/> It is _not_ recommended to make this a small value (i.e. making retries effectively based on polling).
`txnVar.get.commit` | Commits a transaction and lifts the result into `F[_]` | `def commit: F[V]` | 
`TxnVar.of[List[Int]](List())` | Creates a transactional variable | ```def of[T](value: T): F[TxnVar[T]]``` 
`TxnVarMap.of[String, Int](Map())` | Creates a transactional map | ```of[K, V](valueMap: Map[K, V]): F[TxnVarMap[K, V]]``` 
`txnVar.get` | Retrieves value of transactional variable | ```def get: Txn[V]``` |
`txnVarMap.get` | Retrieves an immutable map (i.e. a view) representing transactional map state | ```def get: Txn[Map[K, V]]``` | Performance-wise it is better to retrieve individual keys instead of acquiring the entire map
`txnVarMap.get("David")` | Retrieves optional value depending on whether key exists in the map | ```def get(key: K): Txn[Option[V]]``` | Will raise an error if the key is never created (previously or current transaction). A `None` is returned if the value has been deleted in the current transaction.
`txnVar.set(100)` | Sets the value of transactional variable | ``` def set(newValue: V): Txn[Unit]``` 
`txnVarMap.set(Map("David" -> 100))` | Uses an immutable map to set the transactional map state | ```def set(newValueMap: Map[K, V]): Txn[Unit]``` | Performance-wise it is better to set individual keys instead of setting the entire map in this manner. <br/><br/>This operation will create/delete key-values as needed to update the state of the map.
`txnVarMap.set("David", 100)` | Upserts the key-value into the transactional map | ```def set(key: K, newValue: V): Txn[Unit]``` | Will create the key-value in the transactional map, if the key was not present
`txnVar.modify(_ + 5)` | Modifies the value of a transactional variable | ```def modify(f: V => V): Txn[Unit]```
`txnVarMap.modify("David", _ + 20)` | Modifies the value in a transactional map for a given key | ```def modify(key: K, f: V => V): Txn[Unit]``` | Will throw an error if the `key` is not present in the map (or has been deleted in the current transaction)
`txnVarMap.modify(_.map(i => i._1 -> i._2*2))` | Modifies all the values in the map | ```def modify(f: Map[K, V] => Map[K, V]): Txn[Unit]``` | Transform can create/delete entries.<br/><br/>Again, for performance it is better to work with individual key-value pairs instead of manipulating map views
`txnVarMap.remove("David")` | Removes a key-value from the transactional map | ```def remove(key: K): Txn[Unit]``` | Will throw an error if the key doesn't actually exist in the map (to be consistent with `get` behaviour)
`pure(10)` | Lifts a value into a transactional monad | ```def pure[V](value: V): Txn[V]``` |
`abort(new RuntimeException("foo"))` | Aborts the current transaction | ```def abort(ex: Throwable): Txn[Unit]``` | Variables/Maps changes in the transaction will not be changed if the transaction is aborted
`txn.handleErrorWith(_ => pure("bar"))` | Absorbs an error/abort and remaps to another transaction (of the same wrapped type) | ```def handleErrorWith(f: Throwable => Txn[V]): Txn[V]``` |
`waitFor(value > 10)` | Semantically blocks a transaction until a condition is met | ```def waitFor(predicate: => Boolean): Txn[Unit]``` | Blocking is only semantic (i.e. not locking up a thread while waiting)<br/><br/>This is implemented via retries that are initiated via variable/map updates. One can specify the `retryMaxWait` to facilitate backstop polling for these retries, but this is _not_ recommended (i.e. indicates side-effects are impacting predicate)

### Example

```scala
import cats.effect.{IO, IOApp}

import scala.concurrent.duration._

import ai.entrolution.bengal.stm._

object Main extends IOApp.Simple {

  override def run: IO[Unit] = STM.runtime[IO].flatMap(run)

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

---
---

## PR FAQ
### What is a PR FAQ?
Take a look at [the Medium article on PR FAQs](https://medium.com/agileinsider/press-releases-for-product-managers-everything-you-need-to-know-942485961e31) for a good overview of the concept. I have taken some liberties with the formatting, but I generally like the concept of a living FAQ to help introduce products.

### Why another STM implementation?
I found that blindly optimistic execution strategies led to very poor performance of STM in a number of production scenarios. The situation could only be remedied by sequentially executing queued transactions within a given runtime. I.e. the transactional nature of STM became moot, as I was essentially reducing concurrent execution back down to sequential execution. Thus, I decided to build an STM backed by a scheduler that was more conducive to handling high-contention scenarios, while still being genuinely concurrent. 

Beyond the scheduler, I also wanted to explore adding `Map` as a fundamental transactional data structure to analogise the concept of an index in a DB. This presents some interesting challenges with scheduling around structural (i.e. the 'CRD' in 'CRUD') updates to the map itself, but it's a data structure I just found to be very useful in transactional contexts.

### Why not just contribute to another project?
Indeed, [cats-stm](https://timwspence.github.io/cats-stm/) already exists and provides a nice STM implementation for Cats Effect (Try it!).

Given the requirements I had for the transaction scheduler, I decided that the underpinning implementation would be quite different than cats-stm. In particular, this implementation is based on [Free Monads](https://typelevel.org/cats/datatypes/freemonad.html) that use different interpreters for static analysis and building the transactional log. 

Also, while APIs are quite similar, there are some differences between Bengal and cats-stm. For example, cats-stm has a way to bypass retries with `orElse`, which is not something present in Bengal (this is an intentional design decision). Also, initialisation of `TxnVar` and `TxnVarMap` happen outside the context of the `Txn[_]` monad.

### Why isn't there a way to bypass `waitFor`?
I wanted `waitFor` to have a clear semantic delineation from an `if` statement in the monadic construction. While there is arguably a missed opportunity to define a canonical Semigroup via such a bypass, I have opted for a simpler API (for the time being).

### Why 'Bengal'?
Bengals are a very playful and active cat breed. I figured the name worked for something built on Cats ;).