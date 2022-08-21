import sbt._

object DependencyVersions {
  val scala2p13Version = "2.13.8"
  val scala2p12Version = "2.12.16"
  val scala2p11Version = "2.11.12"

  val catsEffectVersion = "3.3.14"
  val catsFreeVersion   = "2.8.0"

  val catsEffectTestingVersion = "1.4.0"
}

object Dependencies {
  import DependencyVersions._

  private val catsEffect: ModuleID =
    "org.typelevel" %% "cats-effect" % catsEffectVersion

  private val catsEffectTesting: ModuleID =
    "org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % "test"

  private val catsFree: ModuleID =
    "org.typelevel" %% "cats-free" % catsFreeVersion

  val bengalStm: Seq[ModuleID] =
    Seq(
      catsEffect,
      catsFree,
      catsEffectTesting
    )
}
