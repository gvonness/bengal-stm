import sbt._

object DependencyVersions {
  val scala2p13Version = "2.13.8"

  val catsEffectVersion = "3.4.8"
  val catsFreeVersion   = "2.9.0"

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
