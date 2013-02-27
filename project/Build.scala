import sbt._
import Keys._

object RaikuBuild extends Build {

  override lazy val settings = super.settings ++
    Seq(
      name := "raiku",
      version := "0.3.1-SNAPSHOT",
      organization := "nl.gideondk",
      parallelExecution in Test := false,
      scalaVersion := "2.10.0",
      crossScalaVersions := Seq("2.10.0"),
      
      publishTo := Some(Resolver.file("file", new File("/Users/gideondk/Development/gideondk-mvn-repo"))),

      resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "Typesafe Repository (snapshots)" at "http://repo.typesafe.com/typesafe/snapshots/",
                  "gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master",
                  
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases",

                  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
                  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
                  
                  "spray repo" at "http://repo.spray.io"))

  val appDependencies = Seq(
      "org.scalaz" %% "scalaz-core" % "7.0.0-M7",
      "org.scalaz" %% "scalaz-effect" % "7.0.0-M7",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "io.spray" %%  "spray-json" % "1.2.3",
      "com.typesafe.akka" % "akka-actor_2.10" % "2.2-SNAPSHOT",
      "play" %% "play-iteratees" % "2.1.0",
      "com.chuusai" % "shapeless_2.10.0" % "1.2.4",
      "org.specs2" %% "specs2" % "1.13"
  )

  lazy val root = Project(id = "raiku",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= appDependencies
    ) ++ Format.settings
  )
}

object Format {

  import com.typesafe.sbt.SbtScalariform._

  lazy val settings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := formattingPreferences
  )

  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference(AlignParameters, true).
      setPreference(AlignSingleLineCaseStatements, true).
      setPreference(CompactControlReadability, true).
      setPreference(CompactStringConcatenation, true).
      setPreference(DoubleIndentClassDeclaration, true).
      setPreference(IndentLocalDefs, true).
      setPreference(IndentPackageBlocks, true).
      setPreference(IndentSpaces, 2).
      setPreference(MultilineScaladocCommentsStartOnFirstLine, true).
      setPreference(PreserveSpaceBeforeArguments, false).
      setPreference(PreserveDanglingCloseParenthesis, false).
      setPreference(RewriteArrowSymbols, true).
      setPreference(SpaceBeforeColon, false).
      setPreference(SpaceInsideBrackets, false).
      setPreference(SpacesWithinPatternBinders, true)
  }
}