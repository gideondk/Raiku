name := "raiku"

organization := "nl.gideondk"

version := "0.1"

scalaVersion := "2.10.0-RC2"

crossScalaVersions := Seq("2.10.0-RC2")

parallelExecution in Test := false

resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master",
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases",
                  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
                  "repo.codahale.com" at "http://repo.codahale.com",
                  "spray repo" at "http://repo.spray.io"
)

libraryDependencies ++= Seq(		          
//    "org.specs2" %% "specs2" % "1.9" % "test" withSources(),
//    "commons-pool" % "commons-pool" % "1.5.6" withSources(),
      "org.scalaz" % "scalaz-core_2.10.0-RC2" % "7.0.0-M4" withSources(),
      "org.scalaz" % "scalaz-effect_2.10.0-RC2" % "7.0.0-M4" withSources(),
      "com.google.protobuf" % "protobuf-java" % "2.4.1" withSources(),
      "org.specs2" % "specs2_2.10.0-RC2" % "1.12.2",
//    "com.stackmob" % "scaliak_2.9.2" % "0.3-FUTURES",
//    "net.debasishg" % "sjsonapp_2.9.2" % "0.1.1-scalaz-seven",
//    "org.scalatest" %% "scalatest" % "1.7.2" % "test",
//    "io.backchat.jerkson" % "jerkson_2.9.2" % "0.7.0",
//    "net.databinder.dispatch" %% "dispatch-core" % "0.9.4",
//    "io.spray" %%  "spray-json" % "1.2.2",
//    "io.spray" % "spray-client" % "1.0-M5" 
      "io.spray" %%  "spray-json" % "1.2.2" cross CrossVersion.full,
      "com.typesafe.akka" %% "akka-actor" % "2.1.0-RC2" cross CrossVersion.full
)

logBuffered := false
