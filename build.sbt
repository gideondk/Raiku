name := "raiku"

organization := "nl.gideondk"

version := "0.1"

scalaVersion := "2.10.0-RC2"

crossScalaVersions := Seq("2.10.0-RC2")

resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master",
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases",
                  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
                  "repo.codahale.com" at "http://repo.codahale.com",
                  "spray repo" at "http://repo.spray.io"
)

libraryDependencies ++= Seq(		          
      "org.scalaz" % "scalaz-core_2.10.0-RC2" % "7.0.0-M4" withSources(),
      "org.scalaz" % "scalaz-effect_2.10.0-RC2" % "7.0.0-M4" withSources(),
      "com.google.protobuf" % "protobuf-java" % "2.4.1" withSources(),
      "org.specs2" % "specs2_2.10.0-RC2" % "1.12.2",
      "io.spray" %%  "spray-json" % "1.2.2" cross CrossVersion.full,
      "com.typesafe.akka" %% "akka-actor" % "2.1.0-RC2" cross CrossVersion.full
)

logBuffered := false
