name := "tstreams-transaction-server"

version := "1.1.3-SNAPSHOT"

scalaVersion := "2.12.1"

pomExtra :=
  <scm>
    <url>git@github.com:bwsw/tstreams-transaction-server.git</url>
    <connection>scm:git@github.com:bwsw/tstreams-transaction-server.git</connection>
  </scm>
    <developers>
      <developer>
        <id>bitworks</id>
        <name>Bitworks Software, Ltd.</name>
        <url>http://bitworks.software/</url>
      </developer>
    </developers>

fork in run := true
fork in Test := true
licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("http://t-streams.com/"))
pomIncludeRepository := { _ => false }
scalacOptions += "-feature"
scalacOptions += "-deprecation"
parallelExecution in Test := false
organization := "com.bwsw"
publishMavenStyle := true
pomIncludeRepository := { _ => false }

isSnapshot := true
coverageEnabled := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "log4j-1.2.17.jar"}
}

publishArtifact in Test := false
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"


val sroogeGenOutput = "src/main/thrift/gen"
ScroogeSBT.autoImport.scroogeThriftOutputFolder in Compile <<= baseDirectory {
  base => base / sroogeGenOutput
}


ScroogeSBT.autoImport.scroogeBuildOptions in Compile := Seq()
unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"
managedSourceDirectories in Compile += baseDirectory.value / sroogeGenOutput
parallelExecution in Test := false

resolvers ++= Seq(
  "twitter-repo" at "https://maven.twttr.com",
  "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
  "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "Sonatype snapshots OSS" at "https://oss.sonatype.org/content/repositories/snapshots"
)


libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.5",
  "com.twitter" %% "scrooge-core" % "4.14.0",
  "com.twitter" % "libthrift" % "0.5.0-7",
  "org.rocksdb" % "rocksdbjni" % "4.11.2",
  "com.sleepycat" % "je" % "7.0.6",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "io.netty" % "netty-all" % "4.1.7.Final",

//  "com.bwsw" % "journaled-commit-log_2.12" % "1.0.0-SNAPSHOT",

  "org.slf4j" % "slf4j-log4j12" % "1.7.22",

  "org.apache.curator" % "curator-framework" % "2.11.1",
  "org.apache.curator" % "curator-test" % "2.11.1",
  "org.apache.curator" % "curator-recipes" % "2.11.1"
)
