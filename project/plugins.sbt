logLevel := Level.Warn

resolvers ++= Seq(
  "twitter-repo" at "https://maven.twttr.com",
  "Oracle Maven2 Repo" at "http://download.oracle.com/maven"
)

addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.11.0")