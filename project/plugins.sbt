logLevel := Level.Warn

addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.18.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.8")
libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.0-pre5"