resolvers += Resolver.url("mediative:sbt-plugins", url("https://dl.bintray.com/mediative/sbt-plugins"))(Resolver.ivyStylePatterns)
resolvers += Resolver.url("fonseca:sbt-plugins", url("https://dl.bintray.com/fonseca/sbt-plugins"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.mediative.sbt" % "sbt-mediative-core" % "0.5.1")
addSbtPlugin("com.mediative.sbt" % "sbt-mediative-oss" % "0.5.1")
addSbtPlugin("com.mediative.sbt" % "sbt-mediative-devops" % "0.5.1")
addSbtPlugin("com.github.tkawachi" % "sbt-doctest" % "0.4.1")
