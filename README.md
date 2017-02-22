# Amadou - Ignite your Spark ETL jobs
[![Build Status]][Travis]
[![Latest version]][Bintray]

  [Build Status]: https://travis-ci.org/ypg-data/amadou.svg?branch=master
  [Travis]: https://travis-ci.org/ypg-data/amadou
  [Latest version]: https://api.bintray.com/packages/ypg-data/maven/amadou-core/images/download.svg
  [Bintray]: https://bintray.com/ypg-data/maven/amadou-core/_latestVersion

> Amadou was a precious resource to ancient people, allowing them to start a
> fire by catching sparks from flint struck against iron pyrites.
> -- [Wikipedia]

 [Wikipedia]: https://en.wikipedia.org/wiki/Amadou

## Getting Started

Add the following to your `build.sbt`:

```sbt
resolvers += Resolver.bintrayRepo("ypg-data", "maven")
libraryDependencies += "com.mediative" %% "amadou-core" % "0.1.1"
```

See the [TestEtl] job to get an idea of what the library provides.

 [TestEtl]: core/src/test/scala/com.mediative.amadou/test/TestEtl.scala

## Documentation

 - [Scaladoc](https://ypg-data.github.io/amadou/api/#com.mediative.amadou.package)

## Building and Testing

This library is built with sbt, which needs to be installed. Run the following command from the project root, to build and run all test:

    $ sbt compile test

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute.

## Releasing

To release version `x.y.z` run:

    $ sbt release -Dversion=x.y.z

This will run the tests, create a tag and publishing JARs and API docs.

## License

Copyright 2017 Mediative

Licensed under the Apache License, Version 2.0. See LICENSE file for terms and
conditions for use, reproduction, and distribution.
