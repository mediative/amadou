# Amadou - Ignite your Spark ETL jobs

> Amadou was a precious resource to ancient people, allowing them to start a
> fire by catching sparks from flint struck against iron pyrites.
> -- [Wikipedia]

 [Wikipedia]: https://en.wikipedia.org/wiki/Amadou

## Getting Started

See the [TestEtl] job to get an idea of what the library provides.

 [TestEtl]: core/src/test/scala/com.mediative.amadou/test/TestEtl.scala

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
