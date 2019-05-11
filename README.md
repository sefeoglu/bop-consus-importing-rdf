# piveau importing rdf
Microservice for importing from source and feeding a piveau pipe.

The service is based on the [pipe-connector](https://gitlab.fokus.fraunhofer.de/viaduct/pipe-connector) library. Any configuration applicable for the pipe-connector can also be used for this service.

## Table of Contents
1. [Pipe Configuration](#pipe-configuration)
1. [Data Info Object](#data-info-object)
1. [Build](#build)
1. [Run](#run)
1. [Docker](#docker)
1. [License](#license)

## Pipe Configuration

_mandatory_

* `address` 

    Address of the source

_optional_

* `removePrefix`

    Try to remove prefix from uriRefs and take everything after last `/` as identifier. Default is `false`.

* `precedenceUriRef`

    Give uriRef precedence over `dct:identifier` as identifier. Default is `false`.

* `inputFormat` 
    
    Mimetype to read from source. Overwrites header `Content-Type`

* `outputFormat` 
    
    Mimetype to use for payload. Default is `application/n-triples`

    Possible output formats:

     * `application/rdf+xml`
     * `application/n-triples`
     * `application/ld+json`
     * `application/trig`
     * `text/turtle`
     * `text/n3`

* `brokenHydra`
    
    Some sources use a wrong urls in hydra information for paging. 
If set to true the service will try to handle such broken hydra information. Default is `false`
 
## Data Info Object

* `total` 

    Total number of datasets

* `counter` 

    The number of this dataset

* `identifier` 

    The unique identifier in the source of this dataset

* `hash` 

    The hash value calculated at the source

## Build
Requirements:
 * Git
 * Maven
 * Java

```bash
$ git clone https://gitlab.fokus.fraunhofer.de/viaduct/piveau-importing-rdf.git
$ cd piveau-importing-rdf
$ mvn package
```

## Run

```bash
$ java -jar target/piveau-importing-rdf-far.jar
```

## Docker

Build docker image:

```bash
$ docker build -t piveau/piveau-importing-rdf .
```

Run docker image:

```^bash
$ docker run -it -p 8080:8080 piveau/piveau-importing-rdf
```

## License

[Apache License, Version 2.0](LICENSE.md)
