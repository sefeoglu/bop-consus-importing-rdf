# piveau importing rdf
Microservice for importing from source and feeding a pipe.

The service is based on the piveau-pipe-connector library. Any configuration applicable for the pipe-connector can also be used for this service.

## Table of Contents
1. [Build](#build)
1. [Run](#run)
1. [Docker](#docker)
1. [Configuration](#configuration)
    1. [Pipe](#pipe)
    1. [Data Info Object](#data-info-object)
    1. [Environment](#environment)
    1. [Logging](#logging)
1. [License](#license)

## Build
Requirements:
 * Git
 * Maven 3
 * Java 17

```bash
$ git clone <gitrepouri>
$ cd piveau-importing-rdf
$ mvn package
```

## Run

```bash
$ java -jar target/importing-rdf.jar
```

## Docker

Build docker image:

```bash
$ docker build -t piveau/piveau-importing-rdf .
```

Run docker image:

```bash
$ docker run -it -p 8080:8080 piveau/piveau-importing-rdf
```

## Configuration

### Pipe

_mandatory_

* `address` 

    Address of the source

* `catalogue`

    The id of the target catalogue

_optional_

* `removePrefix`

    Try to remove prefix from uriRefs and take everything after last `/` as identifier. Default is `false`.

* `precedenceUriRef`

    Give uriRef precedence over `dct:identifier` as identifier. Default is `false`.

* `inputFormat` 
    
    Mimetype to read from source. Takes precedence over header `Content-Type`

* `outputFormat` 
    
    Mimetype to use for payload. Default is `application/n-triples`

    Possible output formats are:

     * `application/rdf+xml`
     * `application/n-triples`
     * `application/ld+json`
     * `application/trig`
     * `text/turtle`
     * `text/n3`

* `brokenHydra`
    
    Some sources use a wrong urls in hydra information for paging. If set to true the service will try to handle such broken hydra information. Default is `false`
 
* `sendListDelay`

    The delay in milliseconds before the list of identifiers is send. Take precedence over service configuration (see `PVEAU_IMPORTING_SEND_LIST_DELAY`)

* `preProcessing`

    Any pre-processing (e.g. URI encoding fixes) should take place. Overwrites importers configuration (see `PIVEAU_IMPORTING_PREPROCESSING`)

### Data Info Object

* `total` 

    Total number of datasets

* `counter` 

    The number of this dataset

* `identifier` 

    The unique identifier in the source of this dataset

* `catalogue`

    The id of the target catalogue

### Environment
See also piveau-pipe-connector

| Variable                           | Description                                                                     | Default Value |
|:-----------------------------------|:--------------------------------------------------------------------------------|:--------------|
| `PIVEAU_IMPORTING_SEND_LIST_DELAY` | The delay in millisecond for sending the identifier list after the last dataset | `8000`        |
| `PIVEAU_IMPORTING_PREPROCESSING`   | Generally do some pre-processing, e.g. to fix URI encoding issues               | `false`       |
| `PIVEAU_DEFAULT_PULSE`             | Emitting datasets delay in milliseconds                                         | `15`          |

### Logging
See [logback](https://logback.qos.ch/documentation.html) documentation for more details

| Variable                   | Description                                       | Default Value                         |
|:---------------------------|:--------------------------------------------------|:--------------------------------------|
| `PIVEAU_PIPE_LOG_APPENDER` | Configures the log appender for the pipe context  | `STDOUT`                              |
| `PIVEAU_LOGSTASH_HOST`     | The host of the logstash service                  | `logstash`                            |
| `PIVEAU_LOGSTASH_PORT`     | The port the logstash service is running          | `5044`                                |
| `PIVEAU_PIPE_LOG_PATH`     | Path to the file for the file appender            | `logs/piveau-pipe.%d{yyyy-MM-dd}.log` |
| `PIVEAU_PIPE_LOG_LEVEL`    | The log level for the pipe context                | `INFO`                                |
| `PIVEAU_LOG_LEVEL`         | The general log level for the `io.piveau` package | `INFO`                                |

## License

[Apache License, Version 2.0](LICENSE.md)
