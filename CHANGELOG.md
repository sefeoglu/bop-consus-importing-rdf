# ChangeLog

## Unreleased

## 1.7.2 (2022-12-24)

**Changed:**
* Load buildInfo once for faster health check

## 1.7.1 (2022-12-15)

**Removed:**
* Circuit breaker for download requests

## 1.7.0 (2022-08-31)

**Added:**
* Support for gzip compressed files (issue #2177)

**Fixed:**
* Temp file name in case of not preprocessing

## 1.6.3 (2022-06-28)

**Fixed:**
* Connector and body handler

## 1.6.2 (2022-06-27)

**Changed:**
* Lib updates

## 1.6.1 (2021-11-16)

**Fixed:**
* Ensure deletion of temp file

## 1.6.0 (2021-10-06)

**Added:**
* Filter for referencing the catalogue from dataset

## 1.5.1 (2021-10-01)

**Removed:**
* GlobalScope coroutines

**Changed:**
* Handle empty ids correctly

**Added:**
* Warning when duplicates detected

## 1.5.0 (2021-06-05)

**Changed:**
* Increased timeout for requests and circuit breaker

**Added:**
* Reintroduce a pulse value for slower emitting datasets

**Fixed:**
* Temp file creation mode
* Log output of not identifiable dataset
* Total count for non hydra paged sources

## 1.4.1 (2021-02-23)

**Added:**
* Accept in pipe segment config

## 1.4.0 (2021-01-26)

**Changed:**
* Switched to Vert.x 4.0.0

## 1.3.0 (2020-11-09)

**Changed:**
* Log with datum
* Processing of content via streams and temp files

## 1.2.2 (2020-09-21)

**Fixed:**
* preProcessing

## 1.2.1 (2020-09-20)

**Fixed:**
* brokenHydra calculation

**Changed:**
* Error message for not rdf content

**Added:**
* Temp file currently not supported exception
 
## 1.2.0 (2020-09-19)

**Changed:**
* Switch to kotlin and kotlin flows

**Added:**
* Use new pre-processing for JSON-LD

## 1.1.2 (2020-07-13)

**Fixed:**
* Handle missing content type
 
## 1.1.1 (2020-06-18)

**Changed:**
* Pipe startTime

## 1.1.0 (2020-04-15)

**Fixed:**
* Use fixed pre-processing from piveau utils

**Added:**
* Possibility to download into temp file
  
## 1.0.5 (2020-03-05)

**Added:**
* Configurable deletion phase

## 1.0.4 (2020-02-28)

**Changed:**
* Update connector lib

## 1.0.3 (2020-01-24)

**Changed:**
* Update connector and piveau-utils for improved pre-processing
* License

## 1.0.2 (2019-11-28)

**Fixed:**
* Unsupported mime types when pre-processing content

## 1.0.1 (2019-11-17)

**Added:**
* Pre-processing to fix malformed URIRefs
* Configuration for pre-processing

**Fixed:**
* Expect only 200er responses as success and parse them

## 1.0.0 (2019-11-08)

**Added:**
* buildInfo.json for build info via `/health` path
* config.schema.json
* `PIVEAU_LOG_LEVEL` in logback.xml
* Pipe log debug output of data content
* `sendHash` pipe configuration parameter
* `sendHash` to config schema
* Configuration change listener
   
**Changed:**
* `PIVEAU_` prefix to logstash configuration environment variables
* Upgrade gitlab ci maven image
* Use jena utils for canonical hash calculation
* Optional canonical hash attachment on dataInfo
* Requires now latest LTS Java 11
* Docker base image to openjdk:11-jre

**Fixed:**
* Hydra is now HydraPaging
* Update all dependencies
* Force snapshot update when building package in gitlab ci

## 0.1.0 (2019-05-17)

**Added:**
* `catalogue` read from configuration and pass it to the info object
* Environment `PIVEAU_IMPORTING_SEND_LIST_DELAY` for a configurable delay
* `sendListDelay` pipe configuration option

**Changed:**
* Readme
* Default output format to `application/n-triples`

**Removed:**
* `mode` configuration and fetchIdentifier

**Fixed:**
* Use address as baseUri for reading model

## 0.0.2 (2019-05-11)

**Changed:**
* Use new findIdentifier with configuration from pipe

**Removed:**
* fetchIdentifiers + 'mode' configuration parameter

## 0.0.1 (2019-05-03)

Initial release