# ChangeLog

**Added:**
* Pre-processing to fix malformed URIRefs
* Configuration for pre-processing

**Fixed:**
* Expect only 200er responses as success and parse them

## [1.0.0](https://gitlab.fokus.fraunhofer.de/viaduct/piveau-importing-rdf/tags/1.0.0) (2019-11-08)

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

**Removed:**

**Fixed:**
* Hydra is now HydraPaging
* Update all dependencies
* Force snapshot update when building package in gitlab ci

## [0.1.0](https://gitlab.fokus.fraunhofer.de/viaduct/piveau-importing-rdf/tags/0.1.0) (2019-05-17)

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

## [0.0.2](https://gitlab.fokus.fraunhofer.de/viaduct/piveau-importing-rdf/tags/0.0.2) (2019-05-11)

**Changed:**
* Use new findIdentifier with configuration from pipe

**Removed:**
* fetchIdentifiers + 'mode' configuration parameter

## [0.0.1](https://gitlab.fokus.fraunhofer.de/viaduct/piveau-importing-rdf/tags/0.0.1) (2019-05-03)
Initial release