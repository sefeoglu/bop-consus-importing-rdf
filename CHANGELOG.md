# ChangeLog

## Unreleased

**Added:**
* buildInfo.json for build info via `/health` path
* config.schema.json
* Add PIVEAU_LOG_LEVEL in logback.xml
* Add pipe log debug output of data content
 
**Changed:**
* Add `PIVEAU_` prefix to logstash configuration environment variables
* Upgrade to vert.x 3.8.0

**Removed:**

**Fixed:**

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