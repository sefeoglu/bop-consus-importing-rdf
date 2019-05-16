# ChangeLog

## Unreleased

**Added:**
* `catalogue` read from configuration and pass it to the info object
* Environment `PIVEAU_IMPORTING_SEND_LIST_DELAY` for a configurable delay
* `sendListData` pipe configuration option

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