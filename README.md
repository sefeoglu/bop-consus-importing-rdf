# piveau importing rdf
Microservice for importing from source and feeding a piveau pipe.

## Pipe config parameters

_mandatory_

`address` Address of the source

_optional_

`outputFormat` Mimetype to use for payload. Default is `application/n-triples`

Possible output formats:

 * `application/rdf+xml`
 * `application/n-triples`
 * `application/ld+json`
 * `application/trig`
 * `text/turtle`
 * `text/n3`
    
## Data info for payload

`total` Total number of datasets

`counter` The number of this dataset

`identifier` The unique identifier in the source of this dataset
