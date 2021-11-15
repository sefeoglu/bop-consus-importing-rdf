# piveau consus importing rdf helm chart

## Install

```shell
$ helm repo add paca https://paca.fokus.fraunhofer.de/repository/helm-charts
$ helm install <release-name> paca/piveau-consus-importing-rdf
```

You can adjust your release by using the `--set parameter=value` parameter:

```shell
$ helm install --set monitoring=true <release-name> paca/piveau-consus-importing-rdf 
```

Or by passing a value file, e.g.:

```shell
$ helm install -f my-values.yaml <release-name> paca/piveau-consus-importing-rdf
```

## Upgrade

```shell
$ helm upgrade <release-name> paca/piveau-consus-importing-rdf
```

## Configuration

| Parameter                 | Description                                                                                                                            | Default                               |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
|              |                                                                                                        |  |
