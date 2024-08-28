# Chart Details

## Stateful Set

### ConfigMap Sorting

The configurationOverrides variable, represented as a map, supplies the
parameters for the configmap. The configMap is used to populate the KAFKA_*
environment variables in the StatefulSet.

The go templating language does not guarantee order when traversing a map.
Due to the random order of the items in the configMap, the generated Kubernetes
manifest will be different each time. This causes Helm upgrade to trigger a
rolling upgrade of the StatefulSet even though there is no change in the chart.
For more information please check the link here <https://cc-jira.rnd.ki.sw.ericsson.se/browse/ADPPRG-103>

It is therefore required to sort configurationOverrides to avoid the
unnecessary updates.

### annotation checksum/config

Often times configmaps are injected as configuration files in containers.
Depending on the application a restart may be required to be updated with a
subsequent helm upgrade, but if the StatefulSet spec didn't change the
application keeps running with the old configuration resulting in an
inconsistent deployment.

The sha256sum function is used to ensure a StatefulSet's annotation section
is updated if another file changes:

```yaml
kind: StatefulSet
spec:
  template:
    metadata:
      annotations:
        checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
```

The checksum/config field addresses that issue by generating a checksum using
configurationOverrides.

If there is a value change only, the checksum will be different and therefore a
rolling upgrade will be triggered by Helm.

Due to the issue described in *ConfigMap Sorting*, it is required to create a
sorted string out of configurationOverrides.

### KAFKA_ADVERTISED_LISTENERS

The property sets the advertised.listeners property in the Kafka brokers to the
FQDN of the pod.

## Monitoring

This chart uses a jmx-exporter side car container to convert JMX metrics into a
PM Server (Prometheus) compatible format.

Note: The jmx-exporter container includes a list of predefined rule sets.

The *default* rule set will collect any metric produced by the Message Bus KF
container without renaming. This includes JVM metrics.

The *kafka-0-8-2* rule set limits the set of available metrics to Kafka
specific ones. It also renames a few Kafka metrics. It omits JVM metrics
entirely.
