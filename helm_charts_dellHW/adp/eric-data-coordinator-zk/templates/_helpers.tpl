{{/*
Allow for override of chart name
*/}}
{{- define "eric-data-coordinator-zk.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Allow a fully qualified name for naming kubernetes resources
*/}}
{{- define "eric-data-coordinator-zk.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Client service url
*/}}
{{- define "eric-data-coordinator-zk.clientUrl" -}}
{{ template "eric-data-coordinator-zk.fullname" . }}:{{ .Values.clientPort }}
{{- end -}}

{{/*
Name of the Internal Service which controls the domain of the Data Coordinator ZK ensemble.
*/}}
{{- define "eric-data-coordinator-zk.ensembleService.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- printf "%s-ensemble-service" .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-ensemble-service" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Chart name and version used in chart label.
*/}}
{{- define "eric-data-coordinator-zk.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Product information.
*/}}
{{- define "eric-data-coordinator-zk.productinfo" }}
ericsson.com/product-name: "Data Coordinator ZK"
ericsson.com/product-number: "CAV10120/1"
ericsson.com/product-revision: "{{.Values.productInfo.rstate}}"
{{- end }}

{{/*
Client CA Secret Name
*/}}
{{- define "eric-data-coordinator-zk.client.ca.secret" -}}
{{ template "eric-data-coordinator-zk.fullname" . }}-client-ca-secret
{{- end -}}

{{/*
Server Cert Secret Name
*/}}
{{- define "eric-data-coordinator-zk.server.cert.secret" -}}
{{ template "eric-data-coordinator-zk.fullname" . }}-server-cert-secret
{{- end -}}

{{/*
Labels.
*/}}
{{- define "eric-data-coordinator-zk.labels" }}
{{- include "eric-data-coordinator-zk.selectorLabels" . }}
app.kubernetes.io/version: {{ include "eric-data-coordinator-zk.chart" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
app.kubernetes.io/name: {{ include "eric-data-coordinator-zk.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "eric-data-coordinator-zk.selectorLabels" }}
release: {{ .Release.Name | quote }}
app: {{ template "eric-data-coordinator-zk.name" . }}
{{- end }}

{{/*
Allow for override of agent name
*/}}
{{- define "eric-data-coordinator-zk.agentName" -}}
{{ template "eric-data-coordinator-zk.name" . }}-agent
{{- end -}}

{{/*
Labels.
*/}}
{{- define "eric-data-coordinator-zk.agent.labels" }}
{{- include "eric-data-coordinator-zk.agent.selectorLabels" . }}
app.kubernetes.io/version: {{ include "eric-data-coordinator-zk.chart" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end }}

{{/*
brLabelValues.
*/}}
{{- define "eric-data-coordinator-zk.agent.brlabels" }}
{{- if .Values.brAgent.brLabelValue -}}
    {{- print .Values.brAgent.brLabelValue -}}
{{ else }}
    {{ template "eric-data-coordinator-zk.fullname" . }}
{{ end }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "eric-data-coordinator-zk.agent.selectorLabels" }}
app.kubernetes.io/name: {{ include "eric-data-coordinator-zk.agentName" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{ .Values.global.adpBR.brLabelKey }}: {{ template "eric-data-coordinator-zk.agent.brlabels" . }}
{{- end }}

{{/*
volume information.
*/}}
{{- define "eric-data-coordinator-zk.volumes" }}
{{- if .Values.global.security.tls.enabled -}}
  {{- if eq .Values.service.endpoints.datacoordinatorzk.tls.provider "edaTls" -}}
- name: {{ .Values.service.endpoints.datacoordinatorzk.tls.edaTls.secretName | quote }}
  secret:
    secretName: {{ .Values.service.endpoints.datacoordinatorzk.tls.edaTls.secretName | quote }}
  {{- else if eq .Values.service.endpoints.datacoordinatorzk.tls.provider "sip-tls" -}}
- name: server-cert
  secret:
    secretName: {{ include "eric-data-coordinator-zk.server.cert.secret" . | quote }}
- name: siptls-ca
  secret:
    secretName: "eric-sec-sip-tls-trusted-root-cert"
- name: client-ca
  secret:
    secretName: {{ include "eric-data-coordinator-zk.client.ca.secret" . | quote }}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
volume Mount information.
*/}}
{{- define "eric-data-coordinator-zk.secretsMountPath" -}}
{{- if .Values.global.security.tls.enabled }}
  {{- if eq .Values.service.endpoints.datacoordinatorzk.tls.provider "edaTls" }}
  - name: {{ .Values.service.endpoints.datacoordinatorzk.tls.edaTls.secretName | quote }}
    mountPath: "/etc/zookeeper/secrets"
    readOnly: true
  {{- else if eq .Values.service.endpoints.datacoordinatorzk.tls.provider "sip-tls" }}
  - name:  server-cert
    mountPath: "/run/zookeeper/secrets/servercert"
  - name: siptls-ca
    mountPath: "/run/zookeeper/secrets/siptlsca"
  - name: client-ca
    mountPath: "/run/zookeeper/secrets/clientca"
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
configmap volumes + additional volumes
*/}}
{{- define "eric-data-coordinator-zk.agent.volumes" -}}
{{- if eq .Values.security.tls.agentToBro.enabled true }}
- name: {{ template "eric-data-coordinator-zk.name" . }}-siptls-ca
  secret:
    secretName: "eric-sec-sip-tls-trusted-root-cert"
{{- end }}
- name: {{ template "eric-data-coordinator-zk.agentName" . }}
  configMap:
    defaultMode: 0444
    name: {{ template "eric-data-coordinator-zk.agentName" . }}
{{- end -}}

{{/*
configmap volumemounts + additional volume mounts
*/}}
{{- define "eric-data-coordinator-zk.agent.volumeMounts" -}}
{{- if eq .Values.security.tls.agentToBro.enabled true }}
- name: {{ template "eric-data-coordinator-zk.name" . }}-siptls-ca
  mountPath: "/run/sec/cas/siptlsca/"
{{ end -}}
- name: {{ template "eric-data-coordinator-zk.agentName" . }}
  mountPath: /{{ .Values.brAgent.properties.fileName }}
  subPath: {{ .Values.brAgent.properties.fileName }}
- name: {{ template "eric-data-coordinator-zk.agentName" . }}
  mountPath: /{{ .Values.brAgent.logging.fileName }}
  subPath: {{ .Values.brAgent.logging.fileName }}
- name: backupdir
  mountPath: /backupdata
{{ end -}}

{{/*
Semi-colon separated list of backup types
*/}}
{{- define "eric-data-coordinator-zk.agent.backupTypes" }}
{{- range $i, $e := .Values.brAgent.backupTypeList -}}
{{- if eq $i 0 -}}{{- printf " " -}}{{- else -}}{{- printf ";" -}}{{- end -}}{{- . -}}
{{- end -}}
{{- end -}}

{{/*
DNS LIST.
*/}}
{{- define "eric-data-coordinator-zk.dns" -}}
{{- $dnslist := list (include "eric-data-coordinator-zk.dnsname" .) -}}
{{- $dnslist | toJson -}}
{{- end}}

{{/*
Quorum DNS
*/}}
{{- define "eric-data-coordinator-zk.dnsname" -}}
*.{{- include "eric-data-coordinator-zk.ensembleService.fullname" . }}.{{ .Release.Namespace }}.svc.{{ .Values.clusterDomain }}
{{- end}}
