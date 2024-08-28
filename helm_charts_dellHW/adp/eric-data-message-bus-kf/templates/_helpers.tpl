

{{/*
Connection to dataCoordinator
*/}}
{{- define "eric-data-message-bus-kf.dataCoordinator.connectHost" -}}
{{- printf "%s:%s/" .Values.dataCoordinator.clientServiceName  .Values.dataCoordinator.clientPort -}}
{{- end -}}

{{/*
Connection to dataCoordinator with chroot
*/}}
{{- define "eric-data-message-bus-kf.dataCoordinator.connect" -}}
{{ template "eric-data-message-bus-kf.dataCoordinator.connectHost" . }}{{ template "eric-data-message-bus-kf.fullname" . }}
{{- end -}}


{{/*
Expand the name of the chart.
*/}}
{{- define "eric-data-message-bus-kf.fullname" -}}
{{- if .Values.fullnameOverride -}}
  {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
  {{- $name := default .Chart.Name .Values.nameOverride -}}
  {{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}


{{/*
Expand the name of the chart.
*/}}
{{- define "eric-data-message-bus-kf-ext.fullname" -}}
{{- if .Values.fullnameOverride -}}
  {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
  {{- $name := default .Chart.Name .Values.nameOverride -}}
  {{- printf "%s-ext" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "eric-data-message-bus-kf.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Expand the name of the chart.
*/}}
{{- define "eric-data-message-bus-kf.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels.
*/}}
{{- define "eric-data-message-bus-kf.labels" }}
{{- include "eric-data-message-bus-kf.selectorLabels" . }}
app.kubernetes.io/version: {{ include "eric-data-message-bus-kf.chart" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "eric-data-message-bus-kf.selectorLabels" }}
app.kubernetes.io/name: {{ include "eric-data-message-bus-kf.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{/*
Client CA Secret Name
*/}}
{{- define "eric-data-message-bus-kf.client.ca.secret" -}}
{{ template "eric-data-message-bus-kf.fullname" . }}-client-ca-secret
{{- end -}}

{{/*
Server Cert Secret Name
*/}}
{{- define "eric-data-message-bus-kf.server.cert.secret" -}}
{{ template "eric-data-message-bus-kf.fullname" . }}-server-cert-secret
{{- end -}}

{{/*
Product information.
*/}}
{{- define "eric-data-message-bus-kf.productinfo" }}
ericsson.com/product-name: "Message Bus KF"
ericsson.com/product-number: "CAV10119/1"
ericsson.com/product-revision: "{{.Values.productInfo.rstate}}"
{{- end}}

{{/*
volume information.
*/}}
{{- define "eric-data-message-bus-kf.volumes" }}
{{- if .Values.global.security.tls.enabled -}}
  {{- if eq .Values.security.tls.messagebuskf.provider "edaTls" -}}
- name: {{ .Values.security.tls.messagebuskf.edaTls.secretName | quote }}
  secret:
    secretName: {{ .Values.security.tls.messagebuskf.edaTls.secretName | quote }}
  {{- else if eq .Values.security.tls.messagebuskf.provider "sip-tls" -}}
- name: server-cert
  secret:
    secretName: {{ include "eric-data-message-bus-kf.server.cert.secret" . | quote }}
- name: siptls-ca
  secret:
    secretName: "eric-sec-sip-tls-trusted-root-cert"
- name: client-ca
  secret:
    secretName: {{ include "eric-data-message-bus-kf.client.ca.secret" . | quote }}
  {{- end -}}
{{- end -}}
{{ if and ( not .Values.persistence.persistentVolumeClaim.enabled ) ( not .Values.persistentVolumeClaim.enabled ) }}
- name: datadir
  emptyDir: {}
{{- end -}}
{{- end -}}

{{/*
Enable plaintext communication
*/}}
{{- define "eric-data-message-bus-kf.plaintext.enabled" -}}
{{- if  or (not .Values.global.security.tls.enabled) (eq .Values.service.endpoints.messagebuskf.tls.enforced "optional") -}}
true
{{- end -}}
{{- end -}}


{{/*
plaintext port
*/}}
{{- define "eric-data-message-bus-kf.plaintextPort" -}}
{{- if .Values.kafkaPort -}}
{{- .Values.kafkaPort }}
{{- else -}}
{{- .Values.security.plaintext.messagebuskf.port -}}
{{- end -}}
{{- end -}}


{{/*
volume Mount information.
*/}}
{{- define "eric-data-message-bus-kf.secretsMountPath" }}
{{- if .Values.global.security.tls.enabled }}
  {{- if eq .Values.security.tls.messagebuskf.provider "edaTls" }}
  - name: {{ .Values.security.tls.messagebuskf.edaTls.secretName | quote }}
    mountPath: "/etc/kafka/secrets"
    readOnly: true
  {{- else if eq .Values.security.tls.messagebuskf.provider "sip-tls" }}
  - name:  server-cert
    mountPath: {{ template "eric-data-message-bus-kf.servercert" . }}
  - name: siptls-ca
    mountPath: {{ template "eric-data-message-bus-kf.siptlsca" . }}
  - name: client-ca
    mountPath: {{ template "eric-data-message-bus-kf.clientca" . }}
  {{- end -}}
{{- end -}}
{{- end -}}


{{/*
SIP TLS server cert location
*/}}
{{- define "eric-data-message-bus-kf.servercert" -}}
"/run/kafka/secrets/servercert"
{{- end -}}

{{/*
SIP TLS certificate authority location
*/}}
{{- define "eric-data-message-bus-kf.siptlsca" -}}
"/run/kafka/secrets/siptlsca"
{{- end -}}

{{/*
Client CA certificate authority location
*/}}
{{- define "eric-data-message-bus-kf.clientca" -}}
"/run/kafka/secrets/clientca"
{{- end -}}


{{/*
SSL Client Authentication for Kafka
*/}}
{{- define "eric-data-message-bus-kf.clientAuth" -}}
{{- if eq .Values.service.endpoints.messagebuskf.tls.verifyClientCertificate "optional" -}}
"requested"
{{- else -}}
{{- .Values.service.endpoints.messagebuskf.tls.verifyClientCertificate | quote -}}
{{- end -}}
{{- end -}}

{{/*
DNS LIST.
*/}}
{{- define "eric-data-message-bus-kf.dns" -}}
{{- $dnslist := list (include "eric-data-message-bus-kf.dnsname" .) (include "eric-data-message-bus-kf.client" .) -}}
{{- $dnslist | toJson -}}
{{- end}}

{{/*
DNS.
*/}}
{{- define "eric-data-message-bus-kf.dnsname" -}}
*.{{- include "eric-data-message-bus-kf.fullname" . }}.{{ .Release.Namespace }}
{{- end}}

{{/*
Client
*/}}
{{- define "eric-data-message-bus-kf.client" -}}
{{ template "eric-data-message-bus-kf.fullname" . }}-client
{{- end -}}
