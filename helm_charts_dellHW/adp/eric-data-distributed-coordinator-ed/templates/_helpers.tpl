{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "eric-data-distributed-coordinator-ed.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "eric-data-distributed-coordinator-ed.chart" -}}
	{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Registry path for the chart
*/}}
{{- define "eric-data-distributed-coordinator-ed.registryUrl" -}}
{{- $registryUrl := "armdocker.rnd.ericsson.se" -}}
{{- if .Values.global -}}
    {{- if .Values.global.registry -}}
        {{- if .Values.global.registry.url -}}
            {{- $registryUrl = .Values.global.registry.url -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- if .Values.imageCredentials.registry.url -}}
    {{- $registryUrl = .Values.imageCredentials.registry.url -}}
{{- end -}}
{{- print $registryUrl -}}
{{- end -}}

{{/*
Image path for the chart
*/}}
{{- define "eric-data-distributed-coordinator-ed.dcedImagePath" -}}
{{- $registryUrl := ( include "eric-data-distributed-coordinator-ed.registryUrl" . ) }}
{{- $repoPath := .Values.imageCredentials.repoPath -}}
{{- $image := .Values.images.etcd.name -}}
{{- $tag := .Values.images.etcd.tag -}}

{{- $imagePath := printf "%s/%s/%s:%s" $registryUrl $repoPath $image $tag -}}
{{- print (regexReplaceAll "[/]+" $imagePath "/") -}}
{{- end -}}

{{/*
Image path for the BR chart
*/}}
{{- define "eric-data-distributed-coordinator-ed.bragentImagePath" -}}
{{- $registryUrl := ( include "eric-data-distributed-coordinator-ed.registryUrl" . ) }}
{{- $repoPath := .Values.imageCredentials.repoPath -}}
{{- $image := .Values.images.brAgent.name -}}
{{- $tag := .Values.images.brAgent.tag -}}

{{- $imagePath := printf "%s/%s/%s:%s" $registryUrl $repoPath $image $tag -}}
{{- print (regexReplaceAll "[/]+" $imagePath "/") -}}
{{- end -}}


{{/*
Accommodate global params for pullSecrets 
*/}}
{{- define "eric-data-distributed-coordinator-ed.pullSecrets" -}}
{{- $pullSecret := "" -}}
{{- if .Values.global -}}
    {{- if .Values.global.registry -}}
        {{- if .Values.global.registry.pullSecret -}}
            {{- $pullSecret = .Values.global.registry.pullSecret -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- if .Values.imageCredentials.registry.pullSecret -}}
    {{- $pullSecret = .Values.imageCredentials.registry.pullSecret -}}
{{- end -}}
{{- print $pullSecret -}}
{{- end -}}

{{/*
Create peer url
*/}}

{{- define "eric-data-distributed-coordinator-ed.peerUrl" -}}
   {{- printf "https://0.0.0.0:%d" (int64 .Values.ports.etcd.peer) -}}
{{- end -}}

{{/*
If the timezone isn't set by a global parameter, set it to UTC
*/}}
{{- define "eric-data-distributed-coordinator-ed.timezone" -}}
{{- if .Values.global -}}
    {{- .Values.global.timezone | default "UTC" | quote -}}
{{- else -}}
    "UTC"
{{- end -}}
{{- end -}}

{{/*
Client connection scheme
*/}}
{{- define "eric-data-distributed-coordinator-ed.clientConnectionScheme" -}}
  {{ if eq .Values.security.etcd.certificates.enabled true }}
      {{- printf "https" -}}
    {{- else -}}
      {{- printf "http" -}}
  {{- end -}}
{{- end -}}

{{/*
Create client url
*/}}
{{- define "eric-data-distributed-coordinator-ed.clientUrl" -}}
   {{ $scheme := include "eric-data-distributed-coordinator-ed.clientConnectionScheme" . }}
   {{- printf "%s://0.0.0.0:%d" $scheme (int64 .Values.ports.etcd.client) -}}
{{- end -}}

{{/*
Advertised client url
*/}}
{{- define "eric-data-distributed-coordinator-ed.advertiseClientUrl" -}}
    {{- $scheme := include "eric-data-distributed-coordinator-ed.clientConnectionScheme" . -}}
    {{- $chartName := include "eric-data-distributed-coordinator-ed.name" . -}}
    {{- printf "%s://$(ETCD_NAME).%s.%s:%d" $scheme $chartName .Release.Namespace (int64 .Values.ports.etcd.client) -}}
{{- end -}}


{{/*
Advertised peer url
*/}}
{{- define "eric-data-distributed-coordinator-ed.initialAdvertisePeerUrl" -}}
	{{ $chartName := include "eric-data-distributed-coordinator-ed.name" . }}
	{{- printf "https://$(ETCD_NAME).%s-peer.%s.svc.cluster.local:%d" $chartName .Release.Namespace (int64 .Values.ports.etcd.peer) -}}
{{- end -}}


{{/*
client service
*/}}
{{- define "eric-data-distributed-coordinator-ed.clientService" -}}
	{{ $chartName := include "eric-data-distributed-coordinator-ed.name" . }}
	{{- printf "%s.%s:%d" $chartName .Release.Namespace (int64 .Values.ports.etcd.client) -}}
{{- end -}}

{{/*
ETCD endpoint for the agent.
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.endpoint" -}}
    {{ $chartName := include "eric-data-distributed-coordinator-ed.name" . }}
    {{- printf "%s:%d" $chartName (int64 .Values.ports.etcd.client) -}}
{{- end -}}

{{/*
Parameters that cannot be specified in the settings
*/}}
{{- define "eric-data-distributed-coordinator-ed.forbiddenParameters" -}}
  {{ list "ETCD_INITIAL_CLUSTER_TOKEN" "ETCD_NAME"  }}
{{- end -}}

{{/*
Etcd mountpath
*/}}
{{- define "eric-data-distributed-coordinator-ed.mountPath" -}}
  {{- if .Values.persistence.persistentVolumeClaim.enabled -}}
      {{- printf "%s" ( .Values.persistence.persistentVolumeClaim.mountPath) -}}
  {{- else}}
	  {{- printf "%s" ( .Values.persistentVolumeClaim.etcd.mountPath) -}}
  {{- end -}}
{{- end -}}

{{/*
Path to the TLS trusted CA cert file.
*/}}
{{- define "eric-data-distributed-coordinator-ed.trustedCA" -}}
  {{ $mountPath := include "eric-data-distributed-coordinator-ed.mountPath" . }}
  {{- printf "%s/%s/%s" $mountPath  .Values.security.etcd.certificates.ca.combined.path  .Values.security.etcd.certificates.ca.combined.fileName -}}
{{- end -}}

{{/*
Path to the server TLS cert file.
*/}}
{{- define "eric-data-distributed-coordinator-ed.serverCert" -}}
  {{ printf "%s/srvcert.pem" .Values.security.etcd.certificates.server.path }}
{{- end -}}

{{/*
Path to the server TLS key file.
*/}}
{{- define "eric-data-distributed-coordinator-ed.serverKeyFile" -}}
  {{ printf "%s/srvprivkey.pem" .Values.security.etcd.certificates.server.path }}
{{- end -}}

{{/*
Path to the peer TLS cert file.
*/}}
{{- define "eric-data-distributed-coordinator-ed.peerClientCert" -}}
  {{ printf "%s/srvcert.pem" .Values.security.etcd.certificates.peer.path }}
{{- end -}}

{{/*
Path to the peer TLS key file.
*/}}
{{- define "eric-data-distributed-coordinator-ed.peerClientKeyFile" -}}
  {{ printf "%s/srvprivkey.pem" .Values.security.etcd.certificates.peer.path }}
{{- end -}}

{{/*
etcdctl parameters
*/}}
{{- define "eric-data-distributed-coordinator-ed.etcdctlParameters" -}}
- name: ETCDCTL_API
  value: "3"
- name: ETCDCTL_ENDPOINTS
  value: {{ template "eric-data-distributed-coordinator-ed.clientService" . }}
{{- if eq .Values.security.etcd.certificates.enabled true }}
- name: ETCDCTL_CACERT
  value: {{ template "eric-data-distributed-coordinator-ed.trustedCA" . }}
- name: ETCDCTL_CERT
  value: {{ printf "%s/clicert.pem" .Values.security.etcd.certificates.client.path }}
- name: ETCDCTL_KEY
  value: {{ printf "%s/cliprivkey.pem" .Values.security.etcd.certificates.client.path }}
{{- end -}}
{{- end -}}


{{/*
secrets mount paths
*/}}
{{- define "eric-data-distributed-coordinator-ed.secretsMountPath" -}}
{{- if eq .Values.security.etcd.certificates.enabled true }}
- name: server-cert
  mountPath: {{ .Values.security.etcd.certificates.server.path }}
- name: peer-client-cert
  mountPath: {{ .Values.security.etcd.certificates.peer.path }}
- name: bootstrap-ca
  mountPath: {{ printf "%s/%s" .Values.security.etcd.certificates.ca.parentDir .Values.security.etcd.certificates.ca.bootstrap.path }}
- name: client-ca
  mountPath: {{ printf "%s/%s" .Values.security.etcd.certificates.ca.parentDir .Values.security.etcd.certificates.ca.client.path }}
- name: etcdctl-client-cert
  mountPath: {{ .Values.security.etcd.certificates.client.path }}
- name: siptls-ca
  mountPath: {{ printf "%s/%s" .Values.security.etcd.certificates.ca.parentDir .Values.security.etcd.certificates.ca.sipTls.path }}
{{- end }}
{{- end -}}

{{/*
secrets volumes
*/}}
{{- define "eric-data-distributed-coordinator-ed.secretsVolumes" -}}
{{- if eq .Values.security.etcd.certificates.enabled true }}
- name: siptls-ca
  secret:
    optional: true
    secretName: {{ .Values.security.etcd.certificates.ca.sipTls.name }}
- name: bootstrap-ca
  secret:
    optional: true
    secretName: {{ .Values.security.etcd.certificates.ca.bootstrap.name }}
- name: client-ca
  secret:
    optional: true
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-ca
- name: server-cert
  secret:
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-cert
- name: peer-client-cert
  secret:
    optional: true
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-peer-cert
- name: etcdctl-client-cert
  secret:
    optional: true
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-etcdctl-client-cert
{{- end }}
{{- end -}}

{{/*
Validate parameters
*/}}
{{- define "eric-data-distributed-coordinator-ed.validateParameters" -}}
  {{- $definedInvalidParameters := include "eric-data-distributed-coordinator-ed.validateParametersHelper" . -}}
  {{- $len := len $definedInvalidParameters -}}
  {{- if eq $len 0 -}}
    {{- print " valid" -}}
  {{- end -}}

{{- end -}}


{{/*
Validate parameters helper
*/}}
{{- define "eric-data-distributed-coordinator-ed.validateParametersHelper" -}}

   {{ $forbiddenParameters := list "ETCD_INITIAL_CLUSTER_TOKEN" "ETCD_NAME" "ETCDCTL_API" "ETCD_DATA_DIR" "ETCD_LISTEN_PEER_URLS" "ETCD_LISTEN_CLIENT_URLS" "ETCD_ADVERTISE_CLIENT_URLS" "ETCD_INITIAL_ADVERTISE_PEER_URLS" "ETCD_INITIAL_CLUSTER_STATE" "ETCD_INITIAL_CLUSTER" "ETCD_PEER_AUTO_TLS" "ETCD_CLIENT_CERT_AUTH" "ETCD_CERT_FILE" "ETCD_TRUSTED_CA_FILE" "ETCD_KEY_FILE" }}

  {{- range $configName, $configValue := .Values.env.etcd -}}
    {{- if has $configName $forbiddenParameters -}}
	  {{- printf "%s " $configName -}}
    {{- end -}}
  {{- end -}}

{{- end -}}

{{/*
Annotations.
*/}}
{{- define "eric-data-distributed-coordinator-ed.annotations" }}
  ericsson.com/product-name: "Distributed Coordinator ED"
  ericsson.com/product-number: "CAV101067/1"
  ericsson.com/product-revision: {{ .Values.productInfo.rstate | quote }}
{{- end }}

{{/*
Labels
*/}}
{{- define "eric-data-distributed-coordinator-ed.labels" }}
{{- include "eric-data-distributed-coordinator-ed.selectorLabels" . }}
  app.kubernetes.io/version: {{ include "eric-data-distributed-coordinator-ed.chart" . | quote }}
  app.kubernetes.io/managed-by: {{ .Release.Service | quote  }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "eric-data-distributed-coordinator-ed.selectorLabels" }}
  app.kubernetes.io/name: {{ include "eric-data-distributed-coordinator-ed.name" . | quote }}
  app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{/*
Allow for override of agent name
*/}}
{{- define "eric-data-distributed-coordinator-ed.agentName" -}}
{{ template "eric-data-distributed-coordinator-ed.name" . }}-agent
{{- end -}}

{{/*
Agent Labels.
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.labels" }}
{{- include "eric-data-distributed-coordinator-ed.agent.selectorLabels" . }}
app.kubernetes.io/version: {{ include "eric-data-distributed-coordinator-ed.chart" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end }}

{{/*
Accommodate global params for broGrpcServicePort
*/}}

{{- define "eric-data-distributed-coordinator-ed.agent.broGrpcServicePort" -}}
{{- $broGrpcServicePort := "3000" -}}
{{- if .Values.global -}}
    {{- if .Values.global.adpBR -}}
        {{- if .Values.global.adpBR.broGrpcServicePort -}}
            {{- $broGrpcServicePort = .Values.global.adpBR.broGrpcServicePort -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- print $broGrpcServicePort -}}
{{- end -}}

{{/*
Accommodate global params for broServiceName
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.broServiceName" -}}
{{- $broServiceName := "eric-ctrl-bro" -}}
{{- if .Values.global -}}
    {{- if .Values.global.adpBR -}}
        {{- if .Values.global.adpBR.broServiceName -}}
            {{- $broServiceName = .Values.global.adpBR.broServiceName -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- print $broServiceName -}}
{{- end -}}


{{/*
Accommodate global params for brLabelKey
*/}}
{{/*
Get bro service brLabelKey
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.brLabelKey" -}}
{{- $brLabelKey := "adpbrlabelkey" -}}
{{- if .Values.global -}}
    {{- if .Values.global.adpBR -}}
        {{- if .Values.global.adpBR.brLabelKey -}}
            {{- $brLabelKey = .Values.global.adpBR.brLabelKey -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- print $brLabelKey -}}
{{- end -}}



{{/*
Selector labels for Agent.
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.selectorLabels" }}
app.kubernetes.io/name: {{ include "eric-data-distributed-coordinator-ed.agentName" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- if .Values.brAgent.brLabelValue }}
{{ include "eric-data-distributed-coordinator-ed.agent.brLabelKey" . }}: {{ .Values.brAgent.brLabelValue }}
{{ else }}
{{ include "eric-data-distributed-coordinator-ed.agent.brLabelKey" . }}: dc-etcd
{{- end }}
{{- end }}

{{/*
configmap volumes + additional volumes
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.volumes" -}}
{{- if eq .Values.security.tls.agentToBro.enabled true }}
- name: {{ template "eric-data-distributed-coordinator-ed.name" . }}-siptls-ca
  secret:
    optional: false
    secretName: "eric-sec-sip-tls-trusted-root-cert"
{{- end }}
{{- end -}}

{{/*
secrets mount paths
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.secretsMountPath" -}}
{{- if eq .Values.security.etcd.certificates.enabled true }}
- name: server-cert
  mountPath: {{ .Values.security.etcd.certificates.server.path }}
- name: client-ca
  mountPath: {{ printf "%s/%s" .Values.security.etcd.certificates.ca.parentDir .Values.security.etcd.certificates.ca.client.path }}
- name: etcdctl-client-cert
  mountPath: {{ .Values.security.etcd.certificates.client.path }}
- name: siptls-ca
  mountPath: {{ printf "%s/%s" .Values.security.etcd.certificates.ca.parentDir .Values.security.etcd.certificates.ca.sipTls.path }}
{{- end }}
{{- end -}}

{{/*
secrets volumes
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.secretsVolumes" -}}
{{- if eq .Values.security.etcd.certificates.enabled true }}
- name: siptls-ca
  secret:
    optional: false
    secretName: {{ .Values.security.etcd.certificates.ca.sipTls.name }}
- name: client-ca
  secret:
    optional: false
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-ca
- name: server-cert
  secret:
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-cert
- name: etcdctl-client-cert
  secret:
    optional: false
    secretName: {{ template "eric-data-distributed-coordinator-ed.name" . }}-etcdctl-client-cert
{{- end }}
{{- end -}}

{{/*
Semi-colon separated list of backup types
*/}}
{{- define "eric-data-distributed-coordinator-ed.agent.backupTypes" }}
{{- range $i, $e := .Values.brAgent.backupTypeList -}}
{{- if eq $i 0 -}}{{- printf " " -}}{{- else -}}{{- printf ";" -}}{{- end -}}{{- . -}}
{{- end -}}
{{- end -}}


{{/*
Additional SAN in cert to support hostname verification
*/}}
{{- define "eric-data-distributed-coordinator-ed.dns" -}}
{{- $dnslist := list (include "eric-data-distributed-coordinator-ed.dnsname" .) -}}
{{- $dnslist | toJson -}}
{{- end}}


{{/*
Wildcard name to match all ETCD instances.
*/}}
{{- define "eric-data-distributed-coordinator-ed.dnsname" -}}
*.{{- include "eric-data-distributed-coordinator-ed.name" . }}-peer.{{ .Release.Namespace }}.svc.{{ .Values.clusterDomain }}
{{- end}}

{{/*
Create a merged set of nodeSelectors from global and service level -dced.
*/}}
{{- define "eric-data-distributed-coordinator-ed.dcedNodeSelector" -}}
{{- $globalValue := (dict) -}}
{{- if .Values.global -}}
    {{- if .Values.global.nodeSelector -}}
        {{- if .Values.global.nodeSelector.dced -}}
            {{- $globalValue = .Values.global.nodeSelector.dced -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- if .Values.nodeSelector.dced -}}
  {{- range $key, $localValue := .Values.nodeSelector.dced -}}
    {{- if hasKey $globalValue $key -}}
         {{- $Value := index $globalValue $key -}}
         {{- if ne $Value $localValue -}}
           {{- printf "nodeSelector \"%s\" is specified in both global (%s: %s) and service level (%s: %s) with differing values which is not allowed." $key $key $globalValue $key $localValue | fail -}}
         {{- end -}}
     {{- end -}}
    {{- end -}}
    nodeSelector: {{- toYaml (merge $globalValue .Values.nodeSelector.dced) | trim | nindent 2 -}}
{{- else -}}
  {{- if not ( empty $globalValue ) -}}
    nodeSelector: {{- toYaml $globalValue | trim | nindent 2 -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create a merged set of nodeSelectors from global and service level - brAgent.
*/}}
{{- define "eric-data-distributed-coordinator-ed.brAgentNodeSelector" -}}
{{- $globalValue := (dict) -}}
{{- if .Values.global -}}
    {{- if .Values.global.nodeSelector -}}
        {{- if .Values.global.nodeSelector.brAgent -}}
            {{- $globalValue = .Values.global.nodeSelector.brAgent -}}
        {{- end -}}
    {{- end -}}
{{- end -}}
{{- if .Values.nodeSelector.brAgent -}}
  {{- range $key, $localValue := .Values.nodeSelector.brAgent -}}
    {{- if hasKey $globalValue $key -}}
         {{- $Value := index $globalValue $key -}}
         {{- if ne $Value $localValue -}}
           {{- printf "nodeSelector \"%s\" is specified in both global (%s: %s) and service level (%s: %s) with differing values which is not allowed." $key $key $globalValue $key $localValue | fail -}}
         {{- end -}}
     {{- end -}}
    {{- end -}}
    nodeSelector: {{- toYaml (merge $globalValue .Values.nodeSelector.brAgent) | trim | nindent 2 -}}
{{- else -}}
  {{- if not ( empty $globalValue ) -}}
    nodeSelector: {{- toYaml $globalValue | trim | nindent 2 -}}
  {{- end -}}
{{- end -}}
{{- end -}}