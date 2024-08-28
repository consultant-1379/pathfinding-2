{{/*
Create a map from ".Values.global" with defaults if missing in values file.
This hides defaults from values file.
*/}}
{{ define "eric-sec-sip-tls.global" }}
  {{- $globalDefaults := dict "nodeSelector" (dict) -}}
  {{ if .Values.global }}
    {{- mergeOverwrite $globalDefaults .Values.global | toJson -}}
  {{ else }}
    {{- $globalDefaults | toJson -}}
  {{ end }}
{{ end }}

{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "eric-sec-sip-tls.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "eric-sec-sip-tls.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart version as used by the version label.
*/}}
{{- define "eric-sec-sip-tls.version" -}}
{{- printf "%s" .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create image registry url
*/}}
{{- define "eric-sec-sip-tls.registryUrl" -}}
    {{- $registryUrl := "armdocker.rnd.ericsson.se" -}}
    {{- if .Values.global -}}
        {{- if .Values.global.registry -}}
            {{- if .Values.global.registry.url -}}
                {{- $registryUrl = .Values.global.registry.url -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- if .Values.imageCredentials.registry -}}
        {{- if .Values.imageCredentials.registry.url -}}
            {{- $registryUrl = .Values.imageCredentials.registry.url -}}
        {{- end -}}
    {{- end -}}
    {{- print $registryUrl -}}
{{- end -}}

{{/*
Create image registry url and repo path
*/}}
{{- define "eric-sec-sip-tls.registryUrlPath" -}}
{{- include "eric-sec-sip-tls.registryUrl" . }}/{{ .Values.imageCredentials.repoPath }}
{{- end -}}

{{/*
Create image pull secrets
*/}}
{{- define "eric-sec-sip-tls.pullSecrets" -}}
    {{- $globalPullSecret := "" -}}
    {{- if .Values.global -}}
        {{- if .Values.global.pullSecret -}}
            {{- $globalPullSecret = .Values.global.pullSecret -}}
        {{- end -}}
    {{- end -}}
    {{- if .Values.global -}}
        {{- if .Values.global.registry -}}
            {{- if .Values.global.registry.pullSecret -}}
                {{- $globalPullSecret = .Values.global.registry.pullSecret -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- if .Values.imageCredentials -}}
        {{- if .Values.imageCredentials.pullSecret -}}
             {{- $globalPullSecret = .Values.imageCredentials.pullSecret -}}
        {{- end -}}
    {{- end -}}
    {{- if .Values.imageCredentials -}}
        {{- if .Values.imageCredentials.registry -}}
            {{- if .Values.imageCredentials.registry.pullSecret -}}
                {{- $globalPullSecret = .Values.imageCredentials.registry.pullSecret -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- print $globalPullSecret -}}
{{- end -}}

{{/*
Create image pull policy
*/}}
{{- define "eric-sec-sip-tls.pullPolicy" -}}
    {{- $globalRegistryImagePullPolicy := "IfNotPresent" -}}
    {{- if .Values.global -}}
        {{- if .Values.global.registry -}}
            {{- if .Values.global.registry.imagePullPolicy -}}
                {{- $globalRegistryImagePullPolicy = .Values.global.registry.imagePullPolicy -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- if .Values.imageCredentials -}}
        {{- if .Values.imageCredentials.pullPolicy -}}
            {{- $globalRegistryImagePullPolicy = .Values.imageCredentials.pullPolicy -}}
        {{- else if .Values.imageCredentials.registry -}}
            {{- if .Values.imageCredentials.registry.imagePullPolicy -}}
                {{- $globalRegistryImagePullPolicy = .Values.imageCredentials.registry.imagePullPolicy -}}
            {{- end -}}
        {{- end -}}
    {{- end -}}
    {{- print $globalRegistryImagePullPolicy -}}
{{- end -}}


{{/*
Create a merged set of nodeSelectors from global and service level.
*/}}
{{ define "eric-sec-sip-tls.nodeSelector" }}
  {{- $g := fromJson (include "eric-sec-sip-tls.global" .) -}}
  {{- if .Values.nodeSelector -}}
    {{- range $key, $localValue := .Values.nodeSelector -}}
      {{- if hasKey $g.nodeSelector $key -}}
          {{- $globalValue := index $g.nodeSelector $key -}}
          {{- if ne $globalValue $localValue -}}
            {{- printf "nodeSelector \"%s\" is specified in both global (%s: %s) and service level (%s: %s) with differing values which is not allowed." $key $key $globalValue $key $localValue | fail -}}
          {{- end -}}
      {{- end -}}
    {{- end -}}
    {{- toYaml (merge $g.nodeSelector .Values.nodeSelector) | trim -}}
  {{- else -}}
    {{- toYaml $g.nodeSelector | trim -}}
  {{- end -}}
{{ end }}

