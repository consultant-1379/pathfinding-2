{{/*
Various supervisor parameters
*/}}

{{- define "eric-sec-sip-tls.supervisor.emergencyTtl" -}}
{{- $supervisor := default dict .Values.supervisor }}
{{- default 13824000 $supervisor.emergencyTtl -}}
{{- end -}}

{{- define "eric-sec-sip-tls.supervisor.probeInitialDelaySeconds" -}}
{{- $supervisor := default dict .Values.supervisor }}
{{- default 1800 $supervisor.probeInitialDelaySeconds -}}
{{- end -}}

{{- define "eric-sec-sip-tls.supervisor.probePeriodSeconds" -}}
{{- $supervisor := default dict .Values.supervisor }}
{{- default 30 $supervisor.probePeriodSeconds -}}
{{- end -}}

{{- define "eric-sec-sip-tls.supervisor.recoveryThreshold" -}}
{{- $supervisor := default dict .Values.supervisor }}
{{- default 3600 $supervisor.recoveryThreshold -}}
{{- end -}}

{{- define "eric-sec-sip-tls.supervisor.wdcTtl" -}}
{{- $supervisor := default dict .Values.supervisor }}
{{- default 1800 $supervisor.wdcTtl -}}
{{- end -}}
