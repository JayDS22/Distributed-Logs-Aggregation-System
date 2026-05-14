{{/* Common labels and naming helpers */}}

{{- define "logstream.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end -}}

{{- define "logstream.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "logstream.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "logstream.labels" -}}
app.kubernetes.io/name: {{ include "logstream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "logstream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "logstream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
