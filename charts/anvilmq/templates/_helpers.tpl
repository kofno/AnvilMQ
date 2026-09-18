{{- define "anvilmq.name" -}}
{{- printf "%s-anvilmq" .Release.Name | trunc 54 | trimSuffix "-" -}}
{{- end -}}
{{- define "anvilmq.selectorLabels" -}}
app.kubernetes.io/name: anvilmq
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "anvilmq.labels" -}}
{{ include "anvilmq.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{- end -}}
