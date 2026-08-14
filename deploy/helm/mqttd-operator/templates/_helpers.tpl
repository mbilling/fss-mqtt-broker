{{/*
Fullname: release-qualified unless the release is already called mqttd-operator.
*/}}
{{- define "mqttd-operator.fullname" -}}
{{- if eq .Release.Name "mqttd-operator" -}}
mqttd-operator
{{- else -}}
{{ printf "%s-mqttd-operator" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "mqttd-operator.labels" -}}
app.kubernetes.io/name: mqttd-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}
