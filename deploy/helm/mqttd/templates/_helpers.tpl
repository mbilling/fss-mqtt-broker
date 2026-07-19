{{/* Chart name (overridable). */}}
{{- define "mqttd.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully-qualified app name (the StatefulSet / pod-name prefix). */}}
{{- define "mqttd.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Headless service name that backs gossip discovery + the peer mesh. */}}
{{- define "mqttd.headlessName" -}}
{{- printf "%s-headless" (include "mqttd.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mqttd.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Selector labels — stable, used by the StatefulSet selector and Services. */}}
{{- define "mqttd.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mqttd.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Common labels. */}}
{{- define "mqttd.labels" -}}
helm.sh/chart: {{ include "mqttd.chart" . }}
{{ include "mqttd.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: broker
{{- end -}}

{{- define "mqttd.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "mqttd.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The image reference (tag defaults to the chart appVersion). */}}
{{- define "mqttd.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
