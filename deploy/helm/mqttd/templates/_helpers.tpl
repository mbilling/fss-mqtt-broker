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

{{/* ---------------------------------------------------------------------- */}}
{{/* Per-pod cluster-bus certificates from cert-manager's csi-driver          */}}
{{/* (secrets.peerTls.certManager, ADR 0088 D1). "true" when an issuer is     */}}
{{/* named, after checking what must come with it: the certificate's          */}}
{{/* LIFETIME and its RENEWAL LEAD are stated by whoever deploys — the chart  */}}
{{/* has no default for either, and cert-manager's own (90 days, renewed at   */}}
{{/* two thirds) is nobody's decision.                                        */}}
{{/* ---------------------------------------------------------------------- */}}
{{- define "mqttd.peerTlsCertManager" -}}
{{- $peer := .Values.secrets.peerTls -}}
{{- $cm := $peer.certManager | default dict -}}
{{- if $cm.issuerName -}}
{{- if or $peer.secretName $peer.dir -}}
{{- fail "secrets.peerTls.certManager.issuerName is mutually exclusive with secrets.peerTls.secretName and secrets.peerTls.dir: the chart renders the cert-manager csi volume itself and mounts it at secrets.peerTls.mountPath" -}}
{{- end -}}
{{- if not $cm.duration -}}
{{- fail "secrets.peerTls.certManager.duration is required with issuerName: the lifetime of a node's certificate is stated when the cluster is deployed and has no default (ADR 0088), e.g. --set secrets.peerTls.certManager.duration=24h" -}}
{{- end -}}
{{- if not $cm.renewBefore -}}
{{- fail "secrets.peerTls.certManager.renewBefore is required with issuerName: how long before its end a certificate is renewed is stated, not defaulted. Set it ABOVE a third of duration (for duration=24h, renewBefore=10h): the broker starts warning at two thirds of the lifetime, and cert-manager's default renews exactly then" -}}
{{- end -}}
true
{{- end -}}
{{- end -}}

{{/* The csi volume's attributes: the four rules the bus enforces on a peer    */}}
{{/* certificate (CN = pod name, a SAN for the pod's advertise host, both      */}}
{{/* EKUs, an ECDSA PKCS#8 key), then the lifetime and the renewal lead.       */}}
{{/* ${POD_NAME} / ${POD_NAMESPACE} are the csi-driver's own variables.        */}}
{{- define "mqttd.peerTlsCsiAttributes" -}}
{{- $cm := .Values.secrets.peerTls.certManager -}}
csi.cert-manager.io/issuer-name: {{ $cm.issuerName | quote }}
csi.cert-manager.io/issuer-kind: {{ $cm.issuerKind | default "Issuer" | quote }}
csi.cert-manager.io/issuer-group: {{ $cm.issuerGroup | default "cert-manager.io" | quote }}
csi.cert-manager.io/common-name: "${POD_NAME}"
csi.cert-manager.io/dns-names: {{ printf "${POD_NAME}.%s.${POD_NAMESPACE}.svc.cluster.local" (include "mqttd.headlessName" .) | quote }}
csi.cert-manager.io/key-usages: "server auth,client auth"
csi.cert-manager.io/key-algorithm: "ECDSA"
csi.cert-manager.io/key-encoding: "PKCS8"
csi.cert-manager.io/duration: {{ $cm.duration | quote }}
csi.cert-manager.io/renew-before: {{ $cm.renewBefore | quote }}
{{- with (.Values.podSecurityContext | default dict).fsGroup }}
csi.cert-manager.io/fs-group: {{ . | quote }}
{{- end }}
{{- end -}}

{{/* ---------------------------------------------------------------------- */}}
{{/* The broker container's environment.                                     */}}
{{/*                                                                         */}}
{{/* Secret material is referenced BY PATH (ADR 0046 T5), and this is where   */}}
{{/* the paths are DERIVED from the secret names rather than left to the      */}}
{{/* operator to restate. That is the fix for issue #262: the chart used to   */}}
{{/* mount the cluster-bus Secret and reference nothing in it, so setting     */}}
{{/* secrets.peerTls.secretName mounted material no broker ever read and the  */}}
{{/* bus it advertised stayed PLAINTEXT. Now "the secret is set" implies      */}}
{{/* "the secret is used", structurally.                                     */}}
{{/*                                                                         */}}
{{/* Each pod is pointed at ITS OWN leaf through Kubernetes' dependent-env    */}}
{{/* expansion of $(POD_NAME). That indirection is what makes a per-node CN   */}}
{{/* possible at all: a StatefulSet pod template cannot select a different    */}}
{{/* Secret per ordinal, but every pod can select a different KEY inside one  */}}
{{/* Secret. The cluster bus binds node identity to the certificate's Subject */}}
{{/* CN, so a per-pod certificate is not a nicety — one shared leaf drops     */}}
{{/* every peer link.                                                        */}}
{{/*                                                                         */}}
{{/* Chart-managed entries come first, and any name already declared in       */}}
{{/* extraEnv is skipped so an explicit override still wins. POD_NAME is the  */}}
{{/* one exception: it must be DEFINED BEFORE the entries that reference it   */}}
{{/* (Kubernetes leaves a forward reference unexpanded), so it is always      */}}
{{/* emitted first when the peer-bus paths need it.                           */}}
{{/* ---------------------------------------------------------------------- */}}
{{- define "mqttd.brokerEnv" -}}
{{- $declared := dict -}}
{{- range (.Values.extraEnv | default list) -}}
{{- $_ := set $declared .name true -}}
{{- end -}}
{{- $peer := .Values.secrets.peerTls -}}
{{- $gossip := .Values.secrets.gossipKey -}}
{{- if and $peer.secretName $peer.dir }}
{{- fail "secrets.peerTls.secretName and secrets.peerTls.dir are mutually exclusive: one shared Secret keyed by pod name, or one per-pod directory (e.g. a cert-manager csi volume) — not both" }}
{{- end }}
{{- /* secrets.peerTls.certManager: the chart renders the cert-manager csi volume itself
       (statefulset.yaml) at peerTls.mountPath, so from here on it is the `dir` layout. */}}
{{- $dir := $peer.dir -}}
{{- if eq (include "mqttd.peerTlsCertManager" .) "true" }}
{{- $dir = $peer.mountPath -}}
{{- end }}
{{- if $peer.secretName }}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
{{- if not (hasKey $declared "MQTTD_PEER_TLS_CA") }}
- name: MQTTD_PEER_TLS_CA
  value: {{ printf "%s/ca.crt" $peer.mountPath | quote }}
{{- end }}
{{- if not (hasKey $declared "MQTTD_PEER_TLS_CERT") }}
- name: MQTTD_PEER_TLS_CERT
  value: {{ printf "%s/$(POD_NAME).crt" $peer.mountPath | quote }}
{{- end }}
{{- if not (hasKey $declared "MQTTD_PEER_TLS_KEY") }}
- name: MQTTD_PEER_TLS_KEY
  value: {{ printf "%s/$(POD_NAME).key" $peer.mountPath | quote }}
{{- end }}
{{- else if $dir }}
{{- /* A per-pod directory (cert-manager csi-driver layout): tls.crt/tls.key/ca.crt.
       No POD_NAME indirection — the volume itself is per-pod, which is the point:
       no shared Secret, so no pod can read another's key. */}}
{{- if not (hasKey $declared "MQTTD_PEER_TLS_CA") }}
- name: MQTTD_PEER_TLS_CA
  value: {{ printf "%s/ca.crt" $dir | quote }}
{{- end }}
{{- if not (hasKey $declared "MQTTD_PEER_TLS_CERT") }}
- name: MQTTD_PEER_TLS_CERT
  value: {{ printf "%s/tls.crt" $dir | quote }}
{{- end }}
{{- if not (hasKey $declared "MQTTD_PEER_TLS_KEY") }}
- name: MQTTD_PEER_TLS_KEY
  value: {{ printf "%s/tls.key" $dir | quote }}
{{- end }}
{{- end }}
{{- if $gossip.secretName }}
{{- if not (hasKey $declared "MQTTD_SWIM_KEY_FILE") }}
- name: MQTTD_SWIM_KEY_FILE
  value: {{ printf "%s/swim-key" $gossip.mountPath | quote }}
{{- end }}
{{- end }}
{{- /* The admin API (ADR 0081): served with the pod's own cluster-bus certificate. */}}
{{- $admin := .Values.admin -}}
{{- if $admin.enabled }}
{{- if not (or $peer.secretName $dir) }}
{{- fail "admin.enabled needs the cluster-bus certificates (secrets.peerTls): each pod serves the admin API with its own cluster certificate" }}
{{- end }}
{{- if not $admin.clientCa.secretName }}
{{- fail "admin.enabled needs admin.clientCa.secretName: a Secret with ca.crt, the CA that issues admin client certificates" }}
{{- end }}
{{- if not (or $admin.viewers $admin.operators) }}
{{- fail "admin.enabled needs at least one certificate subject in admin.viewers or admin.operators" }}
{{- end }}
- name: MQTTD_ADMIN_BIND
  value: "0.0.0.0:9443"
- name: MQTTD_ADMIN_CERT
  value: {{ ternary (printf "%s/$(POD_NAME).crt" $peer.mountPath) (printf "%s/tls.crt" $dir) (not (empty $peer.secretName)) | quote }}
- name: MQTTD_ADMIN_KEY
  value: {{ ternary (printf "%s/$(POD_NAME).key" $peer.mountPath) (printf "%s/tls.key" $dir) (not (empty $peer.secretName)) | quote }}
- name: MQTTD_ADMIN_CLIENT_CA
  value: {{ printf "%s/ca.crt" $admin.clientCa.mountPath | quote }}
{{- with $admin.viewers }}
- name: MQTTD_ADMIN_VIEWERS
  value: {{ join ";" . | quote }}
{{- end }}
{{- with $admin.operators }}
- name: MQTTD_ADMIN_OPERATORS
  value: {{ join ";" . | quote }}
{{- end }}
{{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* ---------------------------------------------------------------------- */}}
{{/* Boundary bridge (ADR 0025). Opt-in, and deployed BESIDE the broker       */}}
{{/* rather than inside it: separate process, separate identity, separate     */}}
{{/* failure domain — usually a different security zone entirely.            */}}
{{/* ---------------------------------------------------------------------- */}}

{{- define "mqttd.bridgeFullname" -}}
{{- printf "%s-bridge" (include "mqttd.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mqttd.bridgeSelectorLabels" -}}
app.kubernetes.io/name: {{ include "mqttd.name" . }}-bridge
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "mqttd.bridgeLabels" -}}
helm.sh/chart: {{ include "mqttd.chart" . }}
{{ include "mqttd.bridgeSelectorLabels" . }}
app.kubernetes.io/component: bridge
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "mqttd.bridgeImage" -}}
{{- printf "%s:%s" .Values.bridge.image.repository (default .Chart.AppVersion .Values.bridge.image.tag) -}}
{{- end -}}
