{{/* vim: set filetype=mustache: */}}
{{/* Expand the name of the chart. */}}

{{/* Generate dm-pre image */}}
{{- define "dm-pre.image" -}}
{{- if .Values.image.registry }}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}