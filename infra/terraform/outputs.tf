output "namespace" {
  description = "Namespace the streaming consumer runs in."
  value       = kubernetes_namespace_v1.streamlake.metadata[0].name
}

output "deployment" {
  value = kubernetes_deployment_v1.consumer.metadata[0].name
}

output "spark_ui_url" {
  description = "Spark UI once the kind cluster maps 30040 to the host."
  value       = "http://localhost:30040"
}

output "logs_command" {
  value = "kubectl -n ${var.namespace} logs -f deploy/streamlake-consumer"
}
