variable "kubeconfig_path" {
  description = "Path to the kubeconfig for the target cluster."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "kubectl context to deploy into. `kind-streamlake` for the local cluster."
  type        = string
  default     = "kind-streamlake"
}

variable "namespace" {
  description = "Namespace to create and deploy into."
  type        = string
  default     = "streamlake"
}

variable "image" {
  description = "Container image for the streaming consumer."
  type        = string
  default     = "streamlake/stream:local"
}

variable "kafka_bootstrap" {
  description = "Kafka bootstrap servers as reachable from inside the cluster."
  type        = string
  default     = "kafka.streamlake.svc.cluster.local:9092"
}

variable "kafka_topic" {
  type    = string
  default = "streamlake.transactions"
}

variable "contract_mode" {
  description = "fail = a contract breach kills the micro-batch; warn = record and continue."
  type        = string
  default     = "fail"

  validation {
    condition     = contains(["fail", "warn"], var.contract_mode)
    error_message = "contract_mode must be either \"fail\" or \"warn\"."
  }
}

variable "checkpoint_size" {
  description = "Size of the streaming checkpoint volume. Offsets and dedup state live here."
  type        = string
  default     = "5Gi"
}

variable "warehouse_size" {
  type    = string
  default = "20Gi"
}

variable "resources" {
  description = "Container resource requests and limits."
  type = object({
    cpu_request    = string
    memory_request = string
    cpu_limit      = string
    memory_limit   = string
  })
  default = {
    cpu_request    = "500m"
    memory_request = "2Gi"
    cpu_limit      = "2"
    memory_limit   = "4Gi"
  }
}
