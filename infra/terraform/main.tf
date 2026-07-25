# StreamLake streaming consumer on Kubernetes.
#
# What Terraform buys over `kubectl apply` here: the config lives in one typed, validated place
# (contract_mode cannot be a typo, resource sizes are a single object), the whole deployment is
# destroyable in one command, and `terraform plan` shows the diff before the cluster changes.
# The manifests in ../k8s are the same objects for anyone who would rather run kustomize.

locals {
  labels = {
    "app.kubernetes.io/name"       = "streamlake"
    "app.kubernetes.io/component"  = "stream-consumer"
    "app.kubernetes.io/managed-by" = "terraform"
  }
}

resource "kubernetes_namespace_v1" "streamlake" {
  metadata {
    name   = var.namespace
    labels = { "app.kubernetes.io/name" = "streamlake" }
  }
}

resource "kubernetes_config_map_v1" "env" {
  metadata {
    name      = "streamlake-env"
    namespace = kubernetes_namespace_v1.streamlake.metadata[0].name
    labels    = local.labels
  }

  data = {
    KAFKA_BOOTSTRAP      = var.kafka_bootstrap
    KAFKA_TOPIC          = var.kafka_topic
    ICEBERG_CATALOG_TYPE = "hadoop"
    ICEBERG_WAREHOUSE    = "/data/warehouse"
    CONTRACT_MODE        = var.contract_mode
    STREAM_RUN_SECONDS   = "0"
    SPARK_DRIVER_MEMORY  = var.resources.memory_request
    STREAMLAKE_LOG_LEVEL = "INFO"
    TZ                   = "UTC"
  }
}

resource "kubernetes_persistent_volume_claim_v1" "checkpoints" {
  metadata {
    name      = "streamlake-checkpoints"
    namespace = kubernetes_namespace_v1.streamlake.metadata[0].name
    labels    = local.labels
  }

  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = { storage = var.checkpoint_size }
    }
  }

  wait_until_bound = false
}

resource "kubernetes_persistent_volume_claim_v1" "warehouse" {
  metadata {
    name      = "streamlake-warehouse"
    namespace = kubernetes_namespace_v1.streamlake.metadata[0].name
    labels    = local.labels
  }

  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = { storage = var.warehouse_size }
    }
  }

  wait_until_bound = false
}

resource "kubernetes_deployment_v1" "consumer" {
  metadata {
    name      = "streamlake-consumer"
    namespace = kubernetes_namespace_v1.streamlake.metadata[0].name
    labels    = local.labels
  }

  spec {
    # One replica, always. A second consumer would contend for the same checkpoint lock; the
    # way to scale a Structured Streaming job is partitions and executors, not pods.
    replicas = 1

    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = local.labels
    }

    template {
      metadata {
        labels = local.labels
        annotations = {
          # Roll the pods when the config changes, which a ConfigMap update does not do on its own.
          "streamlake.io/config-hash" = sha256(jsonencode(kubernetes_config_map_v1.env.data))
        }
      }

      spec {
        security_context {
          run_as_non_root = true
          run_as_user     = 10001
          fs_group        = 10001
        }

        container {
          name              = "consumer"
          image             = var.image
          image_pull_policy = "IfNotPresent"
          args              = ["consume"]

          env_from {
            config_map_ref {
              name = kubernetes_config_map_v1.env.metadata[0].name
            }
          }

          port {
            name           = "spark-ui"
            container_port = 4040
          }

          resources {
            requests = {
              cpu    = var.resources.cpu_request
              memory = var.resources.memory_request
            }
            limits = {
              cpu    = var.resources.cpu_limit
              memory = var.resources.memory_limit
            }
          }

          volume_mount {
            name       = "checkpoints"
            mount_path = "/data/checkpoints"
          }

          volume_mount {
            name       = "warehouse"
            mount_path = "/data/warehouse"
          }

          liveness_probe {
            tcp_socket {
              port = 4040
            }
            initial_delay_seconds = 90
            period_seconds        = 30
            failure_threshold     = 4
          }

          readiness_probe {
            tcp_socket {
              port = 4040
            }
            initial_delay_seconds = 45
            period_seconds        = 15
          }
        }

        volume {
          name = "checkpoints"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.checkpoints.metadata[0].name
          }
        }

        volume {
          name = "warehouse"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.warehouse.metadata[0].name
          }
        }
      }
    }
  }

  wait_for_rollout = false
}

resource "kubernetes_service_v1" "consumer" {
  metadata {
    name      = "streamlake-consumer"
    namespace = kubernetes_namespace_v1.streamlake.metadata[0].name
    labels    = local.labels
  }

  spec {
    selector = local.labels

    port {
      name        = "spark-ui"
      port        = 4040
      target_port = 4040
      node_port   = 30040
    }

    type = "NodePort"
  }
}
