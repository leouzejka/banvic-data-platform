resource "kubernetes_persistent_volume_claim" "postgres" {
  metadata {
    name      = "banvic-postgres-pvc"
    namespace = kubernetes_namespace.banvic.metadata[0].name
  }

  spec {
    access_modes = ["ReadWriteOnce"]

    resources {
      requests = {
        storage = "5Gi"
      }
    }

    storage_class_name = "standard"
  }
}