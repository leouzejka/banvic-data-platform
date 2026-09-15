resource "kubernetes_service" "postgres" {
  wait_for_load_balancer = false

  metadata {
    name      = "banvic-postgres"
    namespace = kubernetes_namespace.banvic.metadata[0].name
  }

  spec {
    selector = {
      app = "banvic-postgres"
    }

    port {
      port        = 5432
      target_port = 5432
      protocol    = "TCP"
    }

    type = "ClusterIP"
  }
}