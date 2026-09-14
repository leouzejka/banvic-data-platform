resource "kubernetes_secret" "postgres" {
  metadata {
    name      = "banvic-postgres-secret"
    namespace = kubernetes_namespace.banvic.metadata[0].name
  }

  type = "Opaque"

  data = {
    POSTGRES_DB       = "banvic_database"
    POSTGRES_USER     = "banvic"
    POSTGRES_PASSWORD = var.postgres_password
  }

  wait_for_service_account_token = false

  lifecycle {
    ignore_changes = [data]
  }
}