terraform {
  required_version = ">= 1.6"

  required_providers {
    scaleway = {
      source  = "scaleway/scaleway"
      version = "~> 2.0"
    }
  }
}

# Scaleway credentials come from the environment / config file:
#   SCW_ACCESS_KEY, SCW_SECRET_KEY, SCW_DEFAULT_PROJECT_ID, SCW_DEFAULT_ORGANIZATION_ID
#
# This root module deliberately does NOT manage DNS: the single-instance
# deploy points its records at one static IP, which is a two-minute job in the
# Cloudflare dashboard (and `deploy/tofu` already owns the Cloudflare provider
# for the Kapsule path).
provider "scaleway" {
  zone = var.zone
}
