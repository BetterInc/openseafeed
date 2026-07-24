terraform {
  required_version = ">= 1.6"

  required_providers {
    scaleway = {
      source  = "scaleway/scaleway"
      version = "~> 2.0"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

# Scaleway credentials come from the environment / config file:
#   SCW_ACCESS_KEY, SCW_SECRET_KEY, SCW_DEFAULT_PROJECT_ID, SCW_DEFAULT_ORGANIZATION_ID
provider "scaleway" {
  region = var.region
  zone   = var.zone
}

# Cloudflare API token via CLOUDFLARE_API_TOKEN.
provider "cloudflare" {}
