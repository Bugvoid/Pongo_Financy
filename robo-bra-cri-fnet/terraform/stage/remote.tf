terraform {
  backend "s3" {
    encrypt              = false
    bucket               = "eco-rnf"
    key                  = "terraform.tfstate"
    workspace_key_prefix = "terraform/stage"
    region               = "sa-east-1"
  }
}