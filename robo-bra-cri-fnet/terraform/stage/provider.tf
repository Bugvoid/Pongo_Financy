provider "aws" {
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  access_key                  = var.aws_access_key
  secret_key                  = var.aws_secret_key
  region                      = var.aws_region
  default_tags {
    tags = {
      Owner   = "terraform"
      Project = "robo"
    }
  }
}

provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = "us-east-1"
  alias      = "virginia"
  default_tags {
    tags = {
      Owner   = "terraform"
      Project = "robo"
    }
  }
}