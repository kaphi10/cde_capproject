terraform {
  backend "s3" {
    bucket = "kafayat-project-staging" 
    key    = "dev/devstage1.tfstate"
    region = "eu-north-1"
    
  }
}