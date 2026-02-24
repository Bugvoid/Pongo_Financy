locals {
  stage = terraform.workspace
  stage_oficial_branco = local.stage == "oficial" ? "" : title(local.stage)
}

resource "aws_s3_bucket" "bucket" {
  bucket = "robo-cri-informe-${local.stage}"
  
}

resource "aws_s3_bucket_public_access_block" "bucket" {
  bucket = aws_s3_bucket.bucket.id

  block_public_acls   = true
  block_public_policy = true
  restrict_public_buckets = true
  ignore_public_acls = true
}

resource "aws_s3_bucket_policy" "allow_access_from_another_account" {
  bucket = aws_s3_bucket.bucket.id
  policy = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${IDAWS}:role/robosRole${local.stage_oficial_branco}"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::robo-cri-informe-${local.stage}/*"
        }
    ]
  }
  EOF
}

resource "aws_s3_object" "mensal" {
    bucket = aws_s3_bucket.bucket.id
    acl    = "private"
    key    = "mensal/full/"
}

resource "aws_sns_topic" "topic" {
  name = "cri-informe-mensal-${local.stage}"
  policy = <<EOF
  {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SNS:Publish",
      "Resource": "arn:aws:sns:sa-east-1:${IDAWS}:cri-informe-mensal-${local.stage}",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "${aws_s3_bucket.bucket.arn}"
        }
      }
    }
  ] 
  }
  EOF
}

module "sqs_financeiro" {
  source = "./sns_to_sqs"
  stage = local.stage
  topic_arn = aws_sns_topic.topic.arn
  queue_name = "qu-cri-informe-mensal-financeiro"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.bucket.id
  
  topic {
    id = "sns-cri-informe-mensal-${local.stage}"
    topic_arn     = aws_sns_topic.topic.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "mensal/full/"
    filter_suffix = ".xml"
  }
}