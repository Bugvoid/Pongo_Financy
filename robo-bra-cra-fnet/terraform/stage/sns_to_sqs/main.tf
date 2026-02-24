locals {
  stage = terraform.workspace
  stage_oficial_branco = local.stage == "oficial" ? "" : title(local.stage)
}

resource "aws_sqs_queue" "dead" {
  name = "${var.queue_name}-dead-${var.stage}"
}

resource "aws_sqs_queue" "queue" {
  name                       = "${var.queue_name}-${var.stage}"
  redrive_policy             = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.dead.arn}\",\"maxReceiveCount\":10}"
  visibility_timeout_seconds = 30
}


resource "aws_sns_topic_subscription" "target" {
  topic_arn = var.topic_arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.queue.arn
}


resource "aws_sqs_queue_policy" "policy" {
  queue_url = aws_sqs_queue.queue.id

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "${var.queue_name}-policy-${var.stage}",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam:::root",
          "arn:aws:iam:::role/robosRole${local.stage_oficial_branco}"
        ]
      },
      "Action": "SQS:*",
      "Resource": "${aws_sqs_queue.queue.arn}"
    },
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${aws_sqs_queue.queue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${var.topic_arn}"
        }
      }
    }
  ]
}
POLICY
}

resource "aws_sqs_queue_policy" "policy_dead" {
  queue_url = aws_sqs_queue.dead.id

  policy = <<POLICY
{
  "Version": "2008-10-17",
  "Id": "${var.queue_name}-policy-dead-${var.stage}",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam:::root"
      },
      "Action": "SQS:*",
      "Resource": "${aws_sqs_queue.dead.arn}"
    }
  ]
}
POLICY
}