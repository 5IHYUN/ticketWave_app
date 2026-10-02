#!/bin/bash
# LocalStack 기동 시 SQS·S3 리소스 생성 (ready.d 훅, 컨테이너 시작마다 실행)
# AWS dev(Terraform modules/sqs)와 같은 구조
#   - booking-dlq   : 처리 실패 메시지 격리
#   - booking-queue : 5회 수신 후에도 처리 실패 시 DLQ 이동 (RedrivePolicy)
set -euo pipefail
REGION=ap-northeast-2
DLQ_ARN="arn:aws:sqs:${REGION}:000000000000:booking-dlq"   # LocalStack 기본 계정 ID 고정값

awslocal sqs create-queue --queue-name booking-dlq --region "$REGION" >/dev/null
awslocal sqs create-queue --queue-name booking-queue --region "$REGION" \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}" >/dev/null
awslocal s3 mb s3://team5-dev-poster-bucket --region "$REGION" >/dev/null

echo "LocalStack SQS booking-queue(+booking-dlq) and S3 team5-dev-poster-bucket created successfully."
