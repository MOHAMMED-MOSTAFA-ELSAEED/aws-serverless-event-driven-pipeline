import json
import os
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'AuditTrailGlobal')
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    logger.info(f"Processing Event: {json.dumps(event)}")
    try:
        # Extract payload parameters
        payload = event.get('detail', event)
        record_id = payload.get('record_id')
        status = payload.get('status')
        amount = payload.get('amount')
        
        # Data Integrity Validation Rules
        if not record_id or amount is None or amount < 0:
            raise ValueError(f"Invalid record schema or negative amount detected: {payload}")
            
        # Write to DynamoDB Global Table
        table.put_item(
            Item={
                'RecordID': str(record_id),
                'Status': str(status),
                'Amount': str(amount),
                'ProcessedRegion': os.environ.get('AWS_REGION', 'us-east-1')
            }
        )
        logger.info(f"[✔] SUCCESS: RecordID {record_id} successfully processed and stored.")
        return {'statusCode': 200, 'body': json.dumps({'message': 'Success', 'record_id': record_id})}

    except Exception as e:
        logger.error(f"[❌] ERROR: Pipeline processing failed - {str(e)}")
        raise e
