This instruction will focus on some key points to set up the pipeline.<br>

**SET UP S3 BUCKET**<br>

In this scenario, I use shopmart-sales-data as my S3 bucket. Inside the bucket, create the following folders for each stores. For example, `store_001` has four subfolder `raw/, processed/, errors/, analytics/` <br>
![Creat S3 bucket](image.png)
![Create subfolder](image-1.png)


**CREATE LAMBDA FUNCTION**<br>

Create an AWS Lambda Function using Python. The Lambda function is name "process" <br>
![Create Lambda Function](image-2.png)
After that, upload the `lambda_function.py` to the Lambda function <br>
![Code upload](image-3.png)

**CONFIGURE LAMBDA IAM PERMISSIONS**<br>

Attach an IAM role to the Lambda function with permission to: Read objects from S3, Wirte processed, error, and analytics files back to S3, Write logs to CloudWatch, Publish alerts to SNS (set up later). <br>

Required permissions include:
* s3:GetObject
* s3:PutObject
* s3:ListBucket
* logs:CreateLogGroup
* logs:CreateLogStream
* logs:PutLogEvents
* sns:Publish

**CONFIGURE S3 TRIGGER**<br>

Add an S3 trigger to the Lambda function. Use the following trigger configuration<br>
* Event type: `ObjectCreated (PUT)`
* Prefix: `store-001/raw`
* Suffix: 
![Configure S3 trigger](image-4.png)

**CONFIGURE SNS ALERTING**<br>

Create an Amazon SNS topic for data quality alerts. <br>
Subscribe the team email address to the topic and confirm the subcription in your email box.<br>
Add the SNS topic ARN as a Lambda environment variable: `SNS_TOPIC_ARN = arn:aws:sns:<region>:<account-id>:shopmart-data-alerts`

![Subcription](image-5.png)

**SET UP AMAZON ATHENA**<br>

Create an Athena database and external table for the files in the `processed/` folder.<br>
Athena can then be used to query the cleaned sales data directly from S3, such as daily revenue, top products, and payment success rate.
![Athena query](image-6.png)