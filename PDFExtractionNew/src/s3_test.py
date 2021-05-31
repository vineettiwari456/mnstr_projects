import boto3

# from botocore.exceptions import ClientError
#
bucket_name = "lunge-data-pdfs"
import boto3,os
AWS_ACCESS_KEY_ID = "AKIAQDTI4BOM45WFPVP4"
AWS_SECRET_ACCESS_KEY = "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"
# session = boto3.Session(
#     aws_access_key_id=AWS_ACCESS_KEY_ID,
#     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
# )
# s3_connection = session.resource('s3')
# bucket_location = s3_connection.get_bucket_location(Bucket=bucket_name)
# print(bucket_location)
# source_path = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads\\mckinsey-global-private-markets-review-2020-v4.pdf"
# file_name = "mckinsey-global-private-markets-review-2020-v4.pdf"
source_path = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\\PDFExtractor\\\downloads\\FAQs-for-candidates.pdf"
file_name ="FAQs-for-candidates.pdf"
# s3_connection.meta.client.upload_file(source_path,bucket_name,file_name)
# s3_connection.Bucket(bucket_name).put_object(key=file_name,body=source_path)

s3_connection=boto3.client("s3",aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_connection.upload_file(Bucket=bucket_name,Filename=source_path,Key=file_name)

# my_bucket = s3_connection.Bucket(bucket_name)
# for s3_object in my_bucket.objects.all():
#     # Need to split s3_object.key into path and file name, else it will give error file not found.
#     path, filename = os.path.split(s3_object.key)
#     print(path)
#     print(filename)