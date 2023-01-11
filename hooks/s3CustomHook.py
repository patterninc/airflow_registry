from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from botocore.exceptions import ClientError


class S3CustomHook(AwsBaseHook):
    conn_type = 's3'
    hook_name = 's3CustomHook'


    def __init__(self, *args, **kwargs) -> None:
        kwargs['client_type'] = 's3'
        super().__init__(*args, **kwargs)

    def copy_object(self, bucket: str, prefix: str) -> object:
        try:
            paginator = self.get_conn().get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            key_dict = []
            for page in pages:
                for obj in page['Contents']:
                    self.get_conn().copy_object(
                        Bucket=bucket,
                        ContentEncoding='gzip',
                        CopySource=f"{bucket}/{obj['Key']}",
                        Key=obj['Key'],
                        MetadataDirective='REPLACE')
                    key_dict.append(obj['Key'])
                    
            return key_dict
        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e
