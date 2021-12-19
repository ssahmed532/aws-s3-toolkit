
class NonExistentS3BucketError(Exception):
    """Raised when a specific, named S3 Bucket does not exist"""
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.message = 'S3 Bucket does not exist'
        super().__init__(self.message)


class AwsRegions:
    US_EAST1 = 'us-east-1'
    US_EAST2 = 'us-east-2'
    US_WEST1 = 'us-west-1'
    US_WEST2 = 'us-west-2'
    MIDDLE_EAST1 = 'me-south-1'
    SOUTH_AMERICA1 = 'sa-east-1'
    ASIA_PACIFIC_SOUTHEAST1= 'ap-southeast-1'
    EU_CENTRAL1 = 'eu-central-1'
    EU_WEST1 = 'eu-west-1'
    EU_WEST2 = 'eu-west-2'
    EU_WEST3 = 'eu-west-3'
    EU_SOUTH1 = 'eu-south-1'
    EU_NORTH1 = 'eu-north-1'
