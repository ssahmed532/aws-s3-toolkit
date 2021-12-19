import argparse
import boto3
import pprint
import s3_utils

from botocore.exceptions import ClientError
import commons


# TODO:
#   - add support for the following CLI arguments using argparse:
#       - auto-generate S3 bucket name using a specified prefix
#       - auto-generate S3 bucket name using internal, default prefix
#       - [DONE] new S3 bucket name
#       - [DONE] new AWS region in which the bucket has to reside (other than us-east-1)
#   - when the user has not specified a region that the bucket should
#     be created in, print the region in which the bucket was created
#     when successfully completed.
#   - allow for buckets to be created in regions other than the current
#     default region set in aws CLI configuration
#   - add a new CLI option (--show-locations or --show-regions) that
#     lists all supported AWS regions where a new S3 Bucket can be created in
#   - further tighten up security of the newly created S3 Bucket
#   - Block Public Access settings for the new S3 Bucket
#   - bucket creation methods should be moved into the s3_utils module
#   - add a new method to allow creating a bucket by adding the default
#     (or user-specified) prefix to an existing directory name. This will
#     be helpful when uploading a whole directory to a new bucket that will
#     be automatically created prior to uploading the directory.
#   - display total elapsed time to create the new S3 bucket
#   - add detailed logging via the Python standard logging module
#   - add verbose mode
#


args = None


def create_bucket(bucket_name: str, region: str = None) -> bool:
    """Create a new S3 bucket

    Args:
        bucket_name (str): the name of the new S3 bucket
        region (str, optional): the region or location the new Bucket
        should be created in. Defaults to None.

    Returns:
        bool: True if the S3 bucket was created successfully"""

    assert (len(bucket_name) > 0)

    global args

    if args.verbose:
        print(f'DEBUG: create_bucket(): new bucket name={bucket_name}, region={region}')

    s3_client = boto3.client('s3', region_name=region)

    try:
        if region and (region != commons.AwsRegions.US_EAST1):
            # there is a peculiar bug in boto3 such that when the current
            # region is us-east-1 (N. Virginia), the region should not
            # be specified in the create_bucket() API call using the
            # LocationConstraint attribute.
            # But for all other non us-east-1 regions, the region *HAS*
            # to be specified!
            print(f'Creating new bucket \"{bucket_name}\" in region \"{region}\"')
            location = {'LocationConstraint': region}
            response = s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
                )
        else:
            print(f'Creating new bucket \"{bucket_name}\"')
            response = s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f'S3 client error occurred while trying to create bucket:')
        print(f"\tError Code: {e.response['Error']['Code']}")
        print(f"\tError Msg:  {e.response['Error']['Message']}")
        return False

    if args.verbose:
        print('create_bucket() response:')
        pprint.pprint(response)
        print()

    try:
        # enable Server-side Encryption at the Bucket level
        # reference:
        #   https://stackoverflow.com/questions/59218289/s3-default-server-side-encryption-on-large-number-of-buckets-using-python-boto3
        response = s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}
                    },
                ]
            })
    except ClientError as e:
        print(f'S3 client error occurred while trying to enable encryption:')
        print(f"\tError Code: {e.response['Error']['Code']}")
        print(f"\tError Msg:  {e.response['Error']['Message']}")
        return False

    if args.verbose:
        print('put_bucket_encryption() response:')
        pprint.pprint(response)
        print()

    return True


def main(args: argparse.Namespace) -> None:
    if args.verbose:
        print(f'boto3 library version is {boto3.__version__}')
        print(f'Current region is {s3_utils.get_current_region()}')
        print()

    print(f'Name of new S3 bucket to create is: {args.s3_bucket_name}')

    bucket_region = None
    if args.location:
        # TODO:
        #   validate that the region requested is valid is in the list
        #   of Regions that this script supports
        bucket_region = args.location
        print(f'Location/region for new S3 bucket is: {bucket_region}')

    current_region = s3_utils.get_current_region()
    if not bucket_region and (current_region != commons.AwsRegions.US_EAST1):
        # If the current *session* / config region is NOT US_EAST1, then
        # you have to specify the region for the new S3 Bucket otherwise
        # the following IllegalLocationConstraintException will be thrown:
        #   The unspecified location constraint is incompatible for the region specific endpoint
        #
        # If the current *session* / config region is US_EAST1, then the
        # LocationConstraint does not have to be specified.
        bucket_region = current_region

    status = create_bucket(args.s3_bucket_name, bucket_region)
    if status:
        if bucket_region:
            print(f'S3 bucket created successfully in region {bucket_region}')
        else:
            print(f'S3 bucket created successfully')
        print(f"Name of newly created S3 bucket is {args.s3_bucket_name}")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description='Script to create a new S3 Bucket')

    arg_parser.add_argument(
        "-v",
        "--verbose",
        required=False,
        action="store_true",
        help="display verbose output"
        )

    arg_parser.add_argument(
        's3_bucket_name',
        type=str,
        help='name of the new S3 Bucket'
        )

    arg_parser.add_argument(
        '--location',
        action='store',
        metavar='location',
        required=False,
        type=str,
        help='location (region) for the new S3 Bucket'
        )

    args = arg_parser.parse_args()

    main(args)
