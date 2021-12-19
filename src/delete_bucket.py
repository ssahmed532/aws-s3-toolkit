import argparse
import boto3
import pprint
import sys

import commons
import s3_utils

from timeit import default_timer as timer
from botocore.exceptions import ClientError

PROMPT_MSG_EMPTY_BUCKET  = 'Are you sure you want to proceed with emptying the S3 bucket contents? [Y/N] '
PROMPT_MSG_DELETE_BUCKET = 'Are you sure you want to proceed with deleting the S3 bucket? [Y/N] '


def empty_out_bucket(bucket_name: str, location: str, verbose: bool) -> bool:
    """Empty out and delete the contents of the specified S3 bucket

    Args:
        bucket_name (str): name of the S3 bucket
        location (str): the location (region) the S3 bucket resides in
        verbose (bool): enable verbose output

    Returns:
        bool: True if the contents of the specified S3 bucket were
              successfully deleted, False otherwise
    """
    try:
        # this approach is taken from the following SO reference:
        #   https://stackoverflow.com/questions/43326493/what-is-the-fastest-way-to-empty-s3-bucket-using-boto3
        #
        # Note that this approach below will not work for S3 buckets
        # that have versioning enabled
        print(f'Emptying out & deleting the contents of S3 bucket {bucket_name} in location {location} ...')

        start = timer()
        s3_resource = boto3.resource('s3', region_name=location)
        bucket = s3_resource.Bucket(bucket_name)
        response  = bucket.objects.all().delete()
        end = timer()

        elapsed_time = round(end - start, 3)
        print(f'Emptied out bucket contents in {elapsed_time} seconds')

        if verbose:
            print('bucket.objects.all().delete() response:')
            pprint.pprint(response)
            print()

        return True
    except ClientError as e:
        print(f'S3 ClientError occurred while trying to empty out bucket:')
        print(f"\t{e.response['Error']['Code']}: {e.response['Error']['Message']}")
        return False


def delete_bucket(bucket_name: str, location: str, verbose: bool) -> bool:
    """Delete the specified S3 bucket

    Args:
        bucket_name (str): name of the S3 bucket
        location (str): the location (region) the S3 bucket resides in
        verbose (bool): enable verbose output

    Returns:
        bool: True if the specified S3 bucket was successfully deleted,
              False otherwise
    """
    try:
        print(f'Deleting S3 bucket {bucket_name} in location {location} ...')

        start = timer()
        s3_client = boto3.client('s3', region_name=location)
        response = s3_client.delete_bucket(Bucket=bucket_name)
        end = timer()

        elapsed_time = round(end - start, 3)
        print(f'Deleted bucket in {elapsed_time} seconds')

        if verbose:
            print('delete_bucket() response:')
            pprint.pprint(response)
            print()

        if response['ResponseMetadata']['HTTPStatusCode'] == 204:
            print(f'S3 bucket {bucket_name} successfully deleted')
            return True
    except ClientError as e:
        print(f'S3 ClientError occurred while trying to delete bucket:')
        print(f"\t{e.response['Error']['Code']}: {e.response['Error']['Message']}")
        return False


def user_confirm(prompt_msg: str = 'Are you sure you want to proceed? [Y/N] ') -> bool:
    answer = ""

    while answer not in ["y", "yes", "n", "no"]:
        answer = input(prompt_msg).lower()

    return (answer == "y") or (answer == "yes")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description='Script to delete an existing S3 Bucket')

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
        help='name of the S3 Bucket'
        )

    arg_parser.add_argument(
        "-y",
        "--yes",
        required=False,
        action="store_true",
        help="assume Yes to all confirmation prompts"
        )

    args = arg_parser.parse_args()

    if args.verbose:
        print(f'python version: {sys.version}')
        print(f'boto3 library version: {boto3.__version__}')
        print(f'Current region: {boto3.Session().region_name}')
        print()

    no_user_prompt = args.yes

    # get the location of the user-specified S3 bucket; without it the
    # delete bucket operation cannot proceed.
    bucket_location = None
    is_empty = False
    try:
        bucket_location = s3_utils.get_bucket_location(args.s3_bucket_name)
        print(f'bucket {args.s3_bucket_name} resides in {bucket_location}')

        # TODO: revisit this approach to checking if a bucket is empty
        is_empty = s3_utils.is_bucket_empty(args.s3_bucket_name, bucket_location)
    except ClientError as e:
        print(f'ERROR: unable to get location (region) for bucket {args.s3_bucket_name}')
        print(f"\t{e.response['Error']['Code']}: {e.response['Error']['Message']}")
        sys.exit(1)

    if no_user_prompt:
        if not is_empty:
            print(f'WARNING: proceeding with emptying out S3 bucket {args.s3_bucket_name} without prompt/confirmation ...')
            empty_out_bucket(args.s3_bucket_name, bucket_location, args.verbose)
            print()

        print(f'WARNING: proceeding with deleting S3 bucket {args.s3_bucket_name} without prompt/confirmation ...')
        delete_bucket(args.s3_bucket_name, bucket_location, args.verbose)
        print()
    else:
        if not is_empty and user_confirm(PROMPT_MSG_EMPTY_BUCKET):
            empty_out_bucket(args.s3_bucket_name, bucket_location, args.verbose)
        if user_confirm(PROMPT_MSG_DELETE_BUCKET):
            delete_bucket(args.s3_bucket_name, bucket_location, args.verbose)
