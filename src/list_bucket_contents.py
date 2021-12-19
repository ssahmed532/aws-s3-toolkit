import boto3
import sys
import s3_utils


# TODO:
#   - for buckets that reside in a location other than the current
#     default / session region, automatically figure out where that
#     Bucket resides and then specify the LocationConstraint so that
#     we don't get the following sorts of Exceptions:
#
#       botocore.exceptions.ClientError: An error occurred (IllegalLocationConstraintException)
#       when calling the ListObjects operation: The unspecified location constraint is
#       incompatible for the region specific endpoint this request was sent to.
#


def main(bucket_name: str) -> None:
    bucket_location = s3_utils.get_bucket_location(bucket_name)

    bucket_contents = s3_utils.get_bucket_contents(bucket_name, bucket_location)
    if bucket_contents:
        for index, item in enumerate(bucket_contents, start=1):
            print(f'{index}. {item}')
    else:
        print(f'Bucket {bucket_name} is empty!')


if __name__ == "__main__":
    if (len(sys.argv) != 2):
        print(f'Usage: {sys.argv[0]} <S3 bucket name>', file=sys.stderr)
        sys.exit(1)

    bucket_name = sys.argv[1]

    main(bucket_name)
