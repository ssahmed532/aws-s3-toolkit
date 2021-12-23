import argparse
import boto3
import os
import sys

import hash_utils
import s3_utils

from datetime import timedelta
from timeit import default_timer as timer
from tqdm import tqdm
from commons import NonExistentS3BucketError


# TODO
#   - fix the issue when the current region (in config) is not the same
#     as the region in which the S3 Bucket is located.
#   - integrate the Python std logging module, and log all output to
#     an ondisk logfile
#   - modify this script such that the integrity hash files are an internal
#     implementation detail and nothing about them is visible except for when
#     file integrity verification fails
#   - add a verbose mode
#   - add a warning when trying to download an empty bucket
#   - Intelligently calculate an average transfer rate
#     (bytes downloaded / total transfer time) and display it at the end.
#   - [DONE] integrate the argparse module
#   - [DONE] by default, this script should download the contents of the
#     bucket into a local dir with the same name as the S3 bucket
#   - if a --dir <download dir/path> option is specified, then the contents
#     of the S3 bucket should be downloaded into that specified dir/path
#


class S3FileDownloader:
    """A utility class to download files from an S3 Bucket"""

    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name
        self.s3_resource = None
        self.hash_files = []


    def initialize(self) -> None:
        resource = boto3.resource('s3')

        if not s3_utils.check_bucket(resource, self.bucket_name):
            raise NonExistentS3BucketError(self.bucket_name)

        self.s3_resource = resource


    def _download_all_files(self, dir_path: str) -> int:
        files_downloaded = 0

        location = s3_utils.get_bucket_location(self.bucket_name)

        # TODO
        # the ListObjects() API call requires the location-constraint for the
        # S3 Bucket to be specified. For example:
        #   current region is us-east-1
        #   bucket whose contents are to be listed is in another region (me-south-1)
        #
        # Solution is to specify the exact same region in the call to ListObjects()
        # as where the S3 Bucket actually resides
        bucket_files = s3_utils.get_bucket_contents(self.bucket_name, location)

        os.mkdir(dir_path)

        for file in (progress_bar := tqdm(bucket_files, desc='Downloading files')):
            full_file_path = os.path.join(dir_path, file)
            self.s3_resource.Bucket(self.bucket_name).download_file(file, full_file_path)
            files_downloaded += 1
            progress_bar.write(full_file_path)
            split_tup = os.path.splitext(full_file_path)
            if split_tup[1] == '.hash':
                self.hash_files.append(full_file_path)

        return files_downloaded


    def download_all_files(self) -> int:
        return self._download_all_files(self.bucket_name)


    def download_all_files_into_dir(self, dir_path: str) -> int:
        target_dir = os.path.join(dir_path, self.bucket_name)

        # TODO:
        #   if the target_dir already exists, then error out

        print(f'Downloading all files into: {target_dir}')
        return self._download_all_files(target_dir)


    def verify_hashes(self) -> bool:
        verified_count = 0
        failed_count = 0

        for hash_filename in tqdm(self.hash_files, desc='Verifying integrity hashes'):
            if hash_utils.verify_integrity_hash_file(hash_filename):
                os.remove(hash_filename)
                verified_count += 1
            else:
                failed_count += 1

        return verified_count == len(self.hash_files)


def main(args: argparse.Namespace) -> None:
    if args.verbose:
        print(f'boto3 library version is {boto3.__version__}')
        print(f'Current region is {s3_utils.get_current_region()}')
        print()

    print(f'Downloading all files from S3 bucket: {args.s3_bucket_name}')

    file_downloader = S3FileDownloader(args.s3_bucket_name)
    try:
        file_downloader.initialize()
    except NonExistentS3BucketError as e:
        print(f'ERROR: cannot download file(s) from non-existent S3 bucket ({args.s3_bucket_name})', file=sys.stderr)
        sys.exit(2)

    start = timer()
    if args.dir:
        count = file_downloader.download_all_files_into_dir(args.dir)
    else:
        count = file_downloader.download_all_files()
    end = timer()

    elapsed_time = round(end - start, 3)
    print(f'Downloaded {count} files in time: {elapsed_time} seconds')

    verified = file_downloader.verify_hashes()
    if verified:
        print(f'All integrity hashes verified')
    else:
        print(f'WARNING: not all integrity hashes were verified')


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description='Script to download files from an S3 Bucket')

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
        '--dir',
        action='store',
        metavar='dir',
        required=False,
        type=str,
        help='directory within which to download the file or S3 Bucket'
        )

    args = arg_parser.parse_args()

    main(args)
