import boto3
import os
import sys

from datetime import timedelta
from timeit import default_timer as timer
from tqdm import tqdm
from commons import NonExistentS3BucketError

import hash_utils
import s3_utils

# TODO
#   - integrate the argparse module
#   - integrate the Python std logging module, and log all output to
#     an ondisk logfile
#   - when uploading a single file > 50 Mb, display a progress bar
#     showing file upload progress based on the size of the file.
#   - add an option for auto-creating an S3 bucket based on a
#     user-specified bucket prefix
#   - when uploading the contents of an entire directory, generate a
#     manifest file that lists all of the files and their respective
#     hashes
#   - prompt when a file with the same key already exists in the target
#     bucket, only proceed if the user chooses to overwrite those files
#   - add a verbose mode
#   - add an optional flag that adds an extra check/verification step to
#     verify that a file was successfully uploaded to the bucket. Should
#     be optional as this will slow down the end-to-end upload time.
#   - add an option to recurse into all sub-directories when uploading
#     files. If not specified, only upload those files in the root of
#     the specified directory.
#   - calculate and display the average upload speed (Kb / Mb per second)
#   - add an option to skip (cryptographic) hash generation
#   - add an option to only upload a certain type of files (e.g. only PDFs,
#     only .txt files, etc)
#   - [DONE] convert this into a Class
#   - [DONE] add total elapsed time logging
#


class S3FileUploader:
    """A utility class to upload files to an existing S3 Bucket. An instance
       of this class is bound to a specified, named S3 bucket at the
       time of instantiation."""

    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name
        self.s3_resource = None


    def initialize(self) -> None:
        resource = boto3.resource('s3')

        if not s3_utils.check_bucket(resource, self.bucket_name):
            raise NonExistentS3BucketError(self.bucket_name)

        self.s3_resource = resource


    def _upload_file_to_s3_bucket(self, file_path: str, calc_hash: bool) -> bool:
        """Upload specified file to S3 bucket (internal helper)

        Args:
            file_path (str): full path to file to be uploaded
            calc_hash (bool): if True, compute the integrity hash for the file

        Returns:
            bool: True if file was uploaded successfully
        """
        success = True

        real_path = os.path.realpath(file_path)
        filename = os.path.basename(real_path)

        self.s3_resource.Bucket(self.bucket_name).upload_file(Filename=real_path, Key=filename)

        if calc_hash:
            # TODO
            #   - instead of uploading a separate .hash file for each data file
            #     being uploaded, consider using the Object Metadata
            #   - and store the computed hash in a custom Key-Value pair in this
            #     Object Metadata
            hash_filepath = hash_utils.create_integrity_hash_file(file_path)

            self.s3_resource.Bucket(self.bucket_name).upload_file(Filename=real_path + ".hash", Key=filename + ".hash")

            try:
                os.remove(hash_filepath)
            except:
                print(f'ERROR: unable to delete integrity hash file {hash_filepath}')

        return success


    def upload_dir_contents(self, dir_path: str) -> int:
        """Upload all of the files contained in a directory to the S3 bucket

        Args:
            dir_path (str): full path to the directory

        Returns:
            int: number of files successfully uploaded
        """
        files_uploaded = 0

        # Iterate over all files in the specified directory and upload
        # them to this S3 bucket
        # TODO: replace os.walk() with os.scandir()
        for subdir, dirs, files in os.walk(dir_path):
            for filename in (progress_bar := tqdm(files, desc='Uploading files')):
                file_path = subdir + os.sep + filename
                self._upload_file_to_s3_bucket(file_path, True)
                files_uploaded += 1
                progress_bar.write(filename)

        return files_uploaded


    def upload_file(self, file_path: str) -> None:
        """Upload a file to the S3 bucket

        Args:
            file_path (str): full path to the file
        """
        print(f"Uploading file {file_path} to S3 Bucket {self.bucket_name} ... ")
        self._upload_file_to_s3_bucket(file_path, True)
        print(f"Done.")


def main(dir_path: str, s3_bucket_name: str, is_file: bool) -> None:
    file_uploader = S3FileUploader(s3_bucket_name)
    try:
        file_uploader.initialize()
    except NonExistentS3BucketError as e:
        print(f'ERROR: cannot upload file(s) to non-existent S3 bucket ({s3_bucket_name})', file=sys.stderr)
        sys.exit(2)

    start = timer()
    if is_file:
        file_uploader.upload_file(dir_path)
    else:
        files_uploaded = file_uploader.upload_dir_contents(dir_path)
        print(f'{files_uploaded} files uploaded successfully')
    end = timer()

    elapsed_time = round(end - start, 3)
    print(f'Elapsed time: {elapsed_time} seconds')


if __name__ == "__main__":
    if (len(sys.argv) < 3):
        print(f'Usage: {sys.argv[0]} <directory path> <S3 bucket name>', file=sys.stderr)
        sys.exit(1)

    fs_path = sys.argv[1]

    is_file = os.path.isfile(fs_path)

    if not is_file and not os.path.isdir(fs_path):
        print(f'ERROR: invalid or non-existent path {fs_path} to upload to AWS S3 bucket', file=sys.stderr)
        sys.exit(1)

    main(fs_path, sys.argv[2], is_file)
