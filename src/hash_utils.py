import hashlib
import os


# TODO:
#   - change the separator used in the integrity hash file from ':' to
#     '::' to make it entirely unlikely that it could be part of a valid
#     filename
#

HASHFILE_PART_SEP = ":"


def get_hash(filepath: str) -> str:
    with open(filepath, "rb") as f:
        hash_algo = hashlib.blake2b()
        while chunk := f.read(8192):
            hash_algo.update(chunk)

    file_hash = hash_algo.hexdigest()
    return file_hash


def create_integrity_hash_file(filepath: str) -> str:
    real_path = os.path.realpath(filepath)
    filename = os.path.basename(real_path)

    # the integrity hash of the specified file in hexadecimal
    file_hash = get_hash(real_path)

    hash_filepath = real_path + ".hash"

    try:
        with open(hash_filepath, "w+") as f:
            f.write(f'{filename}{HASHFILE_PART_SEP}{file_hash}')

        return hash_filepath
    except:
        print(f'ERROR: unable to write integrity hash to file {hash_filepath}')
        return ""


def verify_integrity_hash_file(hash_filepath: str) -> bool:
    with open(hash_filepath, "r") as f:
        line = f.readline()
        parts = line.split(HASHFILE_PART_SEP)

        if len(parts) != 2:
            print(f'ERROR: unable to parse malformed integrity hash file {hash_filepath}')
            return False

        filepath = os.path.join(os.path.dirname(hash_filepath), parts[0])

        hash_value = parts[1]

        return verify_hash(filepath, hash_value)


def verify_hash(filepath: str, expected_hash: str) -> bool:
    computed_hash = get_hash(filepath)

    return computed_hash == expected_hash
