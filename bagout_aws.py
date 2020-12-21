import boto3
import argparse
import os
from datetime import datetime

# Based on https://stackoverflow.com/a/56267603


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.description = "download an AWS S3 bucket into a bag structure"

    parser.add_argument("-b", "--bucket",
    help = "name of the AWS S3 bucket",
    required = True
    )
    parser.add_argument("-d", "--destination",
    help = "path to location to store the downloaded bucket",
    required = True
    )

    return parser


def get_folder_contents(service, bucket, local, prefix=''):
    files = {}
    dirs = {}
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = service.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for content in contents:
            path = content.get('Key')
            md5 = content.get('ETag').split('-')[0].strip('"')
            size = content.get('Size')
            if path[-1] != '/':
                files[path] = {
                    'md5': md5,
                    'size': size
                }
            else:
                dirs[path] = {
                    'md5': md5,
                    'size': size
                }
        next_token = results.get('NextContinuationToken')
    
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    
    for k in files:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        service.download_file(bucket, k, dest_pathname)

    return files


def make_manifest(bag_path, files):
    man_path = os.path.join(bag_path, 'manifest-md5.txt')

    with open(man_path, 'w') as f:
        for file, info in files.items():
            payload_path = os.path.join('data', file)
            f.write('{}  {}\n'.format(info['md5'], payload_path))


def make_baginfo(bag_path, files):
    tag_path = os.path.join(bag_path, 'baginfo.txt')

    total_files = 0
    total_bytes = 0
    for info in files.values():
        total_files += 1
        total_bytes += info['size']

    with open(tag_path, 'w') as f:
        f.write('Bag-Software-Agent: bagout_aws\n')
        f.write('Bagging-Date: {}\n'.format(datetime.today().strftime('%Y-%m-%d')))
        f.write('Payload-Oxum: {}.{}\n'.format(total_bytes, total_files))


def make_bagtxt(bag_path):
    tag_path = os.path.join(bag_path, 'bagit.txt')

    with open(tag_path, 'w') as f:
        f.write('BagIt-Version: 0.97\n')
        f.write('Tag-File-Character-Encoding: UTF-8\n')


def main():
    args = _make_parser().parse_args()

    #download files
    payload = os.path.join(args.destination, 'data')
    os.makedirs(payload)

    S3 = boto3.client('s3')

    try:
        files = get_folder_contents(S3, args.bucket, payload)
    except Exception as e:
        print(e)
        print ("The credentials have been revoked or expired, please re-run"
            "the application to re-authorize")

    make_manifest(args.destination, files)
    make_baginfo(args.destination, files)
    make_bagtxt(args.destination)

if __name__ == '__main__':
    main()
    
    