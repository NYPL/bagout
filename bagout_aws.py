import botocore
import boto3
import argparse

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


def get_folder_contents(service, bucket, local, prefix=None):
	keys = []
    dirs = []
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
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)


def main():
	args = _make_parser().parse_args()

	#download files
	payload = os.path.join(args.destination, 'data')
	os.makedirs(payload)

	S3 = boto3.resource('s3')

	try:
		get_folder_contents(S3, bucket, payload)
   	except AccessTokenRefreshError:
       	print ("The credentials have been revoked or expired, please re-run"
       		"the application to re-authorize")
	
	