import argparse
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow


# Based on https://github.com/vikynandha-zz/google-drive-backup/blob/master/drive.py


# CLIENT_SECRETS, name of a file containing the OAuth 2.0 information for this
# application, including client_id and client_secret, which are found
# on the API Access tab on the Google APIs
# Console <http://code.google.com/apis/console>
CLIENT_SECRETS = 'client_secrets.json'

# Helpful message to display in the browser if the CLIENT_SECRETS file
# is missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0
To make this sample run you will need to populate the client_secrets.json file
found at:
   %s
with information from the APIs Console <https://code.google.com/apis/console>.
""" % os.path.join(os.path.dirname(__file__), CLIENT_SECRETS)

# Set up a Flow object to be used if we need to authenticate.
FLOW = flow_from_clientsecrets(CLIENT_SECRETS,
    scope='https://www.googleapis.com/auth/drive',
    message=MISSING_CLIENT_SECRETS_MESSAGE)


def _make_parser():
	parser = argparse.ArgumentParser()
	parser.description = "download a Google Drive folder into a bag structure"

	parser.add_argument("-id", "--googledrive",
	help = "id number of the Google Drive folder, found at end of URL",
	required = True
	)
	parser.add_argument("-d", "--destination",
	help = "path to location to store the downloaded bucket",
	required = True
	)

	return parser


def ensure_dir(directory):
    if not os.path.exists(directory):
        log("Creating directory: %s" % directory)
        os.makedirs(directory)


def is_google_doc(drive_file):
    return True if drive_file['mimeType'].startswith('^application/vnd.google-apps') else False


def get_folder_contents(service, http, folder, base_path='./', depth=0):
    try:
        folder_contents = service.files().list(q="'%s' in parents" % folder['id']).execute()
    except:
        log("ERROR: Couldn't get contents of folder %s. Retrying..." % folder['title'])
        get_folder_contents(service, http, folder, base_path, depth)
        return
    folder_contents = folder_contents['items']
    dest_path = base_path

    def is_file(item):
        return item['mimeType'] != 'application/vnd.google-apps.folder'

    def is_folder(item):
        return item['mimeType'] == 'application/vnd.google-apps.folder'

    for item in filter(is_file, folder_contents):
        full_path = os.path.join(dest_path, item['title'].replace('/', '_'))
        download_file(service, item, full_path)

    for item in filter(is_folder, folder_contents):
        get_folder_contents(service, http, item, dest_path, depth+1)


def download_file(service, drive_file, dest_path):
    """Download a file's content.
    Args:
      service: Drive API service instance.
      drive_file: Drive File instance.
    Returns:
      File's content if successful, None otherwise.
    """
    if is_google_doc(drive_file):
        try:
            download_url = drive_file['exportLinks']['application/pdf']
        except KeyError:
            download_url = None
    else:
        download_url = drive_file['downloadUrl']
    if download_url:
        try:
            resp, content = service._http.request(download_url)
        except httplib2.IncompleteRead:
            log('Error while reading file %s. Retrying...' % drive_file['title'].replace('/', '_'))
            download_file(service, drive_file, dest_path)
            return False
        if resp.status == 200:
            try:
                target = open(file_location, 'w+')
            except:
                return False
            target.write(content)
            return True
        else:
            return False
    else:
        # The file doesn't have any content stored on Drive.
        return False


  


def main():
	args = _make_parser().parse_args()

	#check credentials
	storage = Storage('drive.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run(FLOW, storage)

	#download files
	payload = os.path.join(args.destination, 'data')
	os.makedirs(payload)

	http = httplib2.Http()
    http = credentials.authorize(http)
	with build('drive', 'v3', http=creds.authorize(Http())) as drive_service:
		try:
			start_folder = drive_service.files().get(fileId=FLAGS.drive_id).execute()
			get_folder_contents(drive_service, http, start_folder, payload)
		        
    	except AccessTokenRefreshError:
        	print ("The credentials have been revoked or expired, please re-run"
        		"the application to re-authorize")


if __name__ == '__main__':
    main()

	