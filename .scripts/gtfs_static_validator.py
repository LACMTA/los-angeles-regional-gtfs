import os
import sys
import zipfile
import argparse



def get_latest_modified_zip_file(path,folder_branch):
    target_path = path +"/" + folder_branch
    if path is None:
        print('No path provided.')
        sys.exit(1)
    try:
        return max([target_path+'/'+f for f in os.listdir(target_path) if f.endswith('.zip')], key=os.path.getmtime)
    except Exception as e:
        print('Error getting latest modified zip file: ' + str(e))
        sys.exit(1)




def process_zip_files_for_agency_id(agency_id):
    target_zip_files = None
    if agency_id is None:
        print('No agency_id provided.')
        sys.exit(1)
    target_zip_files = get_latest_modified_zip_file('../'+agency_id+'/', folder_branch)
    print('target_zip_files: ' + target_zip_files)
    validate_gtfs_with_gtfs_java_cli(target_zip_files,agency_id, folder_branch)


def validate_gtfs_with_gtfs_java_cli(zip_file,agency_id,folder_branch):
    try:
        print('Validating GTFS with GTFS Java CLI: ' + zip_file)
        os.system('java -jar ./utils/gtfs-validator.jar -i ' + zip_file + ' -o ../.validation/'+agency_id+'/'+folder_branch+'/')
    except Exception as e:
        print('Error validating GTFS with GTFS Java CLI: ' + str(e))
        sys.exit(1)

def extract_zip_file_to_temp_directory(zip_file,agency_id):
    try:
        print('Extracting zip file to temp directory: ' + zip_file)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('../temp/'+agency_id)
    except Exception as e:
        print('Error extracting zip file to temp directory: ' + str(e))
        sys.exit(1)

def check_for_java_installation():
    try:
        print('Checking for Java installation...')
        os.system('java -version')
    except Exception as e:
        print('Error checking for Java installation: ' + str(e))
        print('Attempting to install Java...')
        if os.system == 'Windows':
            os.system('choco install openjdk')
        if os.system == 'Linux':
            os.system('sudo apt install default-jre')


parser = argparse.ArgumentParser(description='Validates GTFS Static Files.')
parser.add_argument('--agency_id', metavar='agency_id', type=str, nargs='+',
                    help='The agency_id of the gtfs.', required=True)
parser.add_argument('--folder_branch', metavar='folder_branch', type=str, nargs='+',
                    help='The state of the gtfs, can either be "current" or "future".', required=True)

args = parser.parse_args()
agency_id = args.agency_id[0]
folder_branch = args.folder_branch[0]


def main():
    check_for_java_installation()
    process_zip_files_for_agency_id(agency_id)
    print('************* Done *************')


if __name__ == '__main__':
    main()
    