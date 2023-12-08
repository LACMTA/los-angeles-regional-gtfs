import os
import sys
import shutil
import zipfile

def process_zip_files_for_agency_id(agency_id):
    script_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    target_zip_files = None
    if agency_id is None:
        print('No agency_id provided.')
        sys.exit(1)
    if agency_id == 'lacmta':
        target_zip_files = get_latest_modified_zip_file(os.path.join(script_dir, 'lacmta/'), 'metro_api', agency_id)
        replace_and_archive_file(target_zip_files, os.path.join(script_dir, 'lacmta/current-base/gtfs_bus.zip'), os.path.join(script_dir, 'lacmta/current-base/archive'))
    if agency_id == 'lacmta-rail':
        target_zip_files = get_latest_modified_zip_file(os.path.join(script_dir, 'lacmta-rail/'), 'metro_api', agency_id)
    extract_zip_file_to_temp_directory(target_zip_files,agency_id)

def get_latest_modified_zip_file(path, target_schema, agency_id):
    print(path)
    if target_schema == 'metro_api_future':
        target_path = os.path.join(path, "future")
    elif target_schema == 'metro_api':
        if agency_id == 'lacmta-rail':
            target_path = os.path.join(path, "current")
        else:
            target_path = os.path.join(path, "current-base")
    else:
        print('Invalid target_schema provided.')
        sys.exit(1)
    if not os.path.exists(target_path):
        print('No such directory: ' + target_path)
        sys.exit(1)
    try:
        return max([os.path.join(target_path, f) for f in os.listdir(target_path) if f.endswith('.zip')], key=os.path.getmtime)
    except Exception as e:
        print('Error getting latest modified zip file: ' + str(e))
        sys.exit(1)


def replace_and_archive_file(source_file, target_file, archive_dir):
    # If the target file exists and is named 'gtfs_bus.zip', move it to the archive directory with a timestamp

    shutil.move(source_file, archive_dir)
    # Copy the source file to the target directory and rename it to 'gtfs_bus.zip'
    shutil.copy2(source_file, target_file)

def extract_zip_file_to_temp_directory(zip_file,agency_id):
    try:
        print('Extracting zip file to temp directory: ' + zip_file)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('../temp/'+agency_id)
    except Exception as e:
        print('Error extracting zip file to temp directory: ' + str(e))
        sys.exit(1)

process_zip_files_for_agency_id('lacmta')