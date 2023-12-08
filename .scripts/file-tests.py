import os
import sys
import shutil
import zipfile
from pathlib import Path

TARGET_SCHEMA = 'metro_api'

def process_zip_files_for_agency_id(agency_id):
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    target_zip_files = None
    if agency_id is None:
        print('No agency_id provided.')
        sys.exit(1)
    if agency_id == 'lacmta':
        target_zip_files = get_latest_modified_zip_file(root_dir / 'lacmta/', 'metro_api', agency_id)
        replace_and_archive_file(target_zip_files, root_dir / 'lacmta/current-base/gtfs_bus.zip', root_dir / 'lacmta/current-base/archive')
    if agency_id == 'lacmta-rail':
        target_zip_files = get_latest_modified_zip_file(root_dir / 'lacmta-rail/', 'metro_api', agency_id)

    extract_zip_file_to_temp_directory(agency_id)

def get_latest_modified_zip_file(path, target_schema, agency_id):
    print(path)
    print(target_schema)
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
        return max([os.path.join(target_path, f) for f in os.listdir(target_path) if f.endswith('.zip') and f != 'gtfs_bus.zip'], key=os.path.getmtime)
    except Exception as e:
        print('Error getting latest modified zip file: ' + str(e))
        sys.exit(1)


def replace_and_archive_file(source_file, target_file, archive_dir):
    # If the target file exists and is named 'gtfs_bus.zip', move it to the archive directory with a timestamp

    shutil.copy2(source_file, archive_dir)
    # Copy the source file to the target directory and rename it to 'gtfs_bus.zip'
    # Assuming target_file is the full path to the file
    new_target_file = os.path.join(os.path.dirname(target_file), 'gtfs_bus.zip')

    # Delete the new_target_file if it exists
    if os.path.exists(new_target_file):
        os.remove(new_target_file)

    os.rename(source_file, new_target_file)

def extract_zip_file_to_temp_directory(agency_id):
    zip_file = None
    if agency_id == 'lacmta':
        if TARGET_SCHEMA == 'metro_api_future':
            zip_file = '../lacmta/future/gtfs_bus.zip'
        elif TARGET_SCHEMA == 'metro_api':
            zip_file = '../lacmta/current-base/gtfs_bus.zip'
    elif agency_id == 'lacmta-rail':
        zip_file = '../lacmta-rail/current/gtfs_rail.zip'
    try:
        print('Extracting zip file to temp directory: ' + zip_file)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('../temp/'+agency_id)
    except Exception as e:
        print('Error extracting zip file to temp directory: ' + str(e))
        sys.exit(1)

process_zip_files_for_agency_id('lacmta')