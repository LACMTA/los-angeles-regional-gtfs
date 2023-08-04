import os
import sys
import zipfile

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
    if agency_id == 'lacmta':
        target_zip_files = get_latest_modified_zip_file(r'../lacmta/', 'future')
    if agency_id == 'lacmta-rail':
        target_zip_files = get_latest_modified_zip_file(r'../lacmta-rail/', 'current')
    extract_zip_file_to_temp_directory(target_zip_files,agency_id)

def extract_zip_file_to_temp_directory(zip_file,agency_id):
    try:
        print('Extracting zip file to temp directory: ' + zip_file)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('../temp/'+agency_id)
    except Exception as e:
        print('Error extracting zip file to temp directory: ' + str(e))
        sys.exit(1)


def main():
    process_zip_files_for_agency_id('lacmta')
    process_zip_files_for_agency_id('lacmta-rail')
    print('Here are the exctracted files: ')
    print(os.listdir('../temp/lacmta'))
    print(os.listdir('../temp/lacmta-rail'))
    print('************* Done *************')


if __name__ == '__main__':
    main()
    