#
# Stores the space repository admin files in SVN.
# The structures is:
#   svn_dir
#       Backup Repo files
#           Region
#              APAC1
#              APAC2
#              EU1
#              US
#                   <space name>
#                           <repo file name><space name>.<repo file extension>
#                       .
#                       .
#
# John van Dijk - Birst 2019
#
# Version       Date            Who                Comment
# 1             08-11-2018      John van Dijk      Initial Version

# Package usage
# --------------------------
# packages included with python installation
import logging
import os
import sys
import subprocess
import getpass
import shutil
from datetime import datetime
from optparse import OptionParser
import time

# packages not included with python installation
# this script requires the Tortoise (SVN) CLI. (command line interface). 
# when installed, svn cli can be changed in Control Panel\All Control Panel Items\Programs and Features\TortoiseSVN*:
# - Select Change.  
# - Select Modify
# - Enable the Command Line Interface

from zeep import \
    Client  # to install, run command: pip install zeep, to update, run command: python -m pip install --upgrade zeep
from CommonFunctions import admin_get_valid_base_urls, admin_login, admin_logout, admin_make_space_current, \
    admin_service_isRepositoryAdmin
from CommonFunctions import admin_upload_repository_files, check_url_is_valid, create_new_space, get_base_url, \
    get_execution_mode_simulation
from CommonFunctions import get_execution_mode_update, get_output_mode_hide, get_output_mode_show, \
    get_repo_download_def
from CommonFunctions import get_space_copy_from_def, get_copy_from_to_spaces, get_repo_download_upload_def, \
    get_repo_upload_def_keys
from CommonFunctions import get_space_copy_to, get_space_copy_to_multi, get_space_copy_url, get_space_id, \
    is_space_available, send_mail, set_space_copy_to
from CommonFunctions import space_exists, svn_path_exists, update_copy_config


# Variables
# ----------

# Constants
# --------------------------

# Functions
# --------------------------
def svn_export_backup(svn_url, backup_dir):
    export_error = ''
    export_out = ''

    # retrieve (export) the repo files from SVN for space.
    with open('svn_out.txt', 'w+') as fout:
        with open('svn_error.txt', 'w+') as ferr:
            subprocess.call(
                'svn export "' + svn_url + '/' + backup_dir + '" ' + '"' + backup_dir + '" --force',
                stdout=fout, stderr=ferr)
            ferr.seek(0)
            export_error = ferr.read()
            if not export_error:
                fout.seek(0)
                export_out = fout.read()

    os.remove('svn_error.txt')
    os.remove('svn_out.txt')

    return export_error, export_out


# global svn_dir_list
def check_spaces_in_svn(backup_dir, base_url, space_list, valid_base_urls):
    error = ''
    missing_spaces_in_svn = {}

    svn_url = get_repo_base_url()

    if base_url not in valid_base_urls.keys():
        error = 'Url ' + base_url + ' is not a valid base url'
    else:
        region = valid_base_urls[base_url][0]
        region_dir = backup_dir + '/' + 'Region' + '/' + region
        # print('region dir: ', region_dir)

        # get list of svn content, starting from LN.BIRST
        with open('svn_out.txt', 'w+') as fout:
            with open('svn_error.txt', 'w+') as ferr:
                subprocess.call(
                    'svn list "' + svn_url + '/' + region_dir + '"',
                    stdout=fout, stderr=ferr)
                # print('svn command: ',
                #       'svn list "' + svn_url + '/' + region_dir + '"')

                ferr.seek(0)
                error = ferr.read()
                if not error:
                    fout.seek(0)
                    svn_dir_list = fout.read().rstrip().split('\n')

        for spack_to_check in space_list:
            spack_to_check_slash = spack_to_check + '/'
            if spack_to_check_slash not in svn_dir_list and spack_to_check_slash not in missing_spaces_in_svn:
                missing_spaces_in_svn[spack_to_check] = region_dir

    return (error, missing_spaces_in_svn)


# Code
# ---------------------------	 
# Init variables
user_name = ''
password = ''
wsdl_url = ''
soap_session = ''
download_url = ''
upload_url = ''
copy_config = {}
download_spaces = []
upload_spaces = []
nr_downloaded_files = 0
nr_spaces_created = 0
space_prefix = ''
backup_dir = ''
download_dir = ''
mail_attachments = []
mail_recipients = ''
generic_recipients = ''
generic_svn_dir = ''
mail_server = ''
mail_sender = ''
mail_password = ''
REPO_BASE_URL = ''
repo_svn_dir = ''
svn_username = ''
svn_password = ''

# Get input args
try:

    # parser = OptionParser()
    # parser.add_option("-u", "--user_name", dest="user_name",
    #                   help="Birst Space Login User Name.", metavar="LOGINUSERNAME")
    # parser.add_option("-p", "--password", dest="password",
    #                   help="Birst Space Login Password.", metavar="LOGINPASSWORD")
    # parser.add_option("-c", "--config_file", dest="config_file",
    #                   help="Config file with tags in section [BackRepo2SVN.py]. Use double quotes.",
    #                   metavar="CONFIGFILE")
    # parser.add_option("-m", "--execution_mode", dest="execution_mode",
    #                   help="Execution mode: s(imulation) or u(pdate). Default is simulation mode. ",
    #                   metavar="EXECUTIONMODE")
    # parser.add_option("-l", "--result_log_file", dest="result_log_file",
    #                   help="Path to log file with logging output. Use double quotes.", metavar="RESULTLOGFILE")
    # parser.add_option("-o", "--output", dest="output",
    #                   help="Show / hide console output. Default show console output.", metavar="OUTPUT")
    # parser.add_option("-b", "--backup_mode", dest="backup_mode",
    #                   help="Backup mode, as defined in config file", metavar="BACKUPMODE")

    # Check that parameters have been passed
    # (options,args) = parser.parse_args()
    # password = options.password
    # user_name = options.user_name
    # config_file = options.config_file
    # result_log_file = options.result_log_file
    # execution_mode = options.execution_mode
    # output_mode = options.output
    # backup_mode = options.backup_mode

    # Check that parameters have been passed
    # (options,args) = parser.parse_args()
    config_file = 'birst_config.txt'
    execution_mode = 'update'

    with open('birst_config.txt') as f:
        for line in f:
            if line.startswith('username = '):
                user_name = line.partition('=')[2].strip()

            if line.startswith('password = '):
                password = line.partition('=')[2].strip()

            if line.startswith('result_log_file = '):
                result_log_file = line.partition('=')[2].strip()

            if line.startswith('output_mode = '):
                output_mode = line.partition('=')[2].strip()

            if line.startswith('backup_mode = '):
                backup_mode = line.partition('=')[2].strip()

            if line.startswith('repo_base_url = '):
                REPO_BASE_URL = line.partition('=')[2].strip()

            if line.startswith('repo_svn_dir = '):
                repo_svn_dir = line.partition('=')[2].strip()

            if line.startswith('svn_username = '):
                svn_username = line.partition('=')[2].strip()

            if line.startswith('svn_password = '):
                svn_password = line.partition('=')[2].strip()

            if line.startswith('guids = '):
                j = 0
                list1 = line.partition('=')[2].strip().split(',')
                list2 = list(map(lambda x: x.strip(), list1))
                for i in admin_get_valid_base_urls():
                    admin_get_valid_base_urls()[i][1] = list2[j]
                    j = j + 1



    # print('connection guids :',admin_get_valid_base_urls())
    if not user_name:
        exit_code = 2
        raise RuntimeError('user_name is not given in config file. Missing tag: user_name')

    if not password:
        exit_code = 2
        raise RuntimeError('password is not given in config file. Missing tag: password')

    if not result_log_file:
        exit_code = 2
        raise RuntimeError('File for logging is not given in config file. Missing tag:result_log_file')

    if not backup_mode:
        exit_code = 2
        raise RuntimeError('backupmode is not given in config file. Missing tag: backup_mode')

    if not execution_mode:
        execution_mode = get_execution_mode_simulation()
    else:
        if execution_mode[0:1].lower() != 'u':
            execution_mode = get_execution_mode_simulation()
        else:
            execution_mode = get_execution_mode_update()

    if not output_mode:
        output_mode = get_output_mode_show()
    else:
        if output_mode[0:1].lower() != 'h':
            output_mode = get_output_mode_show()
        else:
            output_mode = get_output_mode_hide()

    if not REPO_BASE_URL:
        exit_code = 2
        raise RuntimeError('repo_base_url is not given in config file. Missing tag: repo_base_url')

    if not repo_svn_dir:
        exit_code = 2
        raise RuntimeError('repo_svn_dir is not given in config file. Missing tag: repo_svn_dir')

    # if not svn_username:
    #     exit_code = 2
    #     raise RuntimeError('svn_username is not given in config file. Missing tag: svn_username')
    #
    # if not svn_password:
    #     exit_code = 2
    #     raise RuntimeError('svn_password is not given in config file. Missing tag: svn_password')

    try:
        if result_log_file:
            # Set up logging to file. Overwrites previous log file.
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(lineno)d %(levelname)s: %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p',
                filemode='w',
                filename=result_log_file)

        # set up logging to console
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s: %(message)s')
        console.setFormatter(formatter)
        # add the handler to the root logger
        if output_mode == get_output_mode_show():
            logging.getLogger('').addHandler(console)

        logger = logging.getLogger('')

    except Exception as e:
        print("An unexpected error has occurred setting up logging ", sys.exc_info()[0])
        print(e)
        sys.exit(1)

except Exception as e:
    print("An unexpected error has occurred initialising input args ", sys.exc_info()[0])
    print(e)
    sys.exit(exit_code)

    logger.info('Processed input args...')


def get_repo_base_url():
    return REPO_BASE_URL


try:
    current_datetime = datetime.now()
    copy_log_file = result_log_file.partition('.')[0] + current_datetime.strftime('_%Y%m%d_%H%M%S') + '.' + \
                    result_log_file.partition('.')[2]

    logger.info('Backup mode = ' + backup_mode.upper())
    # logger.info('Execution mode = ' + execution_mode.upper())
    logger.info('Birst User name = ' + user_name)

    with open(config_file) as f:
        for line in f:
            if line.startswith('mail_server = '):
                mail_server = line.partition('=')[2].strip()

            if line.startswith('mail_sender = '):
                mail_sender = line.partition('=')[2].strip()

            if line.startswith('mail_password = '):
                mail_password = line.partition('=')[2].strip()

            if line.startswith('generic_mailrecipients = '):
                generic_recipients = line.partition('=')[2].strip()

            if line.startswith('generic_svn_dir = '):
                generic_svn_dir = line.partition('=')[2].strip()

            if line.startswith('ufs_mailrecipients = '):
                mail_recipients = line.partition('=')[2].strip()

            if line.startswith('copy_'):
                update_copy_config(backup_mode, line, copy_config)

    if not mail_server:
        exit_code = 2
        raise RuntimeError('No mail server defined for sending the new queries. Missing tag: mail_server')

    if not mail_sender:
        exit_code = 2
        raise RuntimeError('No mail sender defined for sending the new queries. Missing tag: mail_sender')

    if not mail_password:
        exit_code = 2
        raise RuntimeError('No mail sender password defined for sending the new queries. Missing tag: mail_password')

    if not mail_recipients:
        exit_code = 2
        raise RuntimeError('No mail recipients defined for sending the new queries. Missing tag: b2s_mailrecipients')

    if generic_recipients:
        mail_recipients = generic_recipients

    if generic_svn_dir:
        backup_dir = generic_svn_dir
    else:
        backup_dir = repo_svn_dir

    copy_urls, errors = get_space_copy_url(copy_config)
    if errors:
        for error in errors:
            logger.info(error)

        exit_code = 2
        raise RuntimeError('Backup mode ' + backup_mode + ' is incorrect. Incorrect tag: copy_url')

        # check both the download and upload urls are defined with tag: copy_url
    if len(copy_urls) != 2:
        exit_code = 2
        raise RuntimeError(
            'Backup mode ' + backup_mode + ' is incorrect. Missing download_url and upload_url in tag: copy_url')

    download_url = copy_urls[0]
    if not download_url:
        exit_code = 2
        raise RuntimeError('Backup mode ' + backup_mode + ' is incorrect. Missing download_url in tag: copy_url')

    download_wsdl_url = download_url + '/CommandWebService.asmx?WSDL'
    upload_url = copy_urls[1]
    if not upload_url:
        exit_code = 2
        raise RuntimeError('Backup mode ' + backup_mode + ' is incorrect. Missing upload_url in tag: copy_url')
    upload_wsdl_url = upload_url + '/CommandWebService.asmx?WSDL'

    logger.info('Download from URL = ' + download_url)
    logger.info('Upload to URL = ' + upload_url)
    logger.info(' ')

    # Create zeep client from download wsdl 
    soap_client = Client(wsdl=download_wsdl_url)
    login_token = soap_client.service.Login(user_name, password)
    logger.info('Login ' + download_url + ' with zeep Successful')

    logger.info('Retrieving space info on ' + download_url)
    # get the list of spaces using the listspaces ws
    array_of_download_spaces = soap_client.service.listSpaces(login_token)

    # Logout from Birst
    if login_token:
        logger.info('Logout zeep ...')
        try:
            soap_client.service.Logout(login_token)
            time.sleep(5)
        except Exception as e:
            logger.error('Logout zeep failed')

    # Create zeep client from upload wsdl 
    soap_client = Client(wsdl=upload_wsdl_url)
    login_token = soap_client.service.Login(user_name, password)
    logger.info('Login ' + upload_url + ' with zeep Successful')

    # Log into BWS for admin web services
    # login to app2103 !!
    time.sleep(5)
    soap_session = admin_login(user_name, password, upload_url)
    logger.info('Admin Login Successful')
    logger.info(' ')

    # get the list op spaces using the listspaces ws
    array_of_upload_spaces = soap_client.service.listSpaces(login_token)
    # print('Array of Upload Spaces : ', array_of_upload_spaces)
    # Have to change the code if the array_of_upload_spaces is empty
    if array_of_upload_spaces is None:
        array_of_upload_spaces = []
    space_to_multi = get_space_copy_to_multi(copy_config)
    for copy_to_num, copy_to_space in enumerate(space_to_multi, start=1):

        copy_config = set_space_copy_to(copy_config, copy_to_space)
        copy_from_to_space_names, error = get_copy_from_to_spaces(copy_config, '', array_of_upload_spaces)
        # print('Copy from to space names: ', copy_from_to_space_names)

        for from_to_key in copy_from_to_space_names.keys():
            download_spaces.append(copy_from_to_space_names[from_to_key][0].replace('/', ''))
            upload_spaces.append(copy_from_to_space_names[from_to_key][1].replace('/', ''))

        # print('Upload Spaces : ', upload_spaces)
        # print('Downlaod Spaces: ', download_spaces)

    valid_base_urls = admin_get_valid_base_urls()

    # Check download url is a valid url
    if download_url not in valid_base_urls.keys():
        exit_code = 2
        raise RuntimeError('Download url is not valid: ' + download_url)

    # Check upload url is a valid url
    if upload_url not in valid_base_urls.keys():
        exit_code = 2
        raise RuntimeError('Upload url is not valid: ' + upload_url)

    # Check download space for download url are available in SVN
    check_result = check_spaces_in_svn(backup_dir, download_url, download_spaces, valid_base_urls)
    # print(check_result)
    if check_result[0]:  # error
        exit_code = 2
        raise RuntimeError(check_result[0])

    if check_result[1].keys():  # missing spaces in SVN
        for missing_space_key in check_result[1].keys():
            logger.info('   ')
            logger.info('Space: ' + missing_space_key + ', not in SVN directory: ' + check_result[1][
                missing_space_key] + ' (url: ' + download_url + ')')

        exit_code = 2
        raise RuntimeError('Spaces missing in SVN')

    nr_upload_spaces_found = 0
    # check whether the spaces in the config file are available
    for num, space_to_upload in enumerate(upload_spaces):
        if space_exists(space_to_upload, array_of_upload_spaces):
            nr_upload_spaces_found += 1
            if nr_upload_spaces_found == 1:
                logger.info(' ')
                logger.info('Error: space(s) already exist at ' + upload_url)
            logger.info('    ' + space_to_upload)

    # Have to change the logic code should work even though the spaces are already present.
    if execution_mode == get_execution_mode_update():
        if nr_upload_spaces_found > 0:
            exit_code = 2
            raise RuntimeError('Spaces are present')

    svn_url = get_repo_base_url()

    download_upload_def = get_repo_download_upload_def()
    # get upload order with keys.
    upload_defs_keys = get_repo_upload_def_keys()

    # create export directory when not existing.
    current_dir = os.getcwd()
    os.makedirs(backup_dir, exist_ok=True)

    export_error, export_out = svn_export_backup(svn_url, backup_dir)
    if export_error:
        exit_code = 2
        raise RuntimeError(export_error)

    if not export_out:
        exit_code = 2
        raise RuntimeError('Nothing exported from SVN')

    space_defs_upload_files = {}
    region_dir = os.path.join(current_dir, backup_dir, 'Region', valid_base_urls[download_url][0])
    # check repos file are present after export, for uploading.
    for num_space, download_space in enumerate(download_spaces):
        download_dir = os.path.join(region_dir, download_space)
        upload_files = {}
        if os.path.isdir(download_dir):
            for def_key in upload_defs_keys:
                file_name = download_upload_def[def_key][0].partition('.')[0] + '_' + download_space + '.' + \
                            download_upload_def[def_key][0].partition('.')[2]
                upload_file = os.path.join(download_dir, file_name)
                # for windows OS have to change \ to / or use raw string (r)
                if os.path.isfile(upload_file):
                    upload_files[def_key] = upload_file

            space_defs_upload_files[upload_spaces[num_space]] = upload_files
    # testing upload files
    # print(upload_files)
    download_upload_space_ids = {}

    if execution_mode == get_execution_mode_update():
        user_details_result = admin_service_isRepositoryAdmin(soap_session)
        soap_session = user_details_result[0]
        if user_details_result[2]:  # error
            exit_code = 2
            raise RuntimeError(error)

        user_is_repo_admin = user_details_result[1]
        if user_is_repo_admin != 'true':
            exit_code = 2
            raise RuntimeError('User ' + user_name + ' is not a Repository Admin !!')
        else:
            logger.info('user ' + user_name + ' is a Repository Admin')
            logger.info(' ')

    nr_spaces_created = 0
    for num, upload_space in enumerate(upload_spaces):
        # # create the spaces at upload url 
        if nr_spaces_created == 0:
            logger.info('Creating space(s):')

        logger.info('    space [' + str(num) + ']: ' + upload_space)
        if execution_mode == get_execution_mode_update():
            upload_space_id = create_new_space(soap_client, login_token, upload_space, 'Created for cross site.')
            time.sleep(2)
            download_space = download_spaces[num]
            download_space_id = get_space_id(array_of_download_spaces, download_space)
            download_upload_space_ids[download_space_id] = []
            download_upload_space_ids[download_space_id].append(upload_space)
            download_upload_space_ids[download_space_id].append(upload_space_id)

            array_of_upload_spaces = soap_client.service.listSpaces(login_token)
            # print("Array of upload files after creating spaces: ", array_of_upload_spaces)
            # time.sleep(2)
            nr_spaces_created += 1
        else:
            break

    nr_files_uploaded = 0
    nr_packages_repointed = 0
    nr_connections_repointed = 0
    current_space = ''
    for num, upload_space in enumerate(upload_spaces, start=1):
        logger.info(' ')
        upload_space_id = get_space_id(array_of_upload_spaces, upload_space)
        logger.info('#########################################################################')
        logger.info('Url: ' + upload_url + ', upload space[' + str(num) + ']: ' + upload_space)
        logger.info('       space_id:' + upload_space_id)
        logger.info('#########################################################################')

        if execution_mode == get_execution_mode_update():
            current_space = upload_space
            logger.info('Current space: ' + current_space)

            upload_space_id = get_space_id(array_of_upload_spaces, upload_space)
            upload_result = admin_upload_repository_files(soap_session, download_upload_space_ids, download_url,
                                                          upload_url, upload_space, upload_space_id,
                                                          space_defs_upload_files, logger)
            # time.sleep(5)
            soap_session = upload_result[0]
            if upload_result[1]:  # error
                exit_code = 2
                raise RuntimeError(error)

            array_of_sources = soap_client.service.getSourcesList(login_token, upload_space_id)
            nr_files_uploaded += upload_result[2]
            repoint_connection = upload_result[3]
            repoint_packages = upload_result[4]

            if repoint_packages.keys() or repoint_connection.keys():
                if current_space == upload_space:
                    # prevent repointing in the upload space, otherwise repointing will fail.
                    # print('Download Space IDS: ', download_upload_space_ids)
                    for space_name_id in download_upload_space_ids.values():

                        if space_name_id[1] != upload_space_id:
                            body_sub_elements = {}
                            body_sub_elements['spaceID'] = space_name_id[1]
                            current_space = space_name_id[0]
                            body_sub_elements['spaceName'] = current_space
                            logger.info('Current space: ' + current_space)
                            admin_result = admin_make_space_current(soap_session, current_space, space_name_id[1])
                            soap_session = admin_result[0]
                            if admin_result[1]:  # error
                                exit_code = 2
                                raise RuntimeError(error)
                            break

                if repoint_connection.keys():
                    logger.info('    repointing connection:')
                    from_connection_id = repoint_connection[upload_space][0]
                    to_connection_id = repoint_connection[upload_space][1]
                    # print('From Connection ID :', from_connection_id)
                    # print('To Connection ID: ',to_connection_id)
                    logger.info(
                        '        ' + upload_space + ': repointconnection ' + upload_space_id + ' ' + from_connection_id + ' ' + to_connection_id)
                    try:
                        repointConnectionResponse = soap_client.service.repointConnection(login_token, upload_space_id,
                                                                                          from_connection_id,
                                                                                          to_connection_id)
                        nr_connections_repointed += 1
                    except Exception as e:
                        logger.info('Repoint Connection Error :', e)
                        print("Error Occured during Repointing Connections!!")

                if repoint_packages.keys():
                    repointing_shown = False
                    for repoint_package_key, repoint_package_value in repoint_packages.items():
                        if not repointing_shown:
                            logger.info('    repointing packages:')
                            repointing_shown = True

                        repoint_from_space_id = repoint_package_value[0]
                        repoint_to_space_id = repoint_package_value[1]
                        logger.info(
                            '         ' + repoint_package_key + ': repointpackages ' + upload_space_id + ' ' + repoint_from_space_id + ' ' + repoint_to_space_id)
                        repointPackagesResponse = soap_client.service.repointPackages(login_token, upload_space_id,
                                                                                      repoint_from_space_id,
                                                                                      repoint_to_space_id)
                        nr_packages_repointed += 1
        else:
            logger.info('    uploading files:')
            for num_key, upload_def_key in enumerate(space_defs_upload_files[upload_space].keys(), start=1):
                form_file_name = space_defs_upload_files[upload_space][upload_def_key].rpartition('\\')[2]
                logger.info('        [' + str(num_key) + ']: ' + form_file_name)
                nr_files_uploaded += 1

    logger.info(' ')
    logger.info('=============================================')
    logger.info(' ')
    shutil.rmtree(backup_dir)
    logger.info(' ')
    logger.info('Summary: ')
    logger.info('=========================================================')
    logger.info('# created spaces = ' + str(nr_spaces_created))
    logger.info('# files uploaded = ' + str(nr_files_uploaded))
    logger.info('# created spaces = ' + str(nr_spaces_created))
    logger.info('# connections repointed = ' + str(nr_connections_repointed))
    logger.info('# packages repointed = ' + str(nr_packages_repointed))

    logger.info(' ')
    logger.info('Mail sent to ' + mail_recipients.strip())

    mail_attachments = []
    mail_attachments.append(result_log_file)

    with open(result_log_file) as fp:
        send_mail('The upload repository admin files results for ' + current_datetime.strftime('%d %B %Y %H:%M:%S'),
                  mail_recipients, fp.read(), mail_server, mail_attachments,mail_sender,mail_password)

    # Logout from Birst admin
    if soap_session:
        logger.info('Logout admin ...')
        admin_logout(soap_session)

    # Logout from Birst
    logger.info('   ')
    if login_token:
        logger.info('Logout zeep ...')
        try:
            soap_client.service.Logout(login_token)
            # time.sleep(5)
        except Exception as e:
            logger.error('Logout zeep failed')

    if copy_log_file:
        subprocess.call("copy %s logging\\%s" % (result_log_file, copy_log_file), shell=True, stdout=subprocess.DEVNULL)
        logger.info('copying ' + result_log_file + ' to logging\\' + copy_log_file + ' ...')

    logger.info('Job completed')

except Exception as e:
    logging.error('An unexpected error occurred while processing ' + __file__)
    logger.error(e)

    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logger.error('file = ' + fname)
    logger.error('line number = ' + str(exc_tb.tb_lineno))

    current_dir = os.getcwd()
    full_path_export_dir = os.path.join(current_dir, backup_dir)
    if os.path.isdir(full_path_export_dir):
        logger.error('on error: removing export directory: ' + full_path_export_dir)
        shutil.rmtree(backup_dir)

    error_file = os.path.join(current_dir, 'svn_error.txt')
    if os.path.isfile(error_file):
        logger.error('on error: removing svn_error.txt')
        os.remove('svn_error.txt')

    out_file = os.path.join(current_dir, 'svn_out.txt')
    if os.path.isfile(out_file):
        logger.error('on error: removing svn_out.txt')
        os.remove('svn_out.txt')

    # Logout from Birst admin
    if soap_session:
        logger.error('on error: Logout admin ...')
        admin_logout(soap_session)

    if login_token:
        logger.info('on error: Logout zeep ...')
        try:
            soap_client.service.Logout(login_token)
        except Exception as e:
            logger.error('on error: Logout zeep failed')

    if copy_log_file:
        subprocess.call("copy %s logging\\%s" % (result_log_file, copy_log_file), shell=True, stdout=subprocess.DEVNULL)
        logger.info('copying ' + result_log_file + ' to logging\\' + copy_log_file + ' ...')

    logger.info(' ')
    logger.info('Mail with error sent to ' + mail_recipients.strip())
    mail_attachments.append(result_log_file)

    with open(result_log_file) as fp:
        send_mail('Upload Repository Admin files from SVN Error !!! ' + current_datetime.strftime('%d %B %Y %H:%M:%S'), \
                  mail_recipients, fp.read(), mail_server, mail_attachments,mail_sender,mail_password)

    sys.exit(2)

os.system('pause')
