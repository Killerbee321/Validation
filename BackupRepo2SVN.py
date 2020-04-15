#
# Uploads the space repository admin files in SVN to an url..
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
# 1             23-10-2018      John van Dijk      Initial Version

# Package usage
# --------------------------
# packages included with python installation
import logging
import os
import sys
import subprocess
import socket
import csv
import getpass
import shutil
from datetime import datetime
import time
from optparse import OptionParser
from collections import OrderedDict

# packages not included with python installation
# this script requires the Tortoise (SVN) CLI. (command line interface). 
# when installed, svn cli can be changed in Control Panel\All Control Panel Items\Programs and Features\TortoiseSVN*:
# - Select Change.  
# - Select Modify
# - Enable the Command Line Interface

from zeep import \
    Client  # to install, run command: pip install zeep, to update, run command: python -m pip install --upgrade zeep
from CommonFunctions import admin_download_repository_files, admin_get_valid_base_urls
from CommonFunctions import admin_login, admin_logout, check_url_is_valid, get_base_url, get_execution_mode_simulation, \
    get_execution_mode_update
from CommonFunctions import get_output_mode_hide, get_output_mode_show, get_repo_download_def, \
    get_space_copy_from_def
from CommonFunctions import get_space_copy_from_spaces_def, get_space_copy_url, get_space_id, \
    is_space_available, send_mail
from CommonFunctions import svn_path_exists, update_copy_config
from CommonFunctions import send_mail


# Variables
# ----------

# Constants
# --------------------------

# Functions
# --------------------------

def svn_create_backups_directory_structure(backup_dir, logger, download_url, download_spaces):
    error = ''
    space_dir_list = {}
    valid_base_urls = {}

    svn_url = get_repo_base_url()
    region_dir = backup_dir + '/' + 'Region'

    # get list of svn content, starting from HCM.BIRST
    with open('out.txt', 'w+') as fout:
        with open('err.txt', 'w+') as ferr:
            # Changed Code
            subprocess.call('svn list "' + svn_url + '" -R',
                            stdout=fout, stderr=ferr)
            ferr.seek(0)
            error = ferr.read()
            if not error:
                fout.seek(0)
                svn_dir_list = fout.read().split('\n')
                # print('svn directory list: ', svn_dir_list)

    # create Backup Repo files directory, when not existing
    if not error and not svn_path_exists(backup_dir + '/', svn_dir_list):
        with open('out.txt', 'w+') as fout:
            with open('err.txt', 'w+') as ferr:
                subprocess.call(
                    'svn mkdir "' + svn_url + '/' + backup_dir + '" --message "Create dir"',
                    stdout=fout, stderr=ferr)
                ferr.seek(0)
                error = ferr.read()
                if not error:
                    logger.info('Created in SVN : ' + backup_dir)

    # create Backup Repo files/Region directory, when not existing
    if not error and not svn_path_exists(region_dir + '/', svn_dir_list):
        with open('out.txt', 'w+') as fout:
            with open('err.txt', 'w+') as ferr:
                subprocess.call(
                    'svn mkdir "' + svn_url + '/' + region_dir + '" --message "Create dir"',
                    stdout=fout, stderr=ferr)
                ferr.seek(0)
                error = ferr.read()
                if not error:
                    logger.info('Created in SVN : ' + region_dir)

    # create Backup Repo files/Region/[APAC1, APAC2, EU1, US] directories, when not existing
    if not error:
        valid_base_urls = admin_get_valid_base_urls()
        for url_value in valid_base_urls.values():
            region = url_value[0]
            region_sub_dir = region_dir + '/' + region + '/'
            if not svn_path_exists(region_sub_dir, svn_dir_list):
                with open('out.txt', 'w+') as fout:
                    with open('err.txt', 'w+') as ferr:
                        subprocess.call(
                            'svn mkdir "' + svn_url + '/' + region_sub_dir + '" --message "Create dir"',
                            stdout=fout, stderr=ferr)
                        ferr.seek(0)
                        error = error + ferr.read()
                        if not error:
                            logger.info('Created in SVN: ' + region_sub_dir)

    if not error:
        # create the space directories per region
        base_url = get_base_url(download_url)
        region = valid_base_urls[base_url][0]

        for download_space in download_spaces:
            space_dir = region_dir + '/' + region + '/' + download_space + '/'
            space_dir_list[download_space] = space_dir
            if not svn_path_exists(space_dir, svn_dir_list):
                with open('out.txt', 'w+') as fout:
                    with open('err.txt', 'w+') as ferr:
                        subprocess.call(
                            'svn mkdir "' + svn_url + '/' + space_dir + '" --message "Create dir"',
                            stdout=fout, stderr=ferr)
                        ferr.seek(0)
                        error = error + ferr.read()
                        if not error:
                            logger.info('Created in SVN: ' + space_dir)

    return (error, space_dir_list, svn_dir_list)


# Code
# ---------------------------	 
# Init variables
user_name = ''
password = ''
login_token = ''
soap_session = ''
backup_mode = ''
download_url = ''
wsdl_url = ''
copy_config = {}
download_spaces = []
nr_downloaded_files = 0
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
    # password = 'IRONman@123786'
    # user_name = 'proxyuser/syed.khadri@infor.com/hcmcontent'
    # result_log_file = 'log_bu_rep_dev.txt'
    config_file = 'birst_config.txt'
    execution_mode = 'simulation'
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

            if line.startswith('guids = '):
                j = 0
                list1 = line.partition('=')[2].strip().split(',')
                list2 = list(map(lambda x: x.strip(), list1))
                for i in admin_get_valid_base_urls():
                    admin_get_valid_base_urls()[i][1] = list2[j]
                    j = j + 1

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

    # logger.info('Backup mode = ' + backup_mode.upper())
    # logger.info('Execution mode = ' + execution_mode.upper())
    logger.info('Birst User name = ' + user_name + '\n')

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

            if line.startswith('b2s_mailrecipients = '):
                mail_recipients = line.partition('=')[2].strip()

            if line.startswith('copy_'):
                copy_config = update_copy_config(backup_mode, line, copy_config)

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

    wsdl_url = copy_urls[0] + '/CommandWebService.asmx?WSDL'
    download_url = copy_urls[0]

    logger.info('URL = ' + download_url)

    # Create zeep client from BWS wsdl

    soap_client = Client(wsdl=wsdl_url)
    login_token = soap_client.service.Login(user_name, password)
    logger.info('Login with zeep Successful')

    # Log into BWS for admin web services
    # login to app2103 !! for Repository Admin
    time.sleep(5)
    soap_session = admin_login(user_name, password, download_url)
    logger.info('Admin Login Successful')

    # get the list op spaces using the listspaces ws
    array_of_spaces = soap_client.service.listSpaces(login_token)

    copy_from_def = get_space_copy_from_def()
    copy_from_spaces_def = get_space_copy_from_spaces_def()

    space_prefix = copy_config[copy_from_def]
    for space_name in copy_config[copy_from_spaces_def]:
        download_spaces.append(space_prefix + '_' + space_name)

    download_spaces.sort(key=str.lower)

    all_spaces_available = True
    logger.info('   ')
    logger.info('checking all spaces are available ...')
    # check whether the spaces in the config file are available
    for num, space_to_download in enumerate(download_spaces):
        if space_to_download:
            results = is_space_available(soap_session, space_to_download, array_of_spaces)
            soap_session = results[0]
            if not results[1]:  # boolean is_available
                all_spaces_available = False

            logger.info('    ' + results[2])  # result message

    if not all_spaces_available:
        exit_code = 2
        raise RuntimeError('Not all spaces are available.')

    download_defs = get_repo_download_def()

    # creates the Backups directory structure in de "svn_dir"
    # checks Backups directory and all sub directories are checked in.
    logger.info(' ')
    svn_result = svn_create_backups_directory_structure(backup_dir, logger, download_url, download_spaces)
    if svn_result[0]:  # error 
        exit_code = 2
        raise RuntimeError(svn_result[0])

    download_space_dirs = svn_result[1]
    svn_dir_list = svn_result[2]

    for num, space_to_download in enumerate(download_spaces, start=1):
        logger.info(' ')
        logger.info('#############################################')
        logger.info('Birst Space ' + str(num) + ': ' + space_to_download)
        logger.info('#############################################')
        download_space_id = get_space_id(array_of_spaces, space_to_download)
        # print('space to download ',space_to_download)
        # print('space_prefix ',space_prefix)
        # print('space_name ',space_name)

        space_name = space_to_download.partition(space_prefix)[2].strip()
        # print(space_name)
        download_dir = download_space_dirs[space_to_download]

        download_result = admin_download_repository_files(svn_dir_list, soap_session, space_prefix, space_name,
                                                          download_space_id, download_dir, download_defs, logger,
                                                          get_repo_base_url())
        soap_session = download_result[0]
        if download_result[1]:  # error 
            exit_code = 2
            raise RuntimeError(download_result[1])

        nr_downloaded_files += len(download_result[2])  # list of downloaded files

    logger.info(' ')
    logger.info('=============================================')
    logger.info(' ')
    logger.info('Summary: ')
    logger.info('=========================================================')
    logger.info('# total spaces = ' + str(len(download_spaces)))
    logger.info('# files downloaded: ' + str(nr_downloaded_files))

    logger.info(' ')
    logger.info('Mail sent to ' + mail_recipients.strip())

    mail_attachments = []
    mail_attachments.append(result_log_file)

    with open(result_log_file) as fp:
        send_mail('The backup repository admin files results for ' + current_datetime.strftime('%d %B %Y %H:%M:%S'), \
                  mail_recipients, fp.read(), mail_server, mail_attachments, mail_sender, mail_password)

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
        send_mail('Backup Repository Admin files to SVN Error !!! ' + current_datetime.strftime('%d %B %Y %H:%M:%S'), \
                  mail_recipients, fp.read(), mail_server, mail_attachments, mail_sender, mail_password)

    sys.exit(2)

os.system('pause')
