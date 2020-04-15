#
# This script contains functions used in more than 1 python script.
# To use these functions do: from Commonfunctions import <function> [, <function>, ....]

# Variables
# --------------------------


# functions for Handle Modeler
DELETE_COLUMN = 'delete_column'
REPLACE_LOCALE = 'replace_locale'
REPLACE_SQUARE_BRACKETS = 'replace_square_brackets'
modeler_functions = [DELETE_COLUMN,
                     REPLACE_LOCALE,
                     REPLACE_SQUARE_BRACKETS]

# for Backup, Repoint and AddUser.
SPACE_COPY_FROM = 'copy_from'
SPACE_COPY_URL = 'copy_url'
SPACE_COPY_TO = 'copy_to'
SPACE_COPY_TO_MULTI = 'copy_to_multi'
SPACE_COPY_DATE_TIME = 'copy_date_time'
SPACE_COPY_KEEP = 'copy_keep'
SPACE_COPY_FROM_SPACES = 'copy_from_spaces'
SPACE_COPY_TO_SUFFIX = 'copy_to_suffix'
SPACE_COPY_FORMAT = 'copy_format'
SPACE_REPOINT_PACKAGES = 'repoint_packages'
SPACE_ADD_USERS = 'add_users'
SPACE_COPY_CREATE = 'copy_create'
SPACE_COPY_OPTIONS = 'copy_options'
SPACE_COPY_MODE = 'copy_mode'
SPACE_COPY_MODE_COPY = 'copy'
SPACE_COPY_MODE_REPLICATE = 'replicate'
SPACE_COPY_MODE_SWAP = 'swap'

EXECUTIONMODE_UPDATE = 'update'
EXECUTIONMODE_SIMULATION = 'simulation'
OUTPUTMODE_SHOW = 'show'
OUTPUTMODE_HIDE = 'hide'
UPLOADCHUNKSIZE = 5242880  # 5 MB

all_birst_copy_options = ['birst-connect',
                          'color-palettes',
                          'custom-subject-areas',
                          'dashboardstyles',
                          'data',
                          'datastore',
                          'repository',
                          'packages',
                          'schedules-report',
                          'settings-permissions',
                          'settings-membership',
                          'salesforce',
                          'catalog',
                          'CustomGeoMaps.xml',
                          'spacesettings.xml',
                          'SavedExpressions.xml',
                          'DrillMaps.xml',
                          'connectors',
                          'settings-basic',
                          'themes',
                          'conditions']

currency_types = ['DWC', 'LC', 'RC1', 'RC2', 'RFC', 'TC', 'CC', 'PC']
unit_types = ['BUA', 'BUL', 'BUP', 'BUT', 'BUV', 'BUW', 'IU']

# compare and merge types
compare_and_merge_types = 'sources, hierarchies, custom, variables, aggregates, catalog'

# for admin services
#                   base url                           region    modeler connection guid                               
valid_base_urls = {'https://app2103.bws.birst.com': ['US', '6dcc2f61-c848-4bf9-a8bd-d5504f996fbf'],
                   # app2103 required for repository administration access
                   'https://app2103.eu1.birst.com': ['EU1', 'e55d63bb-4d11-43e3-89d7-30b3dde68cef'],
                   'https://app2103.apac1.birst.com': ['APAC1', '16bff0e4-253b-4017-b03f-c599db2bd743'],
                   'https://app2103.apac2.birst.com': ['APAC2', 'eadda9de-b6f8-4c59-9e21-65671ec22c61'],
                   'https://login.cn1.birst.com': ['CN1','ea77a781-69eb-4f66-ba16-b630571fa591']}

ADMIN_CATALOG_KEY = 'Catalog'
ADMIN_CONNECTOR_CONFIG_KEY = 'ConnectorConfig'
ADMIN_EVENT_ARGUMENT_KEY = '__EVENTARGUMENT'
ADMIN_EVENT_TARGET_KEY = '__EVENTTARGET'
ADMIN_EVENT_VALIDATION_KEY = '__EVENTVALIDATION'
ADMIN_LAST_FOCUS_KEY = '__LASTFOCUS'
ADMIN_NAMED_ID_POLICY_LIST_KEY = 'NameIdPolicyList'
ADMIN_PACKAGES_KEY = 'Packages'
ADMIN_PREVIOUS_PAGE_KEY = '__PREVIOUSPAGE'
ADMIN_REPOSITORY_KEY = 'Repository'
ADMIN_RESULT_SET_SIZE_BOX_ID_KEY = 'ResultSetSizeBoxID'
ADMIN_QUERY_CONNECTION_KEY = 'QueryConnection'
ADMIN_TIME_OUT_BOX_ID_KEY = 'TimeOutBoxID'
ADMIN_USER_AND_GROUPS_KEY = 'UsersAndGroups'
ADMIN_VIEW_STATE_KEY = '__VIEWSTATE'
ADMIN_VIEW_STATE_GEN_KEY = '__VIEWSTATEGENERATOR'
ADMIN_VIEW_STATE_ENCRYPTED_KEY = '__VIEWSTATEENCRYPTED'

ADMIN_SAVED_EXPRESSIONS_KEY = 'SavedExpressions'
ADMIN_DRILL_MAPS_KEY = 'DrillMaps'
ADMIN_CUSTOM_SUBJCT_AREAs_KEY = 'CustomSubjectAreas'
ADMIN_CONDITIONS_KEY = 'Conditions'
ADMIN_SPACE_SETTINGS_KEY = 'SpaceSettings'
ADMIN_DASHBOARD_STYLE_SETTINGS_KEY = 'DashboardStyleSettings'
ADMIN_CUSTOM_GEO_MAPS_KEY = 'CustomGeoMaps'
ADMIN_PAGE_SEQUENCE_KEY = 'PageSequence'
ADMIN_KML_FILES_KEY = 'KMLFiles'
ADMIN_ODBC_MAPPING_KEY = 'ODBCMapping'
ADMIN_PRONTO_PREP_STEPS_KEY = 'ProntoPrepSteps'
ADMIN_SHARED_CONNECTION_FILE_KEY = 'SharedConnectionFile'
ADMIN_SHARED_CONNECTION_NAME_KEY = 'SharedConnectionName'
ADMIN_SMART_CONFIG_KEY = 'SmartConfig'
ADMIN_SFDC_MAPPING_FILES_KEY = 'SFDCMappingFiles'
ADMIN_SFDC_QUERY_INFO_KEY = 'SFDCQueryInfo'
ADMIN_PROCESSING_GROUP_AND_SOURCES_KEY = 'ProcessingGroupsAndSources'
ADMIN_REPOSITORY_PROD_KEY = 'RepositoryProd'
ADMIN_BYTES_READ_KEY = 'BytesRead'
ADMIN_QUERY_BOX_KEY = 'QueryBox'
ADMIN_WORKFLOW_ID_KEY = 'workflowId'
ADMIN_ACCOINT_ID_KEY = 'accountId'
ADMIN_IDP_NAME_VALUE_KEY = 'IdpNameValue'
ADMIN_IDP_ISSUER_ID_VALUE_KEY = 'IdpIssuerIDValue'
ADMIN_CERTIFICATE_VALUE_KEY = 'CertificateValue'
ADMIN_TIMEOUT_VALUE_KEY = 'TimeoutValue'
ADMIN_LOGOUT_PAGE_VALUE_KEY = 'LogoutPageValue'
ADMIN_ERROR_PAGE_VALUE_KEY = 'ErrorPageValue'
ADMIN_IDP_URL_VALUE_KEY = 'IdpUrlValue'
ADMIN_SAML_ACCOUNT_ID_VALUE_KEY = 'SamlAccountIdValues'

repo_download_upload_def = {
    # The order in which the defs are defined, determines the order in which the files are uploaded.
    # Do NOT change the order !!!!
    #                         filename                       
    #                         Download value: 
    #                         Upload name: filename
    #                         Upload name: submit,   
    #                         Upload value: submit
    #                         other settings like: Bytes Read etc
    ADMIN_REPOSITORY_KEY: [
        'repository_dev_mongo.xml',
        'GetRepository',
        'RepositoryUpload',
        'UploadButton',
        'Upload Repository',
        'text/xml',
        ''
    ],
    ADMIN_PACKAGES_KEY: [
        'packages_mongo.xml',
        'GetPackagesXml',
        'PackagesUpload',
        'PackagesUploadButton',
        'Upload Packages.xml',
        'text/xml',
        ''
    ],
    ADMIN_CONNECTOR_CONFIG_KEY: [
        'cloud_connectors_config.xml',
        'GetConnectorConfigFile',
        'ConnectorConfigUpload',
        'UploadConnectorConfig',
        'Upload cloud_connectors_config.xml',
        'text/xml',
        ''
    ],
    ADMIN_USER_AND_GROUPS_KEY: [
        'users_and_groups.xml',
        'GetUsersAndGroups',
        'UsersAndGroupsUpload',
        'UploadUsersAndGroups',
        'Upload UsersAndGroups.xml',
        'text/xml',
        ''
    ],
    ADMIN_CATALOG_KEY: [
        'catalog.zip',
        'GetCatalogFolder',
        'CatalogFolderUpload',
        'UploadCatalogFolderButton',
        'Upload Catalog folder Zip File',
        'application/x-zip-compressed',
        ''
    ],
    ADMIN_SAVED_EXPRESSIONS_KEY: [
        'saved_expressions.xml',
        'GetSavedExpressions',
        'SavedExpressionUpload',
        'SavedExpressionUploadButton',
        'Upload SavedExpressions.xml',
        'text/xml',
        ''
    ],
    ADMIN_DRILL_MAPS_KEY: [
        'drillmaps.xml',
        'GetDrillMaps',
        'DrillMapsUpload',
        'UploadDrillMapsButton',
        'Upload DrillMaps.xml',
        'text/xml',
        ''
    ],
    ADMIN_CUSTOM_SUBJCT_AREAs_KEY: [
        'custom_subject_areas.zip',
        'GetCustomSubjectAreas',
        'CustomSubjectAreasUpload',
        'UploadCustomSubjectAreasButton',
        'Upload CustomSubjectAreas Zip File',
        'application/x-zip-compressed',
        ''
    ],
    ADMIN_CONDITIONS_KEY: [
        'conditions.json',
        'GetConditions',
        'ConditionUpload',
        'ReplicateConditions_btn',
        'Replicate conditions for space',
        'application/json',
        ''
    ],
    ADMIN_SPACE_SETTINGS_KEY: [
        'space_settings.xml',
        'GetSpaceSettings',
        '',  # SpaceSettingsUpload
        '',  # UploadSpaceSettings
        '',  # Upload spacesettings.xml (foreground/background color)
        'text/xml',
        ''
    ],
    ADMIN_DASHBOARD_STYLE_SETTINGS_KEY: [
        'dashboard_style_settings.xml',
        'GetDashboardStyleSettings',
        '',  # DashboardStyleSettingsUpload
        '',  # UploadDashboardStyleSettings
        '',  # Upload DashboardStyleSettings.xml
        'text/xml',
        ''
    ],
    ADMIN_CUSTOM_GEO_MAPS_KEY: [
        'custom_geo_maps.xml',
        'GetCustomGeoMaps',
        '',  # CustomGeoMapsUpload
        '',  # UploadCustomGeoMapsButton
        '',  # Upload CustomGeoMaps.xml
        'text/xml',
        ''
    ],
    ADMIN_PAGE_SEQUENCE_KEY: [
        'page_sequence.txt',
        'GetPageSequence',
        '',  # PageSequenceUpload
        '',  # PageSequenceButton
        '',  # Upload pageSequence.txt (for Kiosk mode)
        'text/plain',
        ''
    ],
    ADMIN_KML_FILES_KEY: [
        'kml.zip',
        'GetKmlFiles',
        '',  # KMLFilesUpload
        '',  # UploadKMLFilesButton
        '',  # Upload KML Zip File
        'application/x-zip-compressed',
        ''
    ],
    ADMIN_ODBC_MAPPING_KEY: [
        'odbc_alias_mapping.xml',
        'GetODBCMapping',
        '',  # ODBCMappingUpload
        '',  # ODBCUploadButton'
        '',  # Upload ODBC Alias Mapping
        'text/xml',
        ''
    ],
    ADMIN_PRONTO_PREP_STEPS_KEY: [
        'pronto_prep_steps.json',
        'GetProntoPrepSteps',
        '',  # ProntoPrepUpload
        '',  # ProntoPrepUploadButton
        '',  # Upload Pronto Prep JSON File
        'application/json',
        ''
    ],
    ADMIN_SHARED_CONNECTION_FILE_KEY: [
        '',
        '',
        '',  # SharedConnectionFileUpload
        '',
        '',
        'application/octet-stream',
        ''
    ],
    ADMIN_SHARED_CONNECTION_NAME_KEY: [
        '',
        '',
        '',  # SharedConnectionName
        '',
        '',
        '',
        ''
    ],
    ADMIN_SMART_CONFIG_KEY: [
        'SmartConfig.json',
        'GetSmartConfig',
        '',  # SmartConfigUpload
        '',  # ReplicateSmartConfig_btn'
        '',  # Replicate smart config for space
        'application/json',
        ''
    ],
    ADMIN_SFDC_MAPPING_FILES_KEY: [
        'sfdc_mapping_files.xml',
        'GetSFDCMappingFiles',
        '',
        '',
        '',
        'text/xml',
        ''
    ],
    ADMIN_SFDC_QUERY_INFO_KEY: [
        'sfdc_query_info.xml',
        'GetSFDCQueryInfo',
        '',
        '',
        '',
        'text/xml',
        ''
    ],
    ADMIN_PROCESSING_GROUP_AND_SOURCES_KEY: [
        'processing_groups_and_sources.txt',
        'GetProcessingGroupsAndSources',
        '',
        '',
        '',
        'text/plain',
        ''
    ],
    ADMIN_REPOSITORY_PROD_KEY: [
        'repository_mongo.xml',
        'GetRepositoryProd',
        '',
        '',
        '',
        'text/xml',
        ''
    ],
    ADMIN_BYTES_READ_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        '512000'
    ],
    ADMIN_QUERY_CONNECTION_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        'Default Connection'
    ],
    ADMIN_QUERY_BOX_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_TIME_OUT_BOX_ID_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        '10'
    ],
    ADMIN_RESULT_SET_SIZE_BOX_ID_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        '10'
    ],
    ADMIN_WORKFLOW_ID_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_ACCOINT_ID_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_IDP_NAME_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_IDP_ISSUER_ID_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_CERTIFICATE_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_TIMEOUT_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_LOGOUT_PAGE_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_ERROR_PAGE_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_IDP_URL_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_SAML_ACCOUNT_ID_VALUE_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ],
    ADMIN_NAMED_ID_POLICY_LIST_KEY: [
        '',
        '',
        '',
        '',
        '',
        '',
        'urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified'
    ]

}

ADMIN_BASE_URL = 'https://app2103.bws.birst.com'
admin_service_base_url = ADMIN_BASE_URL
ADMIN_SESSION_KEY = 'admin_session'
ADMIN_HEADERS_KEY = 'admin_headers'
ADMIN_CONTENT_TYPE_KEY = 'content-type'
ADMIN_CONTENT_TYPE_VALUE = 'text/xml; charset-UTF-8'
ADMIN_BIRST_SPACID = 'BIRST-SPACEID'
ADMIN_SOAP_ACTION_KEY = 'SOAPAction'
ADMIN_SOAP_ACTION_TEMPLATE = 'http://www.birst.com/'  # later on service name is added
ADMIN_XSRF_TOKEN_KEY = 'X-XSRF-TOKEN'
ADMIN_URL_KEY = 'admin_url'
ADMIN_BODY_TEMPLATE = """<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">
                            <soap:Body>
                                <service_name xmlns=\"http://www.birst.com/\"></service_name>
                            </soap:Body>
                        </soap:Envelope>"""
ADMIN_ETL_HIDDEN_SUFFIX = 'etl_hidden'
ADMIN_ETL_HIDDEN_VARIABLE = 'LN_etl_Hidden'
ADMIN_SEC_FILTER_TEMPLATE = '\r\n        &lt;SecFilter&gt;\r\n          &lt;Type&gt;0&lt;/Type&gt;\r\n          &lt;SessionVariable&gt;LN_etl_Hidden&lt;/SessionVariable&gt;\r\n          &lt;Enabled&gt;true&lt;/Enabled&gt;\r\n        &lt;/SecFilter&gt;'
ADMIN_PROC_NOT_USED = 'DoNotProcessWhenUsedInScript'
ADMIN_INTERVAL_COOKIES = 3  # update cookies interval in separate thread

STAGING_TABLE_START = '&lt;stagingTable&gt;'
STAGING_TABLE_END = '&lt;/stagingTable&gt;'
SOURCE_FILE_START = '&lt;sourceFile&gt;'
SOURCE_FILE_END = '&lt;/sourceFile&gt;'
COLUMNS_START = '&lt;Columns&gt;'
COLUMNS_END = '&lt;/Columns&gt;'
IMPORT_COLUMNS_START = '&lt;ImportColumns&gt;'
IMPORT_COLUMNS_END = '&lt;/ImportColumns&gt;'
SOURCE_COLUMN_START = '&lt;SourceColumn&gt;'
SOURCE_COLUMN_END = '&lt;/SourceColumn&gt;'
STAGING_COLUMN_START = '&lt;StagingColumn&gt;'
STAGING_COLUMN_END = '&lt;/StagingColumn&gt;'
LEVELS_START = '&lt;Levels&gt;'
LEVELS_END = '&lt;/Levels&gt;'
ARRAY_OF_STRING_START = '&lt;ArrayOfString&gt;'
ARRAY_OF_STRING_END = '&lt;/ArrayOfString&gt;'
STRING_START = '&lt;string&gt;'
STRING_END = '&lt;/string&gt;'
TRANSACTIONAL_START = '&lt;Transactional&gt;'  # source property: Rows in data source are tranactions (vs.snapshot)
TRANSACTIONAL_END = '&lt;/Transactional&gt;'
DONOTPROCESS = 'DoNotProcessWhenUsedInScript'
DONOTPROCESS_START = '&lt;DoNotProcessWhenUsedInScript&gt;'  # source property: Do not process when used by another script
DONOTPROCESS_END = '&lt;/DoNotProcessWhenUsedInScript&gt;'
DONOTDETECT = 'SkipScanning'
DONOTDETECT_START = '&lt;SkipScanning&gt;'  # source property: Do not detect source data changes on upload
DONOTDETECT_END = '&lt;/SkipScanning&gt;'
LOAD_GROUP = 'LoadGroups'
LOAD_GROUP_START = '&lt;LoadGroups&gt;'
LOAD_GROUP_END = '&lt;/LoadGroups&gt;'
PROCESSING_GROUP = 'SubGroups'
PROCESSING_GROUP_START = '&lt;SubGroups&gt;'  # source property: Bulk insert and delete measure records
PROCESSING_GROUP_END = '&lt;/SubGroups&gt;'
BULKINSERTDELETE_START = '&lt;IncrementalSnapshotFact&gt;'  # source property: Bulk insert and delete measure records
BULKINSERTDALETE_END = '&lt;/IncrementalSnapshotFact&gt;'
SNAPSHOTDELETEKEY_START = '&lt;SnapshotDeleteKeys&gt;'
SNAPSHOTDELETEKEY_END = '&lt;/SnapshotDeleteKeys&gt;'
PREVENT_UPDATE_START = '&lt;PreventUpdate&gt;'
PREVENT_UPDATE_END = '&lt;/PreventUpdate&gt;'
TARGET_TYPES_ELEMENT_START = '&lt;TargetTypes&gt;'
TARGET_TYPES_ELEMENT_END = '&lt;/TargetTypes&gt;'
MEASURE_ELEMENT = '          &lt;string&gt;Measure&lt;/string&gt;\r\n'
TARGET_TYPES_NO_ELEMENTS = '&lt;TargetTypes /&gt;'
LOCK_TYPE_START = '&lt;LockType&gt;'
LOCK_TYPE_END = '&lt;/LockType&gt;'
FIELD_TYPE_LN_FIELD = 'LN Table Field'
FIELD_TYPE_CALC_FIELD = 'Calculated Field'
FIELD_TYPE_LN_META_FIELD = 'LN Meta Data Field'
FIELD_TYPE_UNKNOWN = 'Unknown'

SPACE_STATE_AVALABLE = 'AVAILABLE'

# # REPO_BASE_URL = 'http://nlbalevl1/svn/BODRepository/LN.BIRST'
# # REPO_BASE_URL = 'http://INHYNSKHADRI1.infor.com:8080/svn/HCM.BIRST/'
# REPO_BASE_URL = 'https://inhyvwhcmbidev.infor.com:8443/svn/HCM.BIRST/'
# # REPO_BASE_URL = 'http://INHYNSKHADRI1.infor.com:8080/svn/HCM.BIRST/'
# REPO_SVN_DIR = 'BackUp Repo files'

# Package usage
# --------------------------6
# packages included with python installation
# to reinstall missing module: python -m pip install --upgrade SomeModule
import os, stat
import time
import copy
import smtplib
import shutil
import requests
import threading
import xml.etree.ElementTree as etree
import logging
import json
import base64
import subprocess

from xml.dom import minidom
from email.message import EmailMessage
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import OrderedDict
from xml.etree import ElementTree as ET

# packages not included with python installation

from bs4 import BeautifulSoup  # to install, run command: pip install bs4
import xmltodict  # to install, run command: pip install xmltodict


from requests.adapters import HTTPAdapter
HTTPAdapter.max_retries = 10000

# Functions
# --------------------------
# xml
# ====
def prettify(elem):
    # Return a pretty-printed XML string for the Element.
    rough_string = etree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


# define functions
# ==================
def admin_get_valid_base_urls():
    return (valid_base_urls)


# def get_repo_base_url():
#     return REPO_BASE_URL


# def get_repo_svn_dir():
#     return REPO_SVN_DIR


def svn_path_exists(url, svn_list):
    path_found = False
    for file_dir in svn_list:
        if file_dir.strip() == url.strip():
            path_found = True

    return (path_found)


def get_modeler_functions():
    return modeler_functions


def get_function_delete_column():
    return DELETE_COLUMN


def get_function_replace_locale():
    return REPLACE_LOCALE


def get_function_replace_square_brackets():
    return REPLACE_SQUARE_BRACKETS


def get_execution_mode_update():
    return EXECUTIONMODE_UPDATE


def get_execution_mode_simulation():
    return EXECUTIONMODE_SIMULATION


def get_output_mode_hide():
    return OUTPUTMODE_HIDE


def get_output_mode_show():
    return OUTPUTMODE_SHOW


def get_space_state_available():
    return SPACE_STATE_AVALABLE


def get_space_copy_mode_copy():
    return SPACE_COPY_MODE_COPY


def get_space_copy_mode_replicate():
    return SPACE_COPY_MODE_REPLICATE


def get_space_copy_url_def():
    return SPACE_COPY_URL


def get_space_copy_from_def():
    return SPACE_COPY_FROM


def get_space_copy_to_def():
    return SPACE_COPY_TO


def get_space_copy_from_spaces_def():
    return SPACE_COPY_FROM_SPACES


def get_property_do_no_proces_def():
    return DONOTPROCESS


def get_property_do_no_detect_change():
    return DONOTDETECT


def get_property_processing_group():
    return PROCESSING_GROUP


def get_property_string_start():
    return STRING_START


def get_property_string_end():
    return STRING_END


def get_repo_download_upload_def():
    return repo_download_upload_def


def get_repo_download_def():
    download_defs = {}

    for download_def in repo_download_upload_def.values():
        download_defs[download_def[1]] = download_def[0]

    return download_defs


def get_repo_upload_def_keys():
    upload_def_keys = []

    for upload_def_key, upload_def_value in repo_download_upload_def.items():
        if upload_def_value[2]:
            upload_def_keys.append(upload_def_key)

    return upload_def_keys


def get_base_url(url):
    base_url = ''

    if url.count('/') >= 3:
        # example https://app2103.bws.birst.com/login.aspx
        split_url = url.split('/')
        base_url = split_url[0].strip() + '//' + split_url[2].strip()
    else:
        base_url = url

    return (base_url)


def get_supported_actions(config_file):
    # retrieves the ua_action commands and formats form the config file.

    supported_actions = {}
    tag_ua_action_found = False
    tag_command_found = False
    tag_format_found = False
    action_command = ''
    action_format = ''

    with open(config_file) as f:
        for line in f:
            if line.startswith('# TAG: ua_action'):
                tag_ua_action_found = True

            if tag_ua_action_found:
                if line.startswith('#   - command = '):
                    tag_command_found = True
                    action_command = line.partition('=')[2].strip()

            if tag_command_found:
                if line.startswith('#     - format: ua_action = '):
                    tag_format_found = True
                    action_format = line.partition('#     - format: ')[2].strip()

            if tag_format_found:
                params_list = action_format.partition('=')[2].split(',')
                if params_list:
                    action_command_list = []
                    action_command_list.append(len(params_list))
                    action_command_list.append(action_format)
                    supported_actions[action_command] = action_command_list

                    tag_command_found = False
                    tag_format_found = False
                    action_command = ''
                    action_format = ''

                if not line.startswith('#') or 'TAG' in line:
                    break

    return supported_actions


def get_copy_space_options():
    return all_birst_copy_options


def get_compare_and_merge_types():
    return compare_and_merge_types


def get_currency_types():
    return currency_types


def get_unit_types():
    return unit_types


# modeler functions
# ==================
def check_saved_object_alias(saved_object):
    check_errors = []

    if ' AS ' in saved_object['query'].upper():
        check_errors.append(saved_object['name'] + ':  Error, string like " AS " found in query')

    return check_errors


def check_saved_object(saved_object):
    check_errors = []

    check_errors = check_saved_object_alias(saved_object)

    if '[' in saved_object['query']:
        check_errors.append(saved_object['name'] + ':  Error, string "]" found in query')

    if ']' in saved_object['query']:
        check_errors.append(saved_object['name'] + ':  Error, string "]" found in query')

    if check_errors:
        check_errors.append('        ' + saved_object['query'])

    return (check_errors)


# migrate spaces handling
# ========================
def check_url_is_valid(url_to_check):
    is_valid = False

    if url_to_check in valid_base_urls.keys():
        is_valid = True

    return (is_valid)


# file handling
# ===============
def upload_files_for_space(zeep_client, login_token, space_name, space_id, upload_files, execution_mode, logger):
    upload_token = ''
    job_results = []
    result_message = ''

    for upload_num, upload_file in enumerate(upload_files, start=1):

        result_message = 'Upload file[' + str(upload_num) + '] = ' + upload_file

        file_info = os.path.split(upload_file)
        # logger.info('begin data upload ...')
        if execution_mode == EXECUTIONMODE_UPDATE:
            upload_token = zeep_client.service.beginDataUpload(login_token, space_id, file_info[1])
            # logger.info('upload token = ' + upload_token)

        with open(upload_file, 'rb') as file:
            logger.info('   upload file: ' + upload_file)
            all_lines = [x for x in file.readlines() if x.strip()]
            logger.info('   # lines : ' + str(len(all_lines)))
            file.seek(0)  # rewind the file back to the beginning
            # logger.info('starting upload ...')
            total_size = len(file.read())
            # rewind file pointer to begin
            file.seek(0, 0)
            uploaded_size = 0
            for chunk in iter(lambda: file.read(UPLOADCHUNKSIZE), b''):
                length = len(chunk)
                uploaded_size = uploaded_size + length
                logger.info('   uploaded size ' + str(uploaded_size) + ' of total size ' + str(total_size))
                # logger.info('chunk = ' + str(chunk))
                if execution_mode == EXECUTIONMODE_UPDATE:
                    zeep_client.service.uploadData(login_token, upload_token, length, chunk)
            # logger.info('finishing upload ...')
            if execution_mode == EXECUTIONMODE_UPDATE:
                zeep_client.service.finishDataUpload(login_token, upload_token)
                # logger.info('   finish response = ' + str(zeep_client.service.finishDataUpload(login_token, upload_token)))
            # logger.info('finishing upload done')

        if execution_mode == EXECUTIONMODE_UPDATE:

            upload_datetime_start = datetime.now()
            diff_date = upload_datetime_start - upload_datetime_start

            while True:
                if zeep_client.service.isJobComplete(login_token, upload_token):
                    logger.info('   waited upload to complete ' + str(int(diff_date.seconds)) + ' seconds')
                    break

                upload_datetime_end = datetime.now()
                diff_date = upload_datetime_end - upload_datetime_start
                print("waiting upload to complete: " + str(int(diff_date.seconds)) + ' seconds', end='\r', flush=True)
                time.sleep(1)

            job_status = zeep_client.service.getJobStatus(login_token, upload_token)
            result_message = result_message + ': job status = ' + job_status.statusCode
            logger.info('   Job status = ' + job_status.statusCode)
            if job_status.statusCode != 'Complete':
                result_message = result_message + ',  message = ' + job_status.message + '\n'

            logger.info(' ')
        else:
            result_message = result_message + ': job status = Complete'

        job_results.append(result_message)

    return job_results


# fact check funcions:
# =====================
def get_column_info(columns):
    column_info = {}

    for column in columns:
        detail_info = {}
        detail_info['AnalyzeByDate'] = column.AnalyzeByDate
        detail_info['AnalyzeMeasure'] = column.AnalyzeMeasure
        detail_info['DataType'] = column.DataType

        if is_NoneType(column.EnableSecutityFilter):
            detail_info['EnableSecurityFilter'] = ''
        else:
            detail_info['EnableSecurityFilter'] = column.EnableSecutityFilter

        if is_NoneType(column.Format):
            detail_info['Format'] = ''
        else:
            detail_info['Format'] = column.Format

        if is_NoneType(column.TargetTypes):
            detail_info['Hierarchy'] = ''
        else:
            detail_info['Hierarchy'] = column.TargetTypes.string[0]

        if is_NoneType(column.LockType):
            detail_info['LockType'] = ''
        else:
            detail_info['LockType'] = column.LockType

        detail_info['Measure'] = column.Measure

        if is_NoneType(column.UnknownValue):
            detail_info['UnknownValue'] = ''
        else:
            detail_info['UnknownValue'] = column.UnknownValue

        detail_info['Width'] = column.Width
        column_info[column.Name] = detail_info

    return column_info


def get_subject_area_saved_expressions(soap_client, login_token, subject_area, space_id, logger):
    sa_saved_expressions = []

    subject_area_content = get_subject_area_content(soap_client, logger, login_token, space_id, subject_area)

    if subject_area_content:
        sa_root = ET.fromstring(subject_area_content)
        for saved_expr_node in sa_root.findall(".//*[@t='s']"):
            attrib_l = saved_expr_node.get('l').strip()
            if attrib_l not in sa_saved_expressions:
                sa_saved_expressions.append(attrib_l)

    sa_saved_expressions.sort()

    return sa_saved_expressions


def check_columns_mapped_to_enum(fact_check_errors, column_name, enum_table_field_defs, columns_info, source_script,
                                 fact_check_num, sa_saved_expressions):
    # find mapping in source script
    source_column = '[' + column_name + '] ='
    if source_column in source_script:
        stg_mapping = source_script.partition(source_column)[2].partition('[')[2].partition(']')[0].strip()
        mapped_table_field = stg_mapping.partition('STG_')[2].partition('_kw')[0].strip()

        if mapped_table_field in enum_table_field_defs:
            # column mapped to enum

            if column_name.endswith(' Filter'):
                # look for sort and ims_columns
                sort_column = column_name.replace(' Filter', ' Sort')
                ims_column = 'ims_' + mapped_table_field.replace('.', '_') + '_kw'

                if sort_column not in columns_info.keys():
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': sort column ' + sort_column + ' does not exist. Missing column mapped to enum: ' + mapped_table_field)
                else:
                    saved_expression_name = column_name.partition('Filter')[0].strip()
                    if saved_expression_name not in sa_saved_expressions:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': translation saved expression not found for filter column ' + column_name + '. Missing saved_expression: ' + saved_expression_name)

                if ims_column not in columns_info.keys():
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': ims column ' + ims_column + ' does not exist. Missing column mapped to enum: ' + mapped_table_field)

                if columns_info[column_name]['DataType'] != 'Varchar':
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                                 'DataType'] + ', must be Varchar')

                if not columns_info[column_name]['Hierarchy']:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ' has empty hierarchy. Must be filled')

            elif column_name.endswith(' Sort'):
                # look for filter and ims_columns
                filter_column = column_name.replace(' Sort', ' Filter')
                ims_column = 'ims_' + mapped_table_field.replace('.', '_') + '_kw'

                if filter_column not in columns_info.keys():
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': filter column ' + filter_column + ' does not exist. Missing column mapped to enum: ' + mapped_table_field)
                else:
                    saved_expression_name = column_name.partition('Sort')[0].strip()
                    if saved_expression_name not in sa_saved_expressions:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': translation saved expression not found for sort column ' + column_name + '. Missing saved expression: ' + saved_expression_name)

                if ims_column not in columns_info.keys():
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': ims column ' + ims_column + ' does not exist. Missing column mapped to enum: ' + mapped_table_field)

                if columns_info[column_name]['DataType'] != 'Integer':
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                                 'DataType'] + ', must be Integer')

                if not columns_info[column_name]['Hierarchy']:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ': has empty hierarchy. Must be filled')

            else:
                ims_column = 'ims_' + mapped_table_field.replace('.', '_') + '_kw'
                if column_name == ims_column:
                    # look for filter ans sort columns
                    remaining_script = source_script
                    sort_column = ''
                    filter_column = ''
                    line_end_kw = ' = [STG_' + mapped_table_field + '_kw]'
                    line_end = ' = [STG_' + mapped_table_field + ']'

                    while remaining_script:
                        line, remaining_script = get_line(remaining_script)
                        if line.endswith(line_end_kw):
                            filter_column = line.partition(line_end_kw)[0].partition('[')[2].partition(']')[0].strip()
                        if line.endswith(line_end):
                            sort_column = line.partition(line_end)[0].partition('[')[2].partition(']')[0].strip()

                        if filter_column and sort_column and filter_column.partition(' ')[0] == \
                                sort_column.partition(' ')[0]:
                            break

                    if not filter_column or filter_column not in columns_info.keys():
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': column ' + ims_column + ' has no Filter column mapped to enum: ' + mapped_table_field)
                    if not sort_column or sort_column not in columns_info.keys():
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': column ' + ims_column + ' has no Sort column mapped to enum: ' + mapped_table_field)

                    if columns_info[column_name]['DataType'] != 'Varchar':
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                                     'DataType'] + ', must be Varchar')

                    if not columns_info[column_name]['Hierarchy']:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': column ' + column_name + ' has empty hierarchy. Must be filled')

    else:
        fact_check_errors.append(
            'check #' + str(fact_check_num).rjust(2) + ': column ' + column_name + ' does not exist in source script')

    return fact_check_errors


def get_ln_table_from_input_query(source_script):
    ln_table = source_script.partition('FROM ')[2].partition('STG_')[2].partition(']')[0].strip()

    return ln_table


def get_line(text):
    line = text.partition('\n')[0]
    remaining_text = text.partition('\n')[2]

    return line, remaining_text


def get_iif_true_false_values(iif_statement, set_value_list):
    true_value = iif_statement.partition(',')[2].partition(',')[0].strip()
    false_value = iif_statement.partition(',')[2].partition(',')[2].partition(')')[0].strip()
    set_value_list.append(true_value)
    set_value_list.append(false_value)

    return set_value_list


def get_column_set_value(column_name, source_script):
    remaining_script = source_script
    set_value_1 = []
    set_value_2 = []
    set_value = []

    while remaining_script:
        line, remaining_script = get_line(remaining_script)
        value_check = '[' + column_name + '] = '
        if line.strip().startswith(value_check):
            set_value_1.append(line.partition(value_check)[2].strip())

    # check variable is used to set the column
    # only one level deep for variables and IIF
    for set_val_num, var_set_value in enumerate(set_value_1):
        if set_value_1[set_val_num].startswith('[') and \
                set_value_1[set_val_num].endswith(']') and \
                not set_value_1[set_val_num].startswith('[Grain') and \
                not set_value_1[set_val_num].startswith('[Level'):
            set_variable = set_value_1[set_val_num].partition('[')[2].partition(']')[0].strip()
            variable_value_check = '[' + set_variable + '] = '
            remaining_script = source_script

            dim_set_value = ''
            var_set_value = ''
            while remaining_script:
                line, remaining_script = get_line(remaining_script)
                variable_value_check = '[' + set_variable + '] = '
                if line.strip().startswith(variable_value_check):
                    var_set_value = line.partition(variable_value_check)[2].strip()
                    if var_set_value.strip().startswith('IIF'):
                        set_value_2 = get_iif_true_false_values(var_set_value, set_value_2)
                    else:
                        set_value_2.append(var_set_value)
                else:
                    dim_value_check = 'DIM [' + set_variable + ']'
                    if line.strip().startswith(dim_value_check):
                        dim_set_value = line.partition('=')[2].strip()

            if not set_value_2:
                set_value_2.append(dim_set_value)

        elif set_value_1[set_val_num].startswith('IIF'):
            set_value_2 = get_iif_true_false_values(set_value_1[set_val_num], set_value_2)

        else:
            set_value_2.append(set_value_1[set_val_num])

    if not set_value_2:
        set_value = ['""']
    else:
        set_value = set_value_2

    set_value.sort()

    return set_value


def check_set_values(fact_check_errors, exp_values, column_name, source_script):
    # set_value = get_column_set_value(column_name, StagingTableSubClass.Script.Script)
    # nr_exp_values_found = 0
    # for exp_value in exp_values:
    #     nr_exp_values_found += sum(1 for i in set_value if i == exp_value)

    # if nr_exp_values_found != len(set_value):
    #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': column ' + column_name + ' set to ' + ' or '.join(set_value) + '. Must be ' + ' or '.join(exp_values))

    return fact_check_errors


def parse_script(fact_check_errors, source_script, column_info, fact, fact_check_num, space_id, subject_area):
    #   - 1: script must start with comment /* */.
    #   - 2: variable declarations (lines starting with DIM):
    #     -  variables should have prefix ‘v_’
    #     - should be sorted alphabetically
    #     - should have an initial value
    #   - 3: start and end sections for FUNCTIONS:
    #   - 4: TBD: Function ‘[ROUND_FLOAT]’ checks
    #   - 5: variables with initial value must be set, but can be any section.
    #   - 6: check fact key column is set in section: Key column mapping
    #   - 7: check foreign key columns are set section: Set Foreign keys
    #   - 8: check etl columns are set in section ETL column mapping
    #   - 9: check ims columns are set in section IMS column mapping
    #   - 10: check loc columns zre set in section LOC column mapping
    #   - 11: check remaining columns are in section: Column mapping
    #   - 12: WRITERECORD must exist

    remaining_script = source_script

    # 1. script must start with comment /* */.
    line, remaining_script = get_line(remaining_script)
    comment_start_found = False
    comment_end_found = False
    if not line.startswith(r'/*'):
        fact_check_errors.append('check #' + str(fact_check_num).rjust(
            2) + ': parse script: first line is not the comment line with the description')
    else:
        comment_start_found = True

    if comment_start_found:
        while remaining_script:
            line, remaining_script = get_line(remaining_script)
            if line.startswith(r'*/'):
                comment_end_found = True
                break

        if not comment_end_found:
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': ' + r'parse script: no line in script ending the comment line with: */')

    remaining_script = source_script

    # 2. variable declaration checks (lines starting with DIM):
    #    - variables should have prefix ‘v_’
    #    - should be sorted alphabetically
    #    - should have an initial value
    script_dim_lines = []
    function_dim_lines = []
    var_in_function = False
    while remaining_script:
        line, remaining_script = get_line(remaining_script)
        if line.startswith(r'FUNCTION'):
            var_in_function = True
        else:
            if line.startswith(r'END FUNCTION'):
                var_in_function = False
            else:
                if line.startswith('DIM '):
                    if var_in_function:
                        function_dim_lines.append(line)
                    else:
                        script_dim_lines.append(line)

    for dim_line in script_dim_lines:
        script_variable = dim_line.partition('[')[2].partition(']')[0].strip()
        if not script_variable.startswith('v_'):
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': parse script: script variable: ' + script_variable + ' does not start with: v_')

    for dim_line in function_dim_lines:
        script_variable = dim_line.partition('[')[2].partition(']')[0].strip()
        if not script_variable.startswith('v_'):
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': parse script: function variable: ' + script_variable + ' does not start with: v_')

    if script_dim_lines != sorted(script_dim_lines):
        fact_check_errors.append('check #' + str(fact_check_num).rjust(
            2) + ': parse script: local script variable declarations are not sorted')

    init_value_null_dims_values = []
    for dim_line in script_dim_lines:
        init_value = dim_line.partition(' = ')[2].strip()
        if not init_value:
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': parse script: script variable: ' + script_variable + ' does not have an initial value')
        else:
            if init_value == 'NULL':
                init_value_null_dims_values.append(dim_line)

    # 3. start and end sections for FUNCTIONS:
    remaining_script = source_script

    functions_start_section = False
    functions_end_section = False
    while remaining_script:
        line, remaining_script = get_line(remaining_script)
        if line.startswith(r'//') and 'START FUNCTIONS' in line:
            functions_start_section = True
            functions_end_section = False
        else:
            if line.startswith(r'//') and 'END FUNCTIONS' in line:
                functions_end_section = True
                break

    if not (functions_start_section):
        fact_check_errors.append(
            'check #' + str(fact_check_num).rjust(2) + ': parse script: section does not exist: START FUNCTIONS')

    if not (functions_end_section):
        fact_check_errors.append(
            'check #' + str(fact_check_num).rjust(2) + ': parse script: section does not exist: END FUNCTIONS')

    # 4. TBD: Function ‘[ROUND_FLOAT]’ checks
    # get all columns with type: Number
    # number_columns = []
    # for column_name in column_info.keys():
    #     if column_info[column_name]['DataType'] == 'Number':
    #         number_columns.append(column_name)

    # number_columns.sort()

    # 5. variables with initial value must be set, but can be any section.
    current_section = ''
    section_lines = {}

    remaining_script = source_script

    while remaining_script:
        line, remaining_script = get_line(remaining_script)
        if line.startswith(r'//'):
            section_found = line.partition('-- ')[2].partition(' --')[0].strip()
            if section_found:
                current_section = section_found
        else:
            if line.strip() and not line.startswith(r'/*'):
                if current_section not in section_lines.keys():
                    section_lines[current_section] = []

                section_lines[current_section].append(line)

    for dim_line in init_value_null_dims_values:
        script_variable = dim_line.partition('[')[2].partition(']')[0].strip()
        if script_variable:
            init_var_found = False
            for section_line_key in section_lines.keys():
                for section_line in section_lines[section_line_key]:
                    if '[' + script_variable + '] =' in section_line:
                        init_var_found = True
                        break

                if init_var_found:
                    break

            if not init_var_found:
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': parse script: variable: ' + script_variable + ', not initalized in any section')

    # 6. check fact key column is set in section: Key column mapping
    sorted_column_info = OrderedDict(sorted(column_info.items()))
    remaining_columns = copy.deepcopy(sorted_column_info)
    fact_column_name = fact.replace(' ', '_') + '_Key'
    fact_key_column_found = False
    # logger.info('fact column_name = ' + fact_column_name)
    if 'Key column mapping' in section_lines.keys():
        for section_line in section_lines['Key column mapping']:
            if '[' + fact_column_name + '] =' in section_line:
                fact_key_column_found = True
                break

        if not fact_key_column_found:
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': parse script: fact key column: ' + fact_column_name + ', not mapped in section: Key column mapping')
    else:
        fact_check_errors.append(
            'check #' + str(fact_check_num).rjust(2) + ': parse script: section does not exist: Key column mapping')

    if fact_column_name in remaining_columns.keys():
        del (remaining_columns[fact_column_name])

    # 7. check foreign key columns are set section: Set Foreign keys
    for column_name in sorted_column_info.keys():
        if column_name in remaining_columns.keys():
            fk_column_found = False
            if column_name.endswith('_Key') and column_name != fact_column_name:
                # logger.info('fk column_name = ' + column_name)
                if 'Set Foreign keys' in section_lines.keys():
                    for section_line in section_lines['Set Foreign keys']:
                        if '[' + column_name + '] =' in section_line:
                            fk_column_found = True
                            break

                    if not fk_column_found:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: etl column: ' + column_name + ', not mapped in section: Set Foreign keys')
                else:
                    section_errors = list(
                        filter(lambda x: x.endswith('section does not exist: Set Foreign keys'), fact_check_errors))
                    if not section_errors:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: section does not exist: Set Foreign keys')

                del (remaining_columns[column_name])

    # 8. check etl columns are set in section ETL column mapping
    for column_name in sorted_column_info.keys():
        if column_name in remaining_columns.keys():
            etl_column_found = False
            if 'etl' in column_name:
                # logger.info('etl column_name = ' + column_name)
                if 'ETL column mapping' in section_lines.keys():
                    for section_line in section_lines['ETL column mapping']:
                        if '[' + column_name + '] =' in section_line:
                            etl_column_found = True
                            break

                    if not etl_column_found:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: etl column: ' + column_name + ', not mapped in section: ETL column mapping')
                else:
                    section_errors = list(
                        filter(lambda x: x.endswith('section does not exist: ETL column mapping'), fact_check_errors))
                    if not section_errors:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: section does not exist: ETL column mapping')

                del (remaining_columns[column_name])

    # 9. check ims columns are set in section IMS column mapping
    for column_name in sorted_column_info.keys():
        if column_name in remaining_columns.keys():
            ims_column_found = False
            if 'ims' in column_name:
                # logger.info('ims column_name = ' + column_name)
                if 'IMS column mapping' in section_lines.keys():
                    for section_line in section_lines['IMS column mapping']:
                        if '[' + column_name + '] =' in section_line:
                            ims_column_found = True
                            break

                    if not ims_column_found:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: ims column: ' + column_name + ', not mapped in section: IMS column mapping')

                else:
                    section_errors = list(
                        filter(lambda x: x.endswith('section does not exist: IMS column mapping'), fact_check_errors))
                    if not section_errors:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: section does not exist: IMS column mapping')

                del (remaining_columns[column_name])

    # 10. check loc columns zre set in section LOC column mapping
    for column_name in sorted_column_info.keys():
        if column_name in remaining_columns.keys():
            loc_column_found = False
            if column_name.startswith('loc'):
                # logger.info('loc column_name = ' + column_name)
                if 'LOC column mapping' in section_lines.keys():
                    for section_line in section_lines['LOC column mapping']:
                        if '[' + column_name + '] =' in section_line:
                            loc_column_found = True
                            break

                    if not loc_column_found:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: loc column: ' + column_name + ', not mapped in section: LOC column mapping')

                else:
                    section_errors = list(
                        filter(lambda x: x.endswith('section does not exist: LOC column mapping'), fact_check_errors))
                    if not section_errors:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': parse script: section does not exist: LOC column mapping')

                del (remaining_columns[column_name])

    # 11. check remaining columns are in section: Column mapping
    for column_name in sorted_column_info.keys():
        if column_name in remaining_columns.keys():
            column_found = False
            # logger.info('column_name = ' + column_name)
            if 'Column mapping' in section_lines.keys():
                for section_line in section_lines['Column mapping']:
                    if '[' + column_name + '] =' in section_line:
                        column_found = True
                        break

                if not column_found:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': parse script: column: ' + column_name + ', not mapped in section: Column mapping')

            else:
                section_errors = list(
                    filter(lambda x: x.endswith('section does not exist: Column mapping'), fact_check_errors))
                if not section_errors:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': parse script: section does not exist: Column mapping')

            if column_name in remaining_columns.keys():
                del (remaining_columns[column_name])

    for column_key in remaining_columns.keys():
        fact_check_errors.append(
            'check #' + str(fact_check_num).rjust(2) + ': parse script: column not mapped: ' + column_key)

    # 12. WRITERECORD must exist
    if 'WRITERECORD' not in source_script:
        fact_check_errors.append(
            'check #' + str(fact_check_num).rjust(2) + ': parse script: WRITERECORD does not exist')

    return fact_check_errors


def parse_fact(soap_session, space_prefix, space_name, check_space_id, birst_source, StagingTableSubClass,
               sa_saved_expressions, enum_table_field_defs, prefix, check_numbers):
    error = ''
    fact_check_errors = []
    columns_info = {}
    columns_info = get_column_info(StagingTableSubClass.Columns.SourceColumnSubClass)

    ln_table = get_ln_table_from_input_query(StagingTableSubClass.Script.InputQuery)
    space_to_check = space_prefix + ' ' + space_name

    hierarchy_names = []
    for column_name in columns_info.keys():
        if columns_info[column_name]['Hierarchy']:
            if columns_info[column_name]['Hierarchy'] not in hierarchy_names:
                hierarchy_names.append(columns_info[column_name]['Hierarchy'])

    grain_levels = []
    soap_session, error, grain_levels = admin_get_source_grain_levels(soap_session, space_to_check, check_space_id,
                                                                      birst_source)
    if error:
        raise RuntimeError(error)

    soap_session, error, source_properties = admin_get_source_properties(soap_session, space_to_check, check_space_id,
                                                                         birst_source)
    if error:
        raise RuntimeError(error)

    for column_name in sorted(columns_info.keys()):
        # print('column name = ' + column_name)
        try:
            # 1. Optional: Filter, Sort and ims_<LN table column>_kw
            #    Filter column:
            #    - check Sort column exists
            #    - check ims_<LN table column>_kw column exists
            #    - Type Varchar
            #    - Hierarchy filled
            #    Sort column:
            #    - check Filter column exists
            #    - check ims_<LN table column>_kw column exists
            #    - Type Integer
            #    - Hierarchy filled
            #    ims_<LN table column>_kw column:
            #    - check Filter column exists
            #    - check Sort column exists
            #    - Type Varchar
            #    - Hierarchy filled
            #    Filter column and Sort colum:
            #    - check saved expression for translations exists
            #      for columns Order Status Filter and Order Status Sort the saved expression Order Status must exist
            fact_check_num = 1
            if not check_numbers or fact_check_num in check_numbers:
                fact_check_errors = check_columns_mapped_to_enum( \
                    fact_check_errors, column_name, enum_table_field_defs, columns_info, \
                    StagingTableSubClass.Script.Script, fact_check_num, \
                    sa_saved_expressions)

            # 2. <hierarchy>_Key
            #    - Type: Varchar
            #    - Hierarchy: must be filled with <hierarchy>
            #    - Column name must be: <hierarchy>_Key, <hierarchy>_Level_Key or Child_<hierarchy>_Level_Key 
            #    - Measure: Flag must be set for data security.
            fact_check_num = 2
            if not check_numbers or fact_check_num in check_numbers:
                if column_name.endswith('_Key'):
                    hierarchy_part = column_name.partition('_Key')[0].strip()
                    if hierarchy_part.startswith('Child_'):
                        if hierarchy_part.endswith('_Level'):
                            hierarchy_part = hierarchy_part.replace('Child_', '', 1).replace('_Level', '', 1)
                    else:
                        if hierarchy_part.endswith('_Level'):
                            hierarchy_part = hierarchy_part.replace('_Level', '', 1)
                    if hierarchy_part.replace('_', ' ') in hierarchy_names or \
                            hierarchy_part.replace('_', ' ').replace(' ', '-', 1) in hierarchy_names:
                        # if columns_info[column_name]['DataType'] != 'Integer':
                        #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': column ' + column_name +' has incorrect type. Type = ' + columns_info[column_name]['DataType'] + ', must be Integer')

                        if columns_info[column_name]['Measure'] != True:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' has column Measure not checked. Must be checked for data security.')

                    else:
                        if columns_info[column_name]['Hierarchy']:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' has incorrect hierarchy in name. Hierarchy is ' +
                                                     columns_info[column_name][
                                                         'Hierarchy'] + ', so column name must be ' +
                                                     columns_info[column_name]['Hierarchy'].replace('-', ' ',
                                                                                                    1).replace(' ',
                                                                                                               '_') + '_Key')
                        else:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' not targeted to hierarchy. Must be filled')

            # 3. Optional: etl_<object>_key and etl_<object>_sequencenumber
            #    - etl_<object>_key
            #       - etl_<object>_sequencenumber must also exist.
            #       - Type: Varchar
            #    - etl_<object>_sequencenumber, then
            #      - etl_<object>_key must exist.
            #      - Type: Integer
            #    - both columns targeting to same hierarchy
            fact_check_num = 3
            if not check_numbers or fact_check_num in check_numbers:
                if column_name.startswith('etl') and column_name.endswith('_key'):
                    object_part = column_name.partition('etl_')[2].strip().partition('_key')[0].strip()
                    if '_specific_' not in object_part:
                        if columns_info[column_name]['DataType'] != 'Varchar':
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' has incorrect type. Type = ' +
                                                     columns_info[column_name]['DataType'] + ', must be Varchar')

                        etl_seq_nr_column = 'etl_' + object_part + '_sequencenumber'

                        if etl_seq_nr_column in columns_info.keys():
                            if columns_info[column_name]['Hierarchy'] != columns_info[etl_seq_nr_column]['Hierarchy']:
                                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                    2) + ': column ' + column_name + ' has hierarchy ' + columns_info[column_name][
                                                             'Hierarchy'] + ' and column ' + etl_seq_nr_column + ' has hierarchy ' +
                                                         columns_info[etl_seq_nr_column][
                                                             'Hierarchy'] + '. Must be equal')
                        else:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + etl_seq_nr_column + ' does not exist. Required with column ' + column_name)

                if column_name.startswith('etl') and column_name.endswith('_sequencenumber'):
                    object_part = column_name.partition('etl_')[2].strip().partition('_sequencenumber')[0].strip()
                    if '_specific_' not in object_part:
                        if columns_info[column_name]['DataType'] != 'Integer':
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' has incorrect type. Type = ' +
                                                     columns_info[column_name]['DataType'] + ', must be Integer')

                        fact_check_errors = check_set_values(fact_check_errors, ['0'], column_name,
                                                             StagingTableSubClass.Script.Script)

                        etl_key_column = 'etl_' + object_part + '_key'
                        if etl_key_column in columns_info.keys():
                            if columns_info[column_name]['Hierarchy'] != columns_info[etl_key_column]['Hierarchy']:
                                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                    2) + ': column ' + column_name + ' has hierarchy ' + columns_info[column_name][
                                                             'Hierarchy'] + ' and column ' + etl_key_column + ' has hierarchy ' +
                                                         columns_info[etl_key_column]['Hierarchy'] + '. Must be equal')
                        else:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + etl_key_column + ' does not exist. Required with column ' + column_name)

            # 5. <prefix> <measure name>
            #   - column Measure / AnalyzeByDate is checked
            fact_check_num = 5
            if not check_numbers or fact_check_num in check_numbers:
                if column_name.startswith(prefix) and column_name[4] == ' ' and column_name[5:] != 'deleted':
                    if columns_info[column_name]['DataType'] == 'DateTime':
                        if columns_info[column_name]['AnalyzeByDate'] != True:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' has column AnalyzeByDate not checked. Must be checked when Type = DateTime')
                    else:
                        if columns_info[column_name]['Measure'] != True:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' has column Measure not checked. Must be checked when Type = ' +
                                                     columns_info[column_name]['DataType'])

            # 15. Width
            #   - length of data type 'Varchar' is 50 or multiple of.
            #   - length of all other data types is 1.
            fact_check_num = 15
            if not check_numbers or fact_check_num in check_numbers:
                if columns_info[column_name]['DataType'] == 'Varchar':
                    if columns_info[column_name]['Width'] < 50 or columns_info[column_name]['Width'] % 50 != 0:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': column ' + column_name + ' with type Varchar has incorrect width. Width = ' + str(
                            columns_info[column_name]['Width']) + ', must be 50 or multiple of')
                else:
                    if columns_info[column_name]['Width'] != 1:
                        fact_check_errors.append(
                            'check #' + str(fact_check_num).rjust(2) + ': column ' + column_name + ' with type ' +
                            columns_info[column_name]['DataType'] + ' has incorrect width. Width = ' + str(
                                columns_info[column_name]['Width']) + ', must be 1')

            # 16. LockType
            #   - unchecked for all columns
            fact_check_num = 16
            if not check_numbers or fact_check_num in check_numbers:
                if columns_info[column_name]['LockType']:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ' has LockType checked. Must be unchecked')

            # 17. Format
            #   - empty for all columns
            fact_check_num = 17
            if not check_numbers or fact_check_num in check_numbers:
                if columns_info[column_name]['Format']:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ' has incorrect format: Format = ' + columns_info[column_name][
                                                 'Format'] + ', must be empty')

            # 18. Unknown Value
            #    - Null/Unknown Value Replacement
            #      Varchar		*
            #      Integer		0
            #      Float		0
            #      Number	0
            #      DateTime	1970-01-01T00:00:00Z
            #      Date		1900-01-01 or 9999-12-31
            # if columns_info[column_name]['DataType'] == 'Varchar' and column.UnknownValue != '*':
            #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': ' + column_name + ': Type = ' + columns_info[column_name]['DataType'] + ' and UnknownValue = ' + column.UnknownValue + ', UnknownValue must be *')
            # elif columns_info[column_name]['DataType'] == 'Integer' and column.UnknownValue != '0':
            #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': ' + column_name + ': Type = ' + columns_info[column_name]['DataType'] + ' and UnknownValue = ' + column.UnknownValue + ', UnknownValue must be 0')
            # elif columns_info[column_name]['DataType'] == 'Float' and column.UnknownValue != '0':
            #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': ' + column_name + ': Type = ' + columns_info[column_name]['DataType'] + ' and UnknownValue = ' + column.UnknownValue + ', UnknownValue must be 0')
            # elif columns_info[column_name]['DataType'] == 'Number' and column.UnknownValue != '0':
            #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': ' + column_name + ': Type = ' + columns_info[column_name]['DataType'] + ' and UnknownValue = ' + column.UnknownValue + ', UnknownValue must be 0')
            # elif columns_info[column_name]['DataType'] == 'DateTime' and column.UnknownValue != '1970-01-01T00:00:00Z':
            #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': ' + column_name + ': Type = ' + columns_info[column_name]['DataType'] + ' and UnknownValue = ' + column.UnknownValue + ', UnknownValue must be 1970-01-01T00:00:00Z')
            # elif columns_info[column_name]['DataType'] == 'Date' and column.UnknownValue != '1900-01-01' and  column.UnknownValue != '9999-12-31':
            #     fact_check_errors.append('check #' + str(fact_check_num).rjust(2) + ': ' + column_name + ': Type = ' + columns_info[column_name]['DataType'] + ' and UnknownValue = ' + column.UnknownValue + ', UnknownValue must be 1900-01-01 or 9999-12-31')
            fact_check_num = 18

            # 19. grain
            #    - levels on Grain tab must be according to hierachies on Columns tab
            fact_check_num = 19
            if not check_numbers or fact_check_num in check_numbers:
                if columns_info[column_name]['Hierarchy'] and columns_info[column_name][
                    'Hierarchy'] not in grain_levels:
                    fact_check_errors.append(
                        'check #' + str(fact_check_num).rjust(2) + ': column ' + column_name + ' with Hierarchy = ' +
                        columns_info[column_name]['Hierarchy'] + ' not checked on Grain tab. Must be checked')

            # 22 saved expressions for measures in currency an unit
            fact_check_num = 22
            if not check_numbers or fact_check_num in check_numbers:
                # check last 3 characters
                if column_name[-3:] in currency_types and column_name.startswith(prefix):
                    currency_type = column_name[-3:]
                    if not columns_info[column_name]['Measure']:
                        fact_check_errors.append('check #' + str(fact_check_num).rjust(
                            2) + ': column ' + column_name + ' ends with currency type, but is not a measure. Must be a measure')
                    else:
                        saved_expression_name = column_name.partition(prefix)[2].strip().partition(currency_type)[
                            0].strip()
                        if saved_expression_name not in sa_saved_expressions:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': saved expression not found for measure column ' + column_name + '. Missing saved_expression: ' + saved_expression_name)
                else:
                    if column_name[-3:] in unit_types and column_name.startswith(prefix):
                        unit_type = column_name[-3:]
                        if not columns_info[column_name]['Measure']:
                            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                2) + ': column ' + column_name + ' ends with unit type, but is not a measure. Must be a measure')
                        else:
                            saved_expression_name = column_name.partition(prefix)[2].strip().partition(unit_type)[
                                0].strip()
                            if saved_expression_name not in sa_saved_expressions:
                                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                                    2) + ': saved expression not found for measure column ' + column_name + '. Missing saved_expression: ' + saved_expression_name)

            # 23 analyze measure not checked
            fact_check_num = 23
            if not check_numbers or fact_check_num in check_numbers:
                if columns_info[column_name]['AnalyzeMeasure']:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + column_name + ' has AnalyzeMeasure checked. Must be unchecked')

        except AttributeError as e:
            if is_NoneType(StagingTableSubClass.Script):
                raise RuntimeError('No Script found')
            else:
                raise RuntimeError(e.args[0] + '\n')

    # 4. etl_<fact name>_specific_key
    #   - Type: Varchar
    #   - Hierarchy: etl_<fact name>_specific
    #   - Mandatory column
    fact_check_num = 4
    if not check_numbers or fact_check_num in check_numbers:
        etl_fact_specific_key = 'etl_' + birst_source.replace(' ', '_').lower() + '_specific_key'
        if etl_fact_specific_key in columns_info.keys():
            if columns_info[etl_fact_specific_key]['DataType'] != 'Varchar':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + 'has incorrect type. Type = ' + columns_info[column_name][
                                             'DataType'] + ', must be Varchar')

            if columns_info[etl_fact_specific_key]['Hierarchy']:
                etl_fact_specific = etl_fact_specific_key.partition('_key')[0].strip()
                if columns_info[etl_fact_specific_key]['Hierarchy'] != etl_fact_specific:
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + etl_fact_specific_key + ' mapped to incorrect hierarchy. Must be ' + etl_fact_specific)
            else:
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + etl_fact_specific_key + ' not targeted to hierarchy. Must be filled')
        else:
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + etl_fact_specific_key + ' does not exist. Mandatory column')

    # 6. Optional: <prefix> deleted
    #    - Type: Integer
    fact_check_num = 6
    prefix_del_column = prefix + ' deleted'
    if not check_numbers or fact_check_num in check_numbers:
        if prefix_del_column in columns_info.keys():
            if columns_info[prefix_del_column]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_del_column + ' has incorrect type. Type = ' +
                                         columns_info[prefix_del_column]['DataType'] + ', must be Integer')

    # 7. <prefix>_etl_hidden
    #    - Mandatory column
    #    - Type: Integer
    fact_check_num = 7
    prefix_etl_hidden_column = prefix + '_etl_hidden'
    if not check_numbers or fact_check_num in check_numbers:
        if prefix_etl_hidden_column not in columns_info.keys():
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + prefix_etl_hidden_column + ' does not exist. Mandatory column')
        else:
            if columns_info[prefix_etl_hidden_column]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_etl_hidden_column + ' has incorrect type. Type = ' +
                                         columns_info[prefix_etl_hidden_column]['DataType'] + ', must be Integer')

            fact_check_errors = check_set_values(fact_check_errors, ['0', '1'], prefix_etl_hidden_column,
                                                 StagingTableSubClass.Script.Script)

    # 8. <prefix>_etl_<fact name>_sequencenumber
    #    - Mandatory column
    #    - Type: Integer
    fact_check_num = 8
    if not check_numbers or fact_check_num in check_numbers:
        prefix_etl_fact_seqnr_column = prefix + '_etl_' + birst_source.replace(' ', '_').lower() + '_sequencenumber'
        if prefix_etl_fact_seqnr_column not in columns_info.keys():
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + prefix_etl_fact_seqnr_column + ' does not exist. Mandatory column')
        else:
            if columns_info[prefix_etl_fact_seqnr_column]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_etl_fact_seqnr_column + ' has incorrect type. Type = ' +
                                         columns_info[prefix_etl_fact_seqnr_column]['DataType'] + ', must be Integer')

    # 9. <prefix>_etl_unknown_reference
    #    - Mandatory column
    #    - Type: Integer
    fact_check_num = 9
    prefix_etl_unref_column = prefix + '_etl_unknown_reference'
    if not check_numbers or fact_check_num in check_numbers:
        if prefix_etl_unref_column not in columns_info.keys():
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + prefix_etl_unref_column + ' does not exist. Mandatory column')
        else:
            if columns_info[prefix_etl_unref_column]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_etl_unref_column + ' has icorrect type. Type = ' +
                                         columns_info[prefix_etl_unref_column]['DataType'] + ', must be Integer')

            fact_check_errors = check_set_values(fact_check_errors, ['0', '1'], prefix_etl_unref_column,
                                                 StagingTableSubClass.Script.Script)

    # 10. <prefix>_etl_update_time
    #    - Mandatory column
    #    - Type: Integer
    fact_check_num = 10
    prefix_etl_upd_time_column = prefix + '_etl_update_time'
    if not check_numbers or fact_check_num in check_numbers:
        if prefix_etl_upd_time_column not in columns_info.keys():
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + prefix_etl_upd_time_column + ' does not exist. Mandatory column')
        else:
            if columns_info[prefix_etl_upd_time_column]['DataType'] != 'DateTime':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_etl_upd_time_column + ' has incorrect type. Type = ' +
                                         columns_info[prefix_etl_upd_time_column]['DataType'] + ', must be DateTime')

    # 11. <prefix>_ims_<LN table>_sequencenumber
    #    - Mandatory column
    #    - Type: Integer
    fact_check_num = 11
    if not check_numbers or fact_check_num in check_numbers:
        prefix_fact_count_column = prefix + ' ' + birst_source + ' Count'
        if prefix_fact_count_column not in columns_info.keys():
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + prefix_fact_count_column + ' does not exist. Mandatory column')
        else:
            if columns_info[prefix_fact_count_column]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_fact_count_column + ' has incorrect type. Type = ' +
                                         columns_info[prefix_fact_count_column]['DataType'] + ', must be Integer')

    # 12. <prefix>_ims_<LN Table>_sequencenumber
    #    - Mandatory column
    #    - Type: Integer
    fact_check_num = 12
    if not check_numbers or fact_check_num in check_numbers:
        if ln_table:
            prefix_ims_table_seqnr_column = prefix + '_ims_' + ln_table + '_sequencenumber'
            if prefix_ims_table_seqnr_column not in columns_info.keys():
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + prefix_ims_table_seqnr_column + ' does not exist. Mandatory column')
            else:
                if columns_info[prefix_ims_table_seqnr_column]['DataType'] != 'Integer':
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + prefix_ims_table_seqnr_column + ' has incorrect type. : Type = ' +
                                             columns_info[prefix_ims_table_seqnr_column][
                                                 'DataType'] + ', must be Integer')

    # 13. Optional: <prefix>_ims_<LN table>_sequencenumber_deleted
    #    - Type: Integer
    fact_check_num = 13
    if not check_numbers or fact_check_num in check_numbers:
        if ln_table:
            prefix_ims_table_seqnr_del_column = prefix + '_ims_' + ln_table + '_sequencenumber_deleted'
            if prefix_ims_table_seqnr_del_column in columns_info.keys():
                if columns_info[prefix_ims_table_seqnr_del_column]['DataType'] != 'Integer':
                    fact_check_errors.append('check #' + str(fact_check_num).rjust(
                        2) + ': column ' + prefix_ims_table_seqnr_del_column + ' has incorrect type. Type = ' +
                                             columns_info[prefix_ims_table_seqnr_del_column][
                                                 'DataType'] + ', must be Integer')

    # 14. deleted
    #    - when existing, Hierarchy is filled
    #    - Type: Integer
    fact_check_num = 14
    if not check_numbers or fact_check_num in check_numbers:
        column_name = 'deleted'
        if column_name in columns_info.keys():
            if not columns_info[column_name]['Hierarchy']:
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has empty hierarchy. Must be filled')

            if columns_info[column_name]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                             'DataType'] + ', must be Integer')

            fact_check_errors = check_set_values(fact_check_errors, ['0', '1'], column_name,
                                                 StagingTableSubClass.Script.Script)

    # 15. etl_hidden
    #    - when existing, Hierarchy is filled
    #    - Type: Integer
    fact_check_num = 15
    if not check_numbers or fact_check_num in check_numbers:
        column_name = 'etl_hidden'
        if column_name in columns_info.keys():
            if not columns_info[column_name]['Hierarchy']:
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has empty hierarchy. Must be filled')

            if columns_info[column_name]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                             'DataType'] + ', must be Integer')

            fact_check_errors = check_set_values(fact_check_errors, ['0', '1'], column_name,
                                                 StagingTableSubClass.Script.Script)

    # 16. etl_update_time
    #    - when existing, Hierarchy is filled
    #    - Type: DateTime
    fact_check_num = 16
    if not check_numbers or fact_check_num in check_numbers:
        column_name = 'etl_update_time'
        if column_name in columns_info.keys():
            if not columns_info[column_name]['Hierarchy']:
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has empty hierarchy. Must be filled')

            if columns_info[column_name]['DataType'] != 'DateTime':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                             'DataType'] + ', must be DateTime')

            fact_check_errors = check_set_values(fact_check_errors, ['NOW'], column_name,
                                                 StagingTableSubClass.Script.Script)

    # 17. loc_record_changed
    #    - Mandatory column
    #    - Hierarchy is filled
    #    - Type: Integer
    fact_check_num = 17
    if not check_numbers or fact_check_num in check_numbers:
        column_name = 'loc_record_changed'
        if column_name in columns_info.keys():
            if not columns_info[column_name]['Hierarchy']:
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has empty hierarchy. Must be filled')

            if columns_info[column_name]['DataType'] != 'Integer':
                fact_check_errors.append('check #' + str(fact_check_num).rjust(
                    2) + ': column ' + column_name + ' has incorrect type. Type = ' + columns_info[column_name][
                                             'DataType'] + ', must be Integer')

            fact_check_errors = check_set_values(fact_check_errors, ['1'], column_name,
                                                 StagingTableSubClass.Script.Script)
        else:
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': column ' + column_name + ' does not exist. Mandatory column')

    # 20. properties
    #   - field 'Do not process when used by another script' checked.
    #   - field 'Rows in data source are transactions (vs. snapshot)' checked.
    #   - advanced option: Bulk insert and delete measure records is checked.
    fact_check_num = 20
    if not check_numbers or fact_check_num in check_numbers:
        if source_properties[0] != 'true':
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': property not checked: Do not process when used by another script')

        if source_properties[1] != 'true':
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': property not checked: Rows in data source are transactions (vs. snapshot)')

        if source_properties[2] != 'true':
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': property not checked: Bulk insert and delete measure records')

        fact_key_attribute = birst_source.replace(' ', '_') + '_Key'
        if fact_key_attribute not in source_properties[3]:  # list of checked specific delete keys
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': property key attribute of fact table ' + fact_key_attribute + ' not checked')

    # 21. script checks
    fact_check_num = 21
    if not check_numbers or fact_check_num in check_numbers:
        fact_check_errors = parse_script(fact_check_errors, StagingTableSubClass.Script.Script, columns_info,
                                         birst_source, fact_check_num, check_space_id, space_name)

    # 24 check sequence number in select statement where clause
    fact_check_num = 24
    if not check_numbers or fact_check_num in check_numbers:
        where_clause = StagingTableSubClass.Script.InputQuery.partition('WHERE')[2].partition('\n')[0].strip()
        if 'sequencenumber' not in where_clause:
            fact_check_errors.append('check #' + str(fact_check_num).rjust(
                2) + ': sequencenumber check not found in the select statement where clause.')

    # 25 checks for Admin/Manage Space/
    #   Model Properties
    #   - Process engine version must be ‘Default’
    #   - Processing time zone should be ‘UTC (00:00) Monrovia/Reykjavik’
    #   - Space comments
    #   Model Scan
    #   - Run feature and report errors

    return fact_check_errors


# admin service functions
# =========================
def admin_service_get_base_url():
    return (admin_service_base_url)


def admin_init_soap_session(session):
    soap_session = {}

    # set session
    soap_session[ADMIN_SESSION_KEY] = session

    # set url
    soap_session[ADMIN_URL_KEY] = admin_service_get_base_url() + '/AdminService.asmx'

    # set headers without the soap action service name
    headers = {}
    headers[ADMIN_CONTENT_TYPE_KEY] = ADMIN_CONTENT_TYPE_VALUE
    headers[ADMIN_SOAP_ACTION_KEY] = ADMIN_SOAP_ACTION_TEMPLATE
    headers[ADMIN_XSRF_TOKEN_KEY] = session.cookies['XSRF-TOKEN']
    soap_session[ADMIN_HEADERS_KEY] = headers

    return (soap_session)


def admin_get_etl_hidden_info():
    return (ADMIN_ETL_HIDDEN_SUFFIX, ADMIN_ETL_HIDDEN_VARIABLE)


def admin_get_url(soap_session):
    return (soap_session[ADMIN_URL_KEY])


def admin_get_headers(soap_session, service_name):
    headers = {}
    headers = soap_session[ADMIN_HEADERS_KEY]
    headers[ADMIN_SOAP_ACTION_KEY] = ADMIN_SOAP_ACTION_TEMPLATE + service_name.strip()
    if ADMIN_BIRST_SPACID in headers.keys():
        del headers[ADMIN_BIRST_SPACID]

    return (headers)


def admin_get_smi_headers(soap_session, space_id):
    headers = {}
    headers = soap_session[ADMIN_HEADERS_KEY]
    if ADMIN_SOAP_ACTION_KEY in headers.keys():
        del headers[ADMIN_SOAP_ACTION_KEY]
    headers[ADMIN_BIRST_SPACID] = space_id

    return (headers)


def admin_get_session(soap_session):
    return (soap_session[ADMIN_SESSION_KEY])


def admin_get_body(service_name, body_sub_elements):
    body = ADMIN_BODY_TEMPLATE
    sub_elements_xml = ''
    logging.info("Service name = " + service_name)
    logging.info("===============================")
    for sub_element_key in body_sub_elements.keys():
        if isinstance(body_sub_elements[sub_element_key], str):
            sub_elements_xml = sub_elements_xml + '\n<' + sub_element_key + '>' + body_sub_elements[
                sub_element_key] + '</' + sub_element_key + '>'
        else:
            # body_sub_elements['objecttypes']['string'] = 'str1, str2, str3'
            # body_sub_elements['objecttypes']['string'] = 'str1, str2, str3'
            # will be :
            # <objecttypes>
            #    <string>str1</string>
            #    <string>str2</string>
            #    <string>str3</string>
            # </objecttypes>
            sub_elements_xml = sub_elements_xml + '\n<' + sub_element_key + '>'
            for sub_sub_element_key in body_sub_elements[sub_element_key].keys():
                for element in body_sub_elements[sub_element_key][sub_sub_element_key].split(','):
                    sub_elements_xml = sub_elements_xml + '\n<' + sub_sub_element_key + '>' + element.strip() + '</' + sub_sub_element_key + '>'

            sub_elements_xml = sub_elements_xml + '\n</' + sub_element_key + '>'

    if body_sub_elements:
        sub_elements_xml = sub_elements_xml + '\n'

    body = body.replace('</service_name>', sub_elements_xml + '</service_name>')
    body = body.replace('service_name', service_name)

    # logging.info(body)
    # logging.info("===============================")
    # logging.info(' ')

    return (body)


def admin_get_info(soap_session, service_name, body_sub_elements):
    url = admin_get_url(soap_session)
    headers = admin_get_headers(soap_session, service_name)
    session = admin_get_session(soap_session)
    body = admin_get_body(service_name, body_sub_elements)

    return (url, headers, session, body)


def admin_thread_update_cookies(session, wait_time):
    logging.info('Updating cookies every ' + str(wait_time) + ' seconds')
    while True:
        session.get(admin_service_get_base_url() + '/html/home.aspx')
        time.sleep(wait_time)


def admin_login(user_name, password, admin_base_url=ADMIN_BASE_URL):
    global admin_service_base_url
    soap_session = {}

    admin_service_base_url = admin_base_url

    session = requests.Session()

    get_response = requests.get(admin_base_url + '/Login.html')
    # login_page = BeautifulSoup(get_response.text, "html.parser")

    # view_state = login_page.find('input', {'name': ADMIN_VIEW_STATE_KEY}).get('value')
    # view_state_generator = login_page.find('input', {'name': ADMIN_VIEW_STATE_GEN_KEY}).get('value')
    # previous_page = login_page.find('input', {'name': ADMIN_PREVIOUS_PAGE_KEY}).get('value')
    # event_validation = login_page.find('input', {'name': ADMIN_EVENT_VALIDATION_KEY}).get('value')

    login_data = {
        'username': user_name,
        'password': password
    }
    session.post(admin_base_url + '/cerberus/rest/v1/login', data=login_data)
    session.get(admin_base_url + '/html/home.aspx')

    # set session, url and headers template
    soap_session = admin_init_soap_session(session)

    update_thread = threading.Thread(target=admin_thread_update_cookies, args=(session, ADMIN_INTERVAL_COOKIES,))
    update_thread.daemon = True
    update_thread.start()

    return (soap_session)


def admin_logout(soap_session):
    session = soap_session[ADMIN_SESSION_KEY]
    session.post(admin_service_get_base_url() + '/Logout.aspx')


def post_web_service(soap_session, web_service, body_sub_elements):
    error = ''
    nr_waits = 0

    while True:
        url, headers, session, body = admin_get_info(soap_session, web_service, body_sub_elements)
        response_post = session.post(url, data=body, headers=headers)

        error = ''
        status_code = 0
        if response_post.status_code != 200:
            error = 'admin_service_' + web_service + ' failed, reason: ' + response_post.reason
        else:
            if len(response_post.history) > 0:
                status_code = response_post.history[0].status_code

        if error:
            break

        if status_code == 302:  # incorrect cookies.
            # wait for update cookies in other thread
            nr_waits += 1
            logging.info('[' + str(nr_waits) + ']: Waiting for thread to update cookies ...')
            time.sleep(5)
            logging.info('[' + str(nr_waits) + ']: Waiting done')
        else:
            break

    return (soap_session, response_post, error)


def admin_service_getSpaceDetails(soap_session, space_name, space_id):
    space_details = ''
    error = ''

    # GetSpaceDetails has 2 sub elements
    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    response_post = result[1]
    error = result[2]

    if not error:
        space_details_xml = BeautifulSoup(response_post.text, "xml")
        space_details_result = space_details_xml.find('GetSpaceDetailsResult')
        space_details = space_details_result.contents

    return (soap_session, space_details, error)


def admin_service_isRepositoryAdmin(soap_session):
    repo_admin_result = ''
    error = ''

    # getLoggedInUserDetails has no sub elements
    body_sub_elements = {}

    result = post_web_service(soap_session, 'getLoggedInUserDetails', body_sub_elements)
    soap_session = result[0]
    response_post = result[1]
    error = result[2]

    if not error:
        user_details_xml = BeautifulSoup(response_post.text, "xml")
        repo_admin_result = user_details_xml.find('RepositoryAdmin').text

    return (soap_session, repo_admin_result, error)


def admin_service_ping(soap_session):
    ping_result = ''
    error = ''

    # ping has no sub elements
    body_sub_elements = {}

    result = post_web_service(soap_session, 'ping', body_sub_elements)
    soap_session = result[0]
    response_post = result[1]
    error = result[2]

    if not error:
        ping_xml = BeautifulSoup(response_post.text, "xml")
        ping_result = ping_xml.find('pingResult')

    return (soap_session, ping_result.text, error)


def admin_get_space_state(soap_session, space_name, space_id):
    error = ''
    space_state = ''

    soap_session, space_details, error = admin_service_getSpaceDetails(soap_session, space_name, space_id)

    if not error:
        for space_detail in space_details:
            if space_detail.name == 'SpaceState':
                space_state = space_detail.text

    return (soap_session, space_state, error)


def has_hidden_attribute(filepath):
    return bool(os.stat(filepath).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)


def admin_download_repository_files(svn_list, soap_session, space_prefix, space_name, space_id, svn_download_url,
                                    download_defs, logger,repo_base_url):
    event_targets_not_defined = []
    downloaded_files = []
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        repo_session = admin_get_session(soap_session)
        repository_url = admin_service_get_base_url() + '/Repository.aspx'
        response_get = repo_session.get(repository_url)
        rep_admin_page = BeautifulSoup(response_get.text, "html.parser")

        view_state = rep_admin_page.find('input', {'name': ADMIN_VIEW_STATE_KEY}).get('value')
        view_state_generator = rep_admin_page.find('input', {'name': ADMIN_VIEW_STATE_GEN_KEY}).get('value')
        # PREVIOUSPAGE = login_page.find('input', {'name': ADMIN_PREVIOUS_PAGE_KEY}).get('value')
        event_validation = rep_admin_page.find('input', {'name': ADMIN_EVENT_VALIDATION_KEY}).get('value')
        anchors = rep_admin_page.find_all('a')

        event_targets = {}
        for anchor in anchors:
            if 'id' in anchor.attrs.keys() and anchor.attrs['id']:
                if anchor.attrs['id'].startswith('Get') and 'Download' in anchor.string:
                    event_targets[anchor.attrs['id']] = anchor.string.replace('Download', '').strip()

        event_targets = OrderedDict(sorted(event_targets.items()))

        # check all event target / download filename combinations are defined in the config file
        for event_target in event_targets.keys():
            if event_target not in download_defs.keys():
                event_targets_not_defined.append(event_target)

        if not event_targets_not_defined:
            repo_download_upload_def = get_repo_download_upload_def()

            form_data = {
                ADMIN_VIEW_STATE_KEY: view_state,
                ADMIN_VIEW_STATE_GEN_KEY: view_state_generator,
                ADMIN_EVENT_VALIDATION_KEY: event_validation,
                ADMIN_EVENT_TARGET_KEY: '',
                ADMIN_EVENT_ARGUMENT_KEY: '',
                ADMIN_LAST_FOCUS_KEY: '',
                ADMIN_VIEW_STATE_ENCRYPTED_KEY: '',
                ADMIN_QUERY_CONNECTION_KEY: repo_download_upload_def[ADMIN_QUERY_CONNECTION_KEY][6],
                ADMIN_TIME_OUT_BOX_ID_KEY: repo_download_upload_def[ADMIN_TIME_OUT_BOX_ID_KEY][6],
                ADMIN_RESULT_SET_SIZE_BOX_ID_KEY: repo_download_upload_def[ADMIN_RESULT_SET_SIZE_BOX_ID_KEY][6],
                ADMIN_NAMED_ID_POLICY_LIST_KEY: repo_download_upload_def[ADMIN_NAMED_ID_POLICY_LIST_KEY][6]
            }

            nr_downloads = 0
            checkout_dir = svn_download_url.rpartition('/')[0].rpartition('/')[2].strip()
            checkout_done = False
            for event_target in event_targets.keys():
                form_data[ADMIN_EVENT_TARGET_KEY] = event_target

                file_name = download_defs[event_target].partition('.')[0] + '_' + space_prefix + space_name + '.' + \
                            download_defs[event_target].partition('.')[2].lower().replace(' ', '_')

                print('File name: ', file_name)
                response_post = repo_session.post(repository_url, data=form_data)
                if response_post.status_code != 200:
                    error = 'admin_download_repository_files failed, reason: ' + response_post.reason
                else:
                    rep_event_page = BeautifulSoup(response_post.text, "html.parser")
                    # if __VIEWSTATE on page and type = None, post event target resulted into empty download result, thus without error.
                    # only create files which are not empty
                    if is_NoneType(rep_event_page.find('input', {'name': ADMIN_VIEW_STATE_KEY})):
                        if nr_downloads == 0:
                            logger.info('SVN download url: ' + svn_download_url)
                            logger.info(' ')

                        nr_downloads += 1
                        download_file = svn_download_url + file_name
                        if svn_path_exists(download_file, svn_list):

                            # create checkout directory when not existing.
                            os.makedirs(checkout_dir, exist_ok=True)

                            # checkout directory when not done yet 
                            if not checkout_done:
                                with open('out.txt', 'w+') as fout:
                                    with open('err.txt', 'w+') as ferr:
                                        local_file = checkout_dir + '/' + file_name
                                        svn_command = 'svn checkout "' + repo_base_url + '/' + svn_download_url + '" "' + checkout_dir + '" --force '
                                        subprocess.call(svn_command, stdout=fout, stderr=ferr)
                                        ferr.seek(0)
                                        error = error + ferr.read()
                                        if not error:
                                            logger.info(
                                                '     checked out directory: ' + repo_base_url + '/' + svn_download_url)

                                checkout_done = True

                            with open(checkout_dir + '/' + file_name, 'wb') as fp:
                                fp.write(response_post.content)

                            logger.info('            downloaded file [' + str(nr_downloads) + ']: ' + file_name)
                            logger.info(
                                '                       size: ' + str(os.path.getsize(checkout_dir + '/' + file_name)))

                            downloaded_files.append(checkout_dir + '/' + file_name)
                        else:
                            # create checkout directory when not existing.
                            os.makedirs(checkout_dir, exist_ok=True)

                            # create file with content from post
                            with open(checkout_dir + '/' + file_name, 'wb') as fp:
                                fp.write(response_post.content)

                            # import file into svn.
                            with open('out.txt', 'w+') as fout:
                                with open('err.txt', 'w+') as ferr:
                                    local_file = checkout_dir + '/' + file_name
                                    svn_command = 'svn import "' + local_file + '" "' + repo_base_url + '/' + download_file + '" --message "Import file"'
                                    # print("Svn Import Command: ",svn_command)
                                    subprocess.call(svn_command, stdout=fout, stderr=ferr)
                                    ferr.seek(0)
                                    error = error + ferr.read()
                                    if not error:
                                        logger.info(
                                            '     imported download file [' + str(nr_downloads) + ']: ' + file_name)
                                        logger.info('                       size: ' + str(
                                            os.path.getsize(checkout_dir + '/' + file_name)))

                            downloaded_files.append(checkout_dir + '/' + file_name)

            if checkout_done and not error:
                with open('out.txt', 'w+') as fout:
                    with open('err.txt', 'w+') as ferr:
                        svn_command = 'svn commit "' + checkout_dir + '" --message "Committed done"'
                        subprocess.call(svn_command, stdout=fout, stderr=ferr)
                        ferr.seek(0)
                        error = ferr.read()
                        if not error:
                            fout.seek(0)
                            committed_files_list = fout.read().split('\n')
                            for committed_file in committed_files_list:
                                logger.info('Commit to SVN : ' + committed_file)

            if not error:
                current_dir = os.getcwd()
                os.remove('err.txt')
                os.remove('out.txt')
                if os.path.exists(checkout_dir + '/.svn'):
                    if has_hidden_attribute(checkout_dir + '/.svn'):
                        # Remove hidden and read only attributes of all files in .svn directory, so it can be deleted.
                        os.chdir(current_dir + '/' + checkout_dir + '/.svn')
                        sub_command = 'attrib.exe -H -R /s'
                        subprocess.call(sub_command)
                        os.chdir(current_dir)

                shutil.rmtree(checkout_dir)

        else:
            if len(event_targets_not_defined) > 1:
                error = 'No download filename defined for event types: ' + ",".join(event_targets_not_defined)
            else:
                error = 'No download filename defined for event type: ' + ",".join(event_targets_not_defined)

    return (soap_session, error, downloaded_files)


# path_and_file_name renamed to filepath
def get_file_content(filepath):
    data = ''
    mode = ""

    if filepath.endswith('.zip'):
        mode = "rb"
    else:
        mode = "r"
    # logging.info('Path name: ' + path_and_file_name)
    # print(filepath)
    # print('Path name: ', filepath)
    # filepath=filepath.strip()
    # print('File exists: ', os.path.exists(filepath))
    with open(filepath, mode) as fp1:
        data = fp1.read()
        # print(data)
        # Error found here
    return data


def admin_upload_repository_files(soap_session, download_upload_space_ids, download_url, upload_url, upload_space_name,
                                  upload_space_id, space_defs_upload_files, logger):
    error = ''
    nr_uploaded_files = 0
    repoint_connection = {}
    repoint_packages = {}

    body_sub_elements = {}
    body_sub_elements['spaceID'] = upload_space_id
    body_sub_elements['spaceName'] = upload_space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    # time.sleep(5)
    soap_session = result[0]
    if not result[2]:
        repo_session = admin_get_session(soap_session)
        repository_url = admin_service_get_base_url() + '/Repository.aspx'
        response_get = repo_session.get(repository_url)
        rep_admin_page = BeautifulSoup(response_get.content, "html.parser")

        view_state = rep_admin_page.find('input', {'name': ADMIN_VIEW_STATE_KEY}).get('value')
        view_state_generator = rep_admin_page.find('input', {'name': ADMIN_VIEW_STATE_GEN_KEY}).get('value')
        event_validation = rep_admin_page.find('input', {'name': ADMIN_EVENT_VALIDATION_KEY}).get('value')

        form_data = {
            ADMIN_VIEW_STATE_KEY: view_state,
            ADMIN_VIEW_STATE_GEN_KEY: view_state_generator,
            ADMIN_EVENT_VALIDATION_KEY: event_validation,
            ADMIN_EVENT_TARGET_KEY: '',
            ADMIN_EVENT_ARGUMENT_KEY: '',
            ADMIN_LAST_FOCUS_KEY: '',
            ADMIN_VIEW_STATE_ENCRYPTED_KEY: '',
            ADMIN_QUERY_CONNECTION_KEY: repo_download_upload_def[ADMIN_QUERY_CONNECTION_KEY][6],
            ADMIN_TIME_OUT_BOX_ID_KEY: repo_download_upload_def[ADMIN_TIME_OUT_BOX_ID_KEY][6],
            ADMIN_RESULT_SET_SIZE_BOX_ID_KEY: repo_download_upload_def[ADMIN_RESULT_SET_SIZE_BOX_ID_KEY][6],
            ADMIN_NAMED_ID_POLICY_LIST_KEY: repo_download_upload_def[ADMIN_NAMED_ID_POLICY_LIST_KEY][6]
        }

        logger.info('    uploading files:')

        for num_key, upload_def_key in enumerate(space_defs_upload_files[upload_space_name].keys(), start=1):

            file_content = ''
            # added extra line
            # form_file_full_path = ''
            form_file_full_path = space_defs_upload_files[upload_space_name][upload_def_key]
            # print(os.getcwd())
            # print('listing directories ',os.listdir())
            logger.info('        [' + str(num_key) + ']: ' + form_file_full_path.rpartition('\\')[2])
            # print("full_path_exists: ", os.path.exists(form_file_full_path))

            try:
                file_content = get_file_content(form_file_full_path)
            except Exception as e:
                print(e)

            if repo_download_upload_def[upload_def_key][5] == 'text/xml':
                file_content = file_content.partition('<')[1] + file_content.partition('<')[2]
                file_content = file_content.rpartition('>')[0] + file_content.rpartition('>')[1]

            if upload_def_key == ADMIN_REPOSITORY_KEY:
                # get the spaceids of the download url and upload url for the imported packages
                if '<PackageImport>' in file_content:
                    # get the space ids coming from the download url
                    space_id_elements = admin_get_source_data_elements(file_content, 'SpaceID', False)
                    # get the package names
                    package_name_elements = admin_get_source_data_elements(file_content, 'PackageName', False)
                    for pack_num, space_id_element in enumerate(space_id_elements):
                        old_space_id = admin_get_source_data_element_value(space_id_element, 'SpaceID', False)
                        if old_space_id in download_upload_space_ids.keys():
                            package_name = admin_get_source_data_element_value(package_name_elements[pack_num],
                                                                               'PackageName', False)
                            # Print Package name here
                            logger.info(package_name)
                            new_space_id = download_upload_space_ids[old_space_id][1]

                            space_ids_list = []
                            space_ids_list.append(old_space_id)
                            space_ids_list.append(new_space_id)
                            repoint_packages[package_name] = space_ids_list
            else:
                if upload_def_key == ADMIN_CONNECTOR_CONFIG_KEY:
                    # get repoint modeler connections
                    old_connection_guid = valid_base_urls[download_url][1]
                    if old_connection_guid in file_content:
                        new_connection_guid = valid_base_urls[upload_url][1]

                        space_ids_list = []
                        space_ids_list.append(old_connection_guid)
                        space_ids_list.append(new_connection_guid)
                        repoint_connection[upload_space_name] = space_ids_list

            file_param = {}
            file_param[repo_download_upload_def[upload_def_key][2]] = file_content
            # testing for file param
            # print(file_param)

            form_data[repo_download_upload_def[upload_def_key][3]] = repo_download_upload_def[upload_def_key][4]
            response_post = repo_session.post(repository_url, data=form_data, files=file_param)
            if response_post.status_code != 200:
                error = 'admin_upload_repository_files failed, reason: ' + response_post.reason
            else:
                nr_uploaded_files += 1
                del form_data[repo_download_upload_def[upload_def_key][3]]

            if error:
                break

    return (soap_session, error, nr_uploaded_files, repoint_connection, repoint_packages)


def admin_service_GetSpacePackages(soap_session, space_name, space_id):
    all_packages_by_space = ''
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'getAllPackages', body_sub_elements)

    if not error:
        all_packages_xml = BeautifulSoup(response_post.text, "xml")
        all_packages_by_space = all_packages_xml.find_all('PackageInfoLite')

    return (soap_session, all_packages_by_space, error)


def admin_service_getVariables(soap_session, space_name, space_id):
    all_variables_by_space = ''
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'getVariables', body_sub_elements)

    if not error:
        all_variables_xml = BeautifulSoup(response_post.text, "xml")
        all_variables_by_space = all_variables_xml.find_all('VariableWrapper')

    return (soap_session, all_variables_by_space, error)


def admin_service_GetMeasureTableData(soap_session, space_name, space_id, measure):
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'getDataFlow', body_sub_elements)

        if not error:
            start_names = 'ST_' + measure.strip().replace(' ', '_') + '&#x8;&#x8;datasources\r'
            end_names = '&#x8;'
            tag_names_start = response_post.text.partition(start_names)[2]
            tag_names = tag_names_start.partition(end_names)[0].strip()

            if not tag_names:
                error = 'admin_service_GetDataFlow failed, reason: no names found for fact ' + measure

            if not error:
                body_sub_elements = {}
                body_sub_elements['name'] = tag_names
                body_sub_elements['type'] = 'grain'

                soap_session, response_post, error = post_web_service(soap_session, 'getTableQueryString',
                                                                      body_sub_elements)
                if not error:
                    table_query_xml = BeautifulSoup(response_post.text, "xml")
                    table_query = table_query_xml.text

                    body_sub_elements = {}
                    body_sub_elements['name'] = tag_names
                    body_sub_elements['profile'] = 'false'
                    body_sub_elements['query'] = table_query

                    soap_session, response_post, error = post_web_service(soap_session, 'getMeasureTableData',
                                                                          body_sub_elements)
                    if not error:
                        all_data_xml = BeautifulSoup(response_post.text, "xml")
                        all_rows = all_data_xml.find_all('ArrayOfString')

    return (soap_session, all_rows, error)


def admin_service_GetStagingTableData(soap_session, space_name, space_id, ln_table_or_derived_data_source):
    error = ''
    all_rows = []

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        body_sub_elements['name'] = ln_table_or_derived_data_source
        body_sub_elements['type'] = 'staging'

        soap_session, response_post, error = post_web_service(soap_session, 'getTableQueryString', body_sub_elements)
        if not error:
            table_query_xml = BeautifulSoup(response_post.text, "xml")
            table_query = table_query_xml.text

            body_sub_elements = {}
            body_sub_elements['name'] = ln_table_or_derived_data_source
            body_sub_elements['profile'] = 'false'
            body_sub_elements['query'] = table_query

            soap_session, response_post, error = post_web_service(soap_session, 'getStagingTableData',
                                                                  body_sub_elements)
            if not error:
                all_data_xml = BeautifulSoup(response_post.text, "xml")
                all_rows = all_data_xml.find_all('ArrayOfString')

    return (soap_session, all_rows, error)


def admin_service_GetRepositoriesToCompare(soap_session, space_name, space_id):
    error = ''
    repositories = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'getRepositoriesToCompare',
                                                              body_sub_elements)
        if not error:
            repositories_xml = BeautifulSoup(response_post.text, "xml")
            repositories = repositories_xml.find_all('getRepositoriesToCompareResult')

    return (soap_session, repositories, error)


def admin_service_compareToRepository(soap_session, base_space_name, base_space_id, compare_space, compare_space_id,
                                      object_types):
    error = ''
    differences = ''
    compare_path = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = base_space_id
    body_sub_elements['spaceName'] = base_space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)

    if not error:

        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'getRepositoriesToCompare',
                                                              body_sub_elements)
        if not error:
            repositories_xml = BeautifulSoup(response_post.text, "xml")
            repositories = repositories_xml.find_all('getRepositoriesToCompareResult')

            compare_path = ''
            # get path to repository current space (= base space)
            respository_dict = xmltodict.parse(repositories[0].encode(encoding='utf-8'))
            for path in respository_dict['getRepositoriesToCompareResult']['Paths']['string']:
                if path.startswith(compare_space_id):
                    compare_path = path

            if compare_path:
                body_sub_elements = {}
                body_sub_elements['path'] = compare_path  # r"7ceaff98-bdda-43b9-8df3-0e441442f892\repository_dev.xml"
                body_sub_elements['objecttypes'] = {}
                body_sub_elements['objecttypes']['string'] = object_types
                body_sub_elements['spaceId'] = compare_space_id
                soap_session, response_post, error = post_web_service(soap_session, 'compareToRepository',
                                                                      body_sub_elements)
                if not error:
                    repositories_xml = BeautifulSoup(response_post.text, "xml")
                    differences = repositories_xml.find_all('compareToRepositoryResponse')
            else:
                error = 'path not for compare: ' + compare_space

            # body_sub_elements['path'] = '7ceaff98-bdda-43b9-8df3-0e441442f892\\.xml'

    return (soap_session, differences, error)


def admin_service_GetLevelTableData(soap_session, space_name, space_id, dimension):
    error = ''
    all_rows = []

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        body_sub_elements['name'] = dimension
        body_sub_elements['type'] = 'level'

        soap_session, response_post, error = post_web_service(soap_session, 'getTableQueryString', body_sub_elements)
        if not error:
            table_query_xml = BeautifulSoup(response_post.text, "xml")
            table_query = table_query_xml.text

            body_sub_elements = {}
            body_sub_elements['name'] = dimension
            body_sub_elements['profile'] = 'false'
            body_sub_elements['query'] = table_query

            soap_session, response_post, error = post_web_service(soap_session, 'getLevelData', body_sub_elements)
            if not error:
                all_data_xml = BeautifulSoup(response_post.text, "xml")
                all_rows = all_data_xml.find_all('ArrayOfString')

    return (soap_session, all_rows, error)


def admin_service_GetHierarchies(soap_session, space_name, space_id):
    error = ''
    all_hierarchies = []

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'GetHierarchies', body_sub_elements)

        if not error:
            # get source data
            # assume als load groups are the same for all sources.
            all_data_xml = BeautifulSoup(response_post.text, "xml")
            all_hierarchies = all_data_xml.find_all('Hierarchy')

    return (soap_session, all_hierarchies, error)


def admin_service_GetSources(soap_session, space_name, space_id):
    sources_info = []

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'GetSources', body_sub_elements)

        if not error:
            # get source data
            # assume als load groups are the same for all sources.
            load_groups = response_post.text.partition('loadgroups=')[2].partition('|')[0].strip()
            sources_info.append(load_groups)

    return (soap_session, sources_info, error)


def admin_service_DeleteAll(soap_session, space_name, space_id):
    delete_all_result = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        soap_session, response_post, error = post_web_service(soap_session, 'DeleteAll', body_sub_elements)

        if not error:
            # get errormessage when present
            all_data_xml = BeautifulSoup(response_post.text, "xml")
            error_type = all_data_xml.find_all('ErrorType')
            if error_type[0].text != '0':
                error_message = all_data_xml.find_all('ErrorMessage')
                delete_all_result = error_message[0].text

    return (soap_session, delete_all_result, error)


def admin_service_saveSourceDataAndHierarchies(soap_session, space_name, space_id, data_source):
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        body_sub_elements = {}
        body_sub_elements['el'] = data_source
        body_sub_elements['hierarchiesThatChanged'] = ''
        result = post_web_service(soap_session, 'saveSourceDataAndHierarchies', body_sub_elements)
        soap_session = result[0]
        error = result[2]

    return (soap_session, error)


def admin_service_GetProcessInitData(soap_session, space_name, space_id, load_group):
    process_init_data = []
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        body_sub_elements['automode'] = 'false'
        body_sub_elements['loadGroup'] = load_group
        soap_session, response_post, error = post_web_service(soap_session, 'GetProcessInitData', body_sub_elements)
        if not error:
            # get errormessage when present
            all_data_xml = BeautifulSoup(response_post.text, "xml")
            current_state_xml = all_data_xml.find_all('CurrentState')
            current_state = current_state_xml[0].text
            process_init_data.append(current_state)

    return (soap_session, process_init_data, error)


def admin_service_executeCommand(soap_session, command):
    command_line_results = []
    error = ''

    body_sub_elements = {}
    body_sub_elements['command'] = command

    soap_session, response_post, error = post_web_service(soap_session, 'executeCommand', body_sub_elements)

    if not error:
        reponse_data_xml = BeautifulSoup(response_post.text, "xml")
        command_line_results = reponse_data_xml.find('executeCommandResult').text.strip().split('\n')

    return (soap_session, command_line_results, error)


def admin_service_removeDataSource(soap_session, space_name, space_id, source_name):
    body_sub_elements = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        body_sub_elements = {}
        body_sub_elements['name'] = 'ST_' + source_name

        result = post_web_service(soap_session, 'removeDataSource', body_sub_elements)
        soap_session = result[0]
        error = result[2]

    return (soap_session, error)


def admin_service_GetSourceData(soap_session, space_name, space_id, source_name):
    source_data = ''
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        body_sub_elements['stagingTableName'] = 'ST_' + source_name.replace(' ', '_')
        body_sub_elements['sourceFileName'] = source_name

        soap_session, response_post, error = post_web_service(soap_session, 'GetSourceData', body_sub_elements)
        if not error:
            source_data_start = response_post.text.partition('&lt;SourceData')[1] + \
                                response_post.text.partition('&lt;SourceData')[2]
            source_data = source_data_start.partition('</GetSourceDataResult>')[0]

    return (soap_session, source_data, error)


def admin_service_publish_data(soap_session, soap_client, login_token, proc_space, proc_space_id, array_of_string,
                               delete_all, execution_mode, logger):
    load_group = ''
    error = ''
    processing_time = 0

    proc_datetime_start = datetime.now()
    proc_diff_date_time = proc_datetime_start - proc_datetime_start

    logger.info('   retrieving load group ...')

    soap_session, sources_info, error = admin_service_GetSources(soap_session, proc_space, proc_space_id)
    if not error:
        load_group = sources_info[0]
        logger.info('      load group = ' + load_group)

        if delete_all:
            logger.info('   deleting all processed data ...')
            if execution_mode == get_execution_mode_update():
                soap_session, del_result_message, error = admin_service_DeleteAll(soap_session, proc_space,
                                                                                  proc_space_id)
                if error:
                    error = 'admin_service_DeleteAll() error: ' + error
                else:
                    if del_result_message:
                        error = 'Delete all failed: ' + del_result_message
        else:
            logger.info('   delete all: not done.')

        if not error:
            wait_datetime_start = datetime.now()
            wait_diff_date = wait_datetime_start - wait_datetime_start
            # check space is available for processing.
            while True:
                soap_session, process_init_data, error = admin_service_GetProcessInitData(soap_session, proc_space,
                                                                                          proc_space_id, load_group)
                if error:
                    error = 'admin_service_GetProcessInitData() error: ' + error

                if process_init_data[0] == 'Available' or execution_mode != get_execution_mode_update():  # CurrentState
                    break
                else:
                    time.sleep(0.5)
                    wait_datetime_end = datetime.now()
                    wait_diff_date = wait_datetime_end - wait_datetime_start
                    print('waiting space is available ' + str(int(wait_diff_date.seconds)) + ' seconds', end='\r',
                          flush=True)

            if int(wait_diff_date.seconds) > 0:
                logger.info('      waited space is available ' + str(int(wait_diff_date.seconds)) + ' seconds')

            logger.info('   space state = ' + process_init_data[0])

            if not error:
                load_date = proc_datetime_start.strftime('%Y-%m-%d')
                logger.info('   publishing ' + array_of_string.string[0] + ' ....')

                if execution_mode == get_execution_mode_update():
                    pub_datetime_start = datetime.now()
                    pub_diff_date = pub_datetime_start - pub_datetime_start
                    publish_token = soap_client.service.publishData(login_token, proc_space_id, array_of_string,
                                                                    load_date)
                    old_status_code = 'None'

                    while True:
                        try:
                            publish_complete = soap_client.service.isPublishingComplete(login_token, publish_token)
                        except Exception as e:
                            logger.info('soap_client.service.isPublishingComplete error: ' + e)

                        if publish_complete == True:
                            pub_datetime_end = datetime.now()
                            pub_diff_date = pub_datetime_end - pub_datetime_start
                            print('\n', flush=True)
                            logger.info('   publishing completed in ' + str(int(pub_diff_date.seconds)) + ' seconds')
                            break
                        else:
                            time.sleep(0.5)
                            try:
                                pub_status = soap_client.service.getJobStatus(login_token, publish_token)
                            except Exception as e:
                                logger.info('soap_client.service.getJobStatus error: ' + e)

                            pub_datetime_end = datetime.now()
                            pub_diff_date = pub_datetime_end - pub_datetime_start

                            if pub_status.statusCode == 'None':
                                print('publishing ' + str(int(pub_diff_date.seconds)) + ' seconds', end='\r',
                                      flush=True)
                            else:
                                new_line = ''
                                if pub_status.statusCode != old_status_code:
                                    new_line = '\n'

                                if pub_status.statusCode != 'Failed':
                                    print(new_line + 'publishing ' + str(int(
                                        pub_diff_date.seconds)) + ' seconds, status: ' + pub_status.statusCode + '                                     ',
                                          end='\r', flush=True)
                                else:
                                    print(new_line + 'publishing ' + str(int(
                                        pub_diff_date.seconds)) + ' seconds, status: Failed (' + pub_status.message + ')                             ',
                                          end='\r', flush=True)
                                    error = 'publishing failed: ' + pub_status.message  # error message

                            old_status_code = pub_status.statusCode
                else:
                    logger.info('      publishing completed')

    proc_datetime_end = datetime.now()
    proc_diff_date_time = proc_datetime_end - proc_datetime_start

    processing_time = proc_diff_date_time.seconds

    return soap_session, error, processing_time


def admin_get_all_saved_expressions(soap_session, subject_area, space_id, space_name):
    saved_expr_info = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(admin_service_get_base_url() + '/SMIWeb/rest/v1/subjectAreas/' + subject_area,
                                       headers=smi_headers)

        if response_get.status_code != 200:
            error = 'admin_get_all_saved_expressions failed for space: ' + space_name + ', reason: ' + response_get.reason + '. Possibly caused when not owner of space.'
        else:
            saved_expr_text = response_get.text
            nr_saved_expr = saved_expr_text.count('{"IsMeasureExpression":')

            while '{"IsMeasureExpression":' in saved_expr_text:
                r1_key = saved_expr_text.partition('{"IsMeasureExpression":')[2].partition('"rl":"')[2].partition('"')[
                    0].replace("\\", "").strip()
                saved_expression = saved_expr_text.partition('"savedExpression":"')[2].partition('"')[0].strip()
                saved_expr_info[r1_key] = saved_expression
                saved_expr_text = \
                saved_expr_text.partition('{"IsMeasureExpression":')[2].partition('"l":"')[2].partition('}')[2]

            if nr_saved_expr != len(saved_expr_info.keys()):
                error = 'not all saved expression found: ' + str(len(saved_expr_info.keys())) + ' of ' + str(
                    nr_saved_expr)

    return (soap_session, saved_expr_info, error)


def admin_get_dashboard_info(soap_session, space_name, space_id):
    dashboard_info = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(
            admin_service_get_base_url() + '/SMIWeb/rest/v1/dashboards?detailed=true&includePages=true&locale=en_GB',
            headers=smi_headers)
        dashboard_info = json.loads(response_get.text)

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(admin_service_get_base_url() + '/SMIWeb/rest/dashboards/prompt',
                                       headers=smi_headers)
        prompt_info = json.loads(response_get.text)

    return (soap_session, dashboard_info, prompt_info, error)


def admin_get_dashboard_dashlets(soap_session, space_name, space_id, collection_uuid, dashboard_uuid):
    dashlets_info = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(
            admin_service_get_base_url() + '/SMIWeb/rest/v1/dashboards/' + collection_uuid.strip() + '/pages/' + dashboard_uuid.strip() + '?locale=en_GB',
            headers=smi_headers)
        dashlets_info = json.loads(response_get.text)

    return (soap_session, dashlets_info, error)


def admin_get_conditions_info(soap_session, space_name, space_id):
    conditions_info = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(admin_service_get_base_url() + '/SMIWeb/rest/v1/conditions', headers=smi_headers)
        conditions_info = json.loads(response_get.text)

    return (soap_session, conditions_info, error)


def admin_get_modeler_connections(soap_session, space_name, space_id):
    connections_info = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(
            admin_service_get_base_url() + '/ConnectorController/rest/v1/connectors/connections', headers=smi_headers)
        connections_info = json.loads(response_get.text)

    return (soap_session, connections_info, error)


def admin_get_modeler_saved_objects(soap_session, connection_id, space_name, space_id):
    saved_objects = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        response_get = smi_session.get(
            admin_service_get_base_url() + '/ConnectorController/rest/v1/connectors/spaces/' + space_id.strip() + '/connections/' + connection_id.strip() + '/savedObjects',
            headers=smi_headers)
        saved_objects = json.loads(response_get.text)

    return (soap_session, saved_objects, error)


def admin_set_modeler_saved_object(soap_session, connection_id, space_name, space_id, new_saved_object):
    result = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    if not result[2]:
        smi_session = admin_get_session(soap_session)
        smi_session.get(admin_service_get_base_url() + '/apps/')

        object_name = new_saved_object['name']
        new_saved_object_json = json.dumps(new_saved_object)
        smi_headers = {}
        smi_headers[ADMIN_XSRF_TOKEN_KEY] = smi_session.cookies['XSRF-TOKEN']
        smi_headers[ADMIN_CONTENT_TYPE_KEY] = 'application/json;charset=UTF-8'
        session_url = admin_service_get_base_url() + '/ConnectorController/rest/v1/connectors/spaces/' + space_id.strip() + '/connections/' + connection_id.strip() + '/extractionObjects/' + object_name.strip() + '?connectionString=connector:birst://database:1.0'
        response_put = smi_session.put(session_url, data=new_saved_object_json, headers=smi_headers)
        result = json.loads(response_put.text)

        if 'success' not in result.keys():
            error = 'Update object ' + object_name + ' failed, response post text = ' + response_put.text

    return (soap_session, result, error)


def admin_update_staging_column_data(staging_column, element, new_value):
    old_staging_column = staging_column
    new_staging_column = staging_column

    element_start = '&lt;' + element.strip() + '&gt;'
    element_end = '&lt;/' + element.strip() + '&gt;'

    new_staging_column_start = old_staging_column.partition(element_start)[0] + \
                               old_staging_column.partition(element_start)[1] + new_value.strip()
    new_staging_column = new_staging_column_start + old_staging_column.partition(element_end)[1] + \
                         old_staging_column.partition(element_end)[2]

    return (new_staging_column)


def admin_get_source_data_element_value(source_data, element, use_lt_gt=True):
    element_value = ''

    if use_lt_gt:
        element_start = '&lt;' + element.strip() + '&gt;'
        element_end = '&lt;/' + element.strip() + '&gt;'
    else:
        element_start = '<' + element.strip() + '>'
        element_end = '</' + element.strip() + '>'

    if element_start in source_data and element_end in source_data:
        element_value_start = source_data.partition(element_start)[2].strip()
        element_value = element_value_start.partition(element_end)[0].strip()

    return (element_value)


def admin_get_source_data_elements(source_data, element, use_lt_gt=True):
    elements = []

    remaining_source_data = source_data

    if use_lt_gt:
        element_start = '&lt;' + element.strip() + '&gt;'
        element_end = '&lt;/' + element.strip() + '&gt;'
    else:
        element_start = '<' + element.strip() + '>'
        element_end = '</' + element.strip() + '>'

    while element_start in remaining_source_data:
        staging_column_begin = remaining_source_data.partition(element_start)[1] + \
                               remaining_source_data.partition(element_start)[2]
        staging_column = staging_column_begin.partition(element_end)[0] + staging_column_begin.partition(element_end)[1]
        remaining_source_data = staging_column_begin.partition(element_end)[2]
        elements.append(staging_column)

    return (elements)


def admin_get_source_data_staging_columns_etl_hidden(source_data):
    staging_columns = []

    remaining_source_data = source_data

    while STAGING_COLUMN_START in remaining_source_data:
        staging_column_begin = remaining_source_data.partition(STAGING_COLUMN_START)[1] + \
                               remaining_source_data.partition(STAGING_COLUMN_START)[2]
        staging_column = staging_column_begin.partition(STAGING_COLUMN_END)[0] + \
                         staging_column_begin.partition(STAGING_COLUMN_END)[1]
        remaining_source_data = staging_column_begin.partition(STAGING_COLUMN_END)[2]

        colume_name = admin_get_source_data_element_value(admin_get_source_data_elements(staging_column, 'Name')[0],
                                                          'Name')
        if colume_name.endswith(ADMIN_ETL_HIDDEN_SUFFIX):
            staging_columns.append(staging_column)

    return (staging_columns)


def admin_update_column_part(column_part, column_name, element, new_element_value):
    error = ''
    new_column_part = ''

    xml_element_start = '&lt;' + element + '&gt;'
    xml_element_end = '&lt;/' + element + '&gt;'

    if element in column_part:
        new_column_part_begin = column_part.partition(xml_element_start)[0] + column_part.partition(xml_element_start)[
            1]
        new_column_part_begin = new_column_part_begin + new_element_value + xml_element_end
        new_column_part = new_column_part_begin + column_part.partition(xml_element_end)[2]
    else:
        error = 'admin_update_column_part failed, reason: Column: ' + column_name.strip() + ',  has no element: ' + element

    return (new_column_part, error)


def admin_update_data_source_column(source_data, column_name, element, new_element_value):
    new_data_source = ''
    error = ''
    remaining_source_data = source_data
    element_found = False

    xml_name = '&lt;Name&gt;' + column_name.strip() + '&lt;/Name&gt;'

    while STAGING_COLUMN_START in remaining_source_data:
        prev_staging_column = remaining_source_data.partition(STAGING_COLUMN_START)[0]
        staging_column_begin = remaining_source_data.partition(STAGING_COLUMN_START)[1] + \
                               remaining_source_data.partition(STAGING_COLUMN_START)[2]
        staging_column = staging_column_begin.partition(STAGING_COLUMN_END)[0] + \
                         staging_column_begin.partition(STAGING_COLUMN_END)[1]
        remaining_source_data = staging_column_begin.partition(STAGING_COLUMN_END)[2]

        if xml_name in staging_column:
            element_found = True
            staging_column = admin_update_staging_column_data(staging_column, element, new_element_value)
            new_data_source = new_data_source + prev_staging_column + staging_column + remaining_source_data
            remaining_source_data = ''
        else:
            new_data_source = new_data_source + prev_staging_column + staging_column

    if not element_found:
        error = 'admin_update_data_source_column failed, reason: Column: ' + column_name.strip() + ',  has no element: ' + element

    return (new_data_source, error)


def verify_get_special_column_info(special_column):
    # for special column digit the info is taken from ln field.
    # tje ln field is the part before the underscore

    expected_datatype = expected_phys_len = ''

    if special_column == 'kw' or special_column == 'deleted':
        expected_datatype = 'deleted'
        expected_phys_len = str(50)
    else:
        if special_column == 'compnr' or special_column == 'sequencenumber':
            expected_datatype = 'Integer'
            expected_phys_len = str(1)

    return expected_datatype, expected_phys_len


def verify_calc_birst_width(ln_data_type, ln_physical_length):
    varchar_dataypes = ['String', 'Multibyte', 'kw', 'deleted', 'digit']

    birst_width = ''
    if ln_physical_length == '###':
        ln_physical_length = '1000'

    if ln_data_type in varchar_dataypes:
        if ln_data_type == 'kw' or ln_data_type == 'deleted':
            birst_width = str(50)
        else:
            div_result = divmod(int(ln_physical_length), 50)
            if div_result[1] == 0:
                birst_width = ln_physical_length
            else:
                birst_width = str((div_result[0] + 1) * 50)
    else:
        birst_width = str(1)

    return birst_width


def verify_check_datatype_width(ln_data_type, ln_physical_length, birst_data_type, birst_width):
    verifiy_rule = {"Byte": "Integer",
                    "Integer": "Integer",
                    "Long": "Integer",
                    "Enumerated": "Integer",
                    "Set": "Integer",
                    "Text": "Integer",
                    "Float": "Float",
                    "Double": "Float",
                    "String": "Varchar",
                    "Multibyte": "Varchar",
                    "Date": "Date",
                    "UTC Date/T": "DateTime",
                    "sequencenumber": "Integer",
                    "kw": "Varchar",
                    "compnr": "Integer",
                    "deleted": "Varchar",
                    "digit": "Varchar"}

    expected_birst_width = verify_calc_birst_width(ln_data_type, ln_physical_length)

    return verifiy_rule[ln_data_type], expected_birst_width


def verify_update_date_type_width(force_update, staging_column, source_column, import_column, ln_table, table_defs,
                                  calc_defs, logger, verbose_output):
    error = ''
    is_calculated_field = False
    staging_column_birst_data_type = ''
    staging_column_birst_width = ''
    source_column_birst_data_type = ''
    source_column_birst_width = ''
    import_column_birst_data_type = ''
    import_column_birst_width = ''
    changes_made = []
    datatype_changes_made = 0
    width_changes = 0

    field_type = FIELD_TYPE_UNKNOWN
    ln_table_dot = ln_table + '.'
    ln_fields = list(filter(lambda x: x.startswith(ln_table_dot), table_defs.keys()))

    column_name = admin_get_source_data_element_value(staging_column, 'Name')
    is_ln_table_field, special_column = verify_source_column_valid(column_name, ln_table, ln_fields)

    if is_ln_table_field:
        staging_column_birst_data_type = admin_get_source_data_element_value(staging_column, 'DataType')
        staging_column_birst_width = admin_get_source_data_element_value(staging_column, 'Width')
        if source_column:
            source_column_birst_data_type = admin_get_source_data_element_value(source_column, 'DataType')
            source_column_birst_width = admin_get_source_data_element_value(source_column, 'Width')
        if import_column:
            import_column_birst_data_type = admin_get_source_data_element_value(import_column, 'DataType')
            import_column_birst_width = admin_get_source_data_element_value(import_column, 'Width')

        if not special_column or special_column == 'digit':
            # LN field or column with e.g. _1 at end
            if special_column == 'digit':
                ln_field = ln_table + '.' + column_name.partition('_')[0].strip()
            else:
                ln_field = ln_table + '.' + column_name

            field_table_def_info = verify_get_field_tabledef_info(table_defs[ln_field])
            expected_datatype = field_table_def_info[1]
            expected_phys_len = field_table_def_info[2]
            field_type = FIELD_TYPE_LN_FIELD
        else:
            expected_datatype, expected_phys_len = verify_get_special_column_info(special_column)
            field_type = FIELD_TYPE_LN_META_FIELD
    else:
        # Check if it is a calculated field
        table_field = ln_table + '.' + column_name
        if table_field in calc_defs.keys():
            field_type = FIELD_TYPE_CALC_FIELD
            is_calculated_field = True
            staging_column_birst_data_type = admin_get_source_data_element_value(staging_column, 'DataType')
            staging_column_birst_width = admin_get_source_data_element_value(staging_column, 'Width')
            if source_column:
                source_column_birst_data_type = admin_get_source_data_element_value(source_column, 'DataType')
                source_column_birst_width = admin_get_source_data_element_value(source_column, 'Width')

            if import_column:
                import_column_birst_data_type = admin_get_source_data_element_value(import_column, 'DataType')
                import_column_birst_width = admin_get_source_data_element_value(import_column, 'Width')

            expected_datatype = calc_defs[table_field][1].capitalize()  # Type
            expected_phys_len = calc_defs[table_field][2]  # Length
        else:
            field_type = FIELD_TYPE_UNKNOWN

    if column_name != 'Day ID':
        logger.info(' ')
        logger.info(field_type.ljust(16, ' ') + ' ' + column_name.ljust(16, ' ') + ':')
        logger.info('--------------------------------')

    if is_ln_table_field or is_calculated_field:
        # Check staging columns
        new_birst_data_type, new_birst_width = verify_check_datatype_width(expected_datatype, expected_phys_len,
                                                                           staging_column_birst_data_type,
                                                                           staging_column_birst_width)

        if new_birst_data_type != staging_column_birst_data_type or force_update:
            # update datatype
            staging_column, error = admin_update_column_part(staging_column, column_name, 'DataType',
                                                             new_birst_data_type)
            logger.info(
                '        updated Staging Column: DataType: ' + staging_column_birst_data_type + ' => ' + new_birst_data_type)
            datatype_changes_made += 1
        else:
            if verbose_output:
                logger.info('                Staging Column: DataType: ' + staging_column_birst_data_type)

        # update width
        if new_birst_width != staging_column_birst_width or force_update:
            staging_column, error = admin_update_column_part(staging_column, column_name, 'Width', new_birst_width)
            logger.info(
                '        updated Staging Column: Width: ' + staging_column_birst_width + ' => ' + new_birst_width)
            width_changes += 1
        else:
            if verbose_output:
                logger.info('                Staging Column: Width: ' + staging_column_birst_width)

        if not error and source_column:
            # Check source columns
            new_birst_data_type, new_birst_width = verify_check_datatype_width(expected_datatype, expected_phys_len,
                                                                               source_column_birst_data_type,
                                                                               source_column_birst_width)

            if new_birst_data_type != source_column_birst_data_type or force_update:
                # update datatype
                source_column, error = admin_update_column_part(source_column, column_name, 'DataType',
                                                                new_birst_data_type)
                logger.info(
                    '        updated  Source Column: DataType: ' + source_column_birst_data_type + ' => ' + new_birst_data_type)
                datatype_changes_made += 1
            else:
                if verbose_output:
                    logger.info('                 Source Column: DataType: ' + source_column_birst_data_type)

            if new_birst_width != source_column_birst_width or force_update:
                # update width
                source_column, error = admin_update_column_part(source_column, column_name, 'Width', new_birst_width)
                logger.info(
                    '        updated  Source Column: Width: ' + source_column_birst_width + ' => ' + new_birst_width)
                width_changes += 1
            else:
                if verbose_output:
                    logger.info('                 Source Column: Width: ' + source_column_birst_width)

        if not error and import_column:
            # Check import columns
            new_birst_data_type, new_birst_width = verify_check_datatype_width(expected_datatype, expected_phys_len,
                                                                               import_column_birst_data_type,
                                                                               import_column_birst_width)

            if new_birst_data_type != import_column_birst_data_type or force_update:
                # update datatype
                import_column, error = admin_update_column_part(import_column, column_name, 'DataType',
                                                                new_birst_data_type)
                logger.info(
                    '        updated  Import Column: DataType: ' + import_column_birst_data_type + ' => ' + new_birst_data_type)
                datatype_changes_made += 1
            else:
                if verbose_output:
                    logger.info('                 Import Column: DataType: ' + import_column_birst_data_type)

            if new_birst_width != import_column_birst_width or force_update:
                # update width
                import_column, error = admin_update_column_part(import_column, column_name, 'Width', new_birst_width)
                logger.info(
                    '        updated  Import Column: Width: ' + import_column_birst_width + ' => ' + new_birst_width)
                width_changes += 1
            else:
                if verbose_output:
                    logger.info('                 Import Column: Width: ' + import_column_birst_width)

    changes_made.append(datatype_changes_made)
    changes_made.append(width_changes)

    return (staging_column, source_column, import_column, error, changes_made, field_type)


def verify_update_target_type_measure(staging_column, source_column, import_column, logger, verbose_output):
    changes_made = 0

    # check Measure element is set in TargetTypes element.
    # if so, remove the element.
    if TARGET_TYPES_ELEMENT_START in staging_column and MEASURE_ELEMENT in staging_column:
        # remove Measure element in TargetTypes
        staging_column = staging_column.partition(MEASURE_ELEMENT)[0] + staging_column.partition(MEASURE_ELEMENT)[2]
        logger.info('        updated Staging Column: Measure: true => false')
        changes_made += 1
    else:
        if verbose_output:
            logger.info('                Staging Column: Measure: true => false')

    if source_column:
        if TARGET_TYPES_ELEMENT_START in source_column and MEASURE_ELEMENT in source_column:
            # remove Measure element in TargetTypes
            source_column = source_column.partition(MEASURE_ELEMENT)[0] + source_column.partition(MEASURE_ELEMENT)[2]
            logger.info('        updated  Source Column: Measure: true => false')
            changes_made += 1
        else:
            if verbose_output:
                logger.info('                 Source Column: Measure: false')

    if import_column:
        if TARGET_TYPES_ELEMENT_START in import_column and MEASURE_ELEMENT in import_column:
            # remove Measure element in TargetTypes
            import_column = import_column.partition(MEASURE_ELEMENT)[0] + import_column.partition(MEASURE_ELEMENT)[2]
            logger.info('        updated  Import Column: Measure: true => false')
            changes_made += 1
        else:
            if verbose_output:
                logger.info('                 Import Column: Measure: false')

    return (staging_column, source_column, import_column, changes_made)


def verify_update_lock_type(staging_column, source_column, import_column, logger, verbose_output):
    changes_made = 0

    # add the element Lock Type with true, when not present.
    if LOCK_TYPE_START not in staging_column:
        # add Lock Type element set to true after last element
        staging_column_begin = staging_column.partition(STAGING_COLUMN_END)[
                                   0] + '  ' + LOCK_TYPE_START + 'true' + LOCK_TYPE_END + '\r\n      '
        staging_column = staging_column_begin + staging_column.partition(STAGING_COLUMN_END)[1]
        logger.info('        updated Staging Column: LockType: false => true')
        changes_made += 1
    else:
        if verbose_output:
            logger.info('                Staging Column: LockType: true')

    prevent_update_element = '  ' + PREVENT_UPDATE_START + 'true' + PREVENT_UPDATE_END + '\r\n      '

    if source_column:
        if PREVENT_UPDATE_START not in source_column:
            source_column_begin = source_column.partition(SOURCE_COLUMN_END)[0] + prevent_update_element
            source_column = source_column_begin + source_column.partition(SOURCE_COLUMN_END)[1] + \
                            source_column.partition(SOURCE_COLUMN_END)[2]
            logger.info('        updated  Source Column: PreventUpdate: false => true')
            changes_made += 1
        else:
            if verbose_output:
                logger.info('                 Source Column: PreventUpdate: true')

    if import_column:
        if PREVENT_UPDATE_START not in import_column:
            import_column_begin = import_column.partition(SOURCE_COLUMN_END)[0] + prevent_update_element
            import_column = import_column_begin + import_column.partition(SOURCE_COLUMN_END)[1] + \
                            import_column.partition(SOURCE_COLUMN_END)[2]
            logger.info('        updated  Import Column: PreventUpdate: false => true')
            changes_made += 1
        else:
            if verbose_output:
                logger.info('                 Import Column: PreventUpdate: true')

    return (staging_column, source_column, import_column, changes_made)


def get_source_column(source_columns, column_name):
    column_data = ''

    xml_name = '&lt;Name&gt;' + column_name + '&lt;/Name&gt;'

    if xml_name in source_columns:
        source_columns_remaining = source_columns.partition(SOURCE_COLUMN_START)[1] + \
                                   source_columns.partition(SOURCE_COLUMN_START)[2]

        while source_columns_remaining:
            # get SourceColumn
            column_data = source_columns_remaining.partition(SOURCE_COLUMN_START)[1] + \
                          source_columns_remaining.partition(SOURCE_COLUMN_START)[2].partition(SOURCE_COLUMN_END)[0] + \
                          source_columns_remaining.partition(SOURCE_COLUMN_END)[1]

            source_columns_remaining = source_columns_remaining.partition(SOURCE_COLUMN_END)[2]
            if xml_name in column_data:
                break

    return (column_data)


def admin_get_source_grain_list(soap_session, space_name, space_id, source_name):
    error = ''
    grain_list = []

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)
    if not error:
        remaining_staging_data = source_data
        while STAGING_COLUMN_START in remaining_staging_data and not error:

            staging_column_begin = remaining_staging_data.partition(STAGING_COLUMN_START)[1] + \
                                   remaining_staging_data.partition(STAGING_COLUMN_START)[2]
            staging_column = staging_column_begin.partition(STAGING_COLUMN_END)[0] + \
                             staging_column_begin.partition(STAGING_COLUMN_END)[1]
            if admin_get_source_data_element_value(staging_column, 'NaturalKey') == 'true':
                grain_name = admin_get_source_data_element_value(staging_column, 'Name')
                if grain_name not in grain_list:
                    grain_list.append(grain_name)

            remaining_staging_data = staging_column_begin.partition(STAGING_COLUMN_END)[2]

    return (soap_session, error, grain_list)


def admin_get_source_grain_levels(soap_session, space_name, space_id, source_name):
    error = ''
    grain_levels = []

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)
    if not error:
        remaining_staging_data = source_data
        levels_begin = remaining_staging_data.partition(LEVELS_START)[1] + \
                       remaining_staging_data.partition(LEVELS_START)[2]
        levels_data = levels_begin.partition(LEVELS_END)[0] + levels_begin.partition(LEVELS_END)[1]

        while STRING_START in levels_data:
            string_element_data_begin = levels_data.partition(STRING_START)[1] + levels_data.partition(STRING_START)[2]
            string_element_data = string_element_data_begin.partition(STRING_END)[0] + \
                                  string_element_data_begin.partition(STRING_END)[1]
            string_element = admin_get_source_data_element_value(string_element_data, 'string')
            if string_element and string_element not in grain_levels:
                grain_levels.append(string_element)

            levels_data = string_element_data_begin.partition(STRING_END)[2]

    return (soap_session, error, grain_levels)


def admin_get_source_properties(soap_session, space_name, space_id, source_name):
    error = ''
    properties = []
    prop_do_not_proc = 'false'
    prop_transactional = 'false'
    prop_bulk_insert_delete = 'false'
    prop_do_not_detect = 'false'
    prop_snapshot_delete_keys = []
    prop_processing_group = ''

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)
    if not error:

        # source property: Do not process when used by another script
        if DONOTPROCESS_START in source_data:
            prop_do_not_proc = source_data.partition(DONOTPROCESS_START)[2].partition(DONOTPROCESS_END)[0].strip()

        # source property: Rows in data source are tranactions (vs.snapshot)
        if TRANSACTIONAL_START in source_data:
            prop_transactional = source_data.partition(TRANSACTIONAL_START)[2].partition(TRANSACTIONAL_END)[0].strip()

        # source property: Bulk insert and delete measure records
        if BULKINSERTDELETE_START in source_data:
            prop_bulk_insert_delete = source_data.partition(BULKINSERTDELETE_START)[2].partition(BULKINSERTDALETE_END)[
                0].strip()

        if SNAPSHOTDELETEKEY_START in source_data:
            snapshot_delete_keys = source_data.partition(SNAPSHOTDELETEKEY_START)[2].partition(SNAPSHOTDELETEKEY_END)[
                0].strip()
            while STRING_START in snapshot_delete_keys:
                snapshot_delete_key = snapshot_delete_keys.partition(STRING_START)[2].partition(STRING_END)[0].strip()
                prop_snapshot_delete_keys.append(snapshot_delete_key)
                snapshot_delete_keys = snapshot_delete_keys.partition(STRING_END)[2].strip()

        if DONOTDETECT_START in source_data:
            prop_do_not_detect = source_data.partition(DONOTDETECT_START)[2].partition(DONOTDETECT_END)[0].strip()

        if PROCESSING_GROUP_START in source_data:
            prop_processing_group = source_data.partition(PROCESSING_GROUP_START)[2].partition(PROCESSING_GROUP_END)[
                0].strip()

    properties.append(prop_do_not_proc)
    properties.append(prop_transactional)
    properties.append(prop_bulk_insert_delete)
    properties.append(prop_snapshot_delete_keys)
    properties.append(prop_do_not_detect)
    properties.append(prop_processing_group)

    return (soap_session, error, properties)


def admin_verify_update_source_column(soap_session, force_update, space_name, space_id, source_name, source_data,
                                      ln_table, table_defs, calc_defs, execution_mode, logger, verbose_output):
    error = ''
    change_list = []
    field_list = []
    column_source_data = {}
    nr_table_changes = 0
    nr_datatype_changes = 0
    nr_width_changes = 0
    nr_target_type_changes = 0
    nr_lock_type_changes = 0
    nr_ln_table_fields = 0
    nr_calculated_fields = 0
    nr_unknown_fields = 0
    nr_ln_meta_data_fields = 0
    unknown_fields = []

    # Source data is taken from the prod repo file.
    # Any updates made here, are saved in dev repo file.
    # A Process Data is required to synchonize both repo files again.
    if not source_data:
        soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)

    old_source_data = copy.deepcopy(source_data)
    new_source_data = ''

    if not error:
        remaining_staging_data = source_data
        source_columns = source_data.partition(SOURCE_FILE_START)[2].partition(COLUMNS_END)[0]
        import_columns = source_data.partition(IMPORT_COLUMNS_START)[2].partition(IMPORT_COLUMNS_END)[0]

        while STAGING_COLUMN_START in remaining_staging_data and not error:

            staging_column_begin = remaining_staging_data.partition(STAGING_COLUMN_START)[1] + \
                                   remaining_staging_data.partition(STAGING_COLUMN_START)[2]
            staging_column = staging_column_begin.partition(STAGING_COLUMN_END)[0] + \
                             staging_column_begin.partition(STAGING_COLUMN_END)[1]
            remaining_staging_data = staging_column_begin.partition(STAGING_COLUMN_END)[2]

            column_name = admin_get_source_data_element_value(staging_column, 'Name')
            source_file_column = admin_get_source_data_element_value(staging_column, 'SourceFileColumn')

            if source_file_column:
                source_column = get_source_column(source_columns, column_name)
                import_column = get_source_column(import_columns, column_name)
            else:
                source_column = ''
                import_column = ''

            old_source_data_list = []
            old_source_data_list.append(staging_column)
            old_source_data_list.append(source_column)
            old_source_data_list.append(import_column)
            column_source_data[column_name] = []
            column_source_data[column_name].append(old_source_data_list)

            # Check Staging Column, Source Column, Import Source Column
            # - Check Data Type and Width
            # - Remove Target Type Measure.
            # - Add Lock Type
            # - field_type = LN Table Field, Calculated Field or Unknown
            new_staging_column, new_source_column, new_import_column, error, changes_made, field_type = verify_update_date_type_width(
                force_update, staging_column, source_column, import_column, ln_table, table_defs, calc_defs, logger,
                verbose_output)

            if field_type != FIELD_TYPE_UNKNOWN:
                if field_type == FIELD_TYPE_LN_FIELD:
                    nr_ln_table_fields += 1
                elif field_type == FIELD_TYPE_LN_META_FIELD:
                    nr_ln_meta_data_fields += 1
                else:
                    nr_calculated_fields += 1

                nr_datatype_changes += changes_made[0]  # changed data type
                nr_width_changes += changes_made[1]  # changed width

                if not error:
                    new_staging_column, new_source_column, new_import_column, changes_made = verify_update_target_type_measure(
                        new_staging_column, new_source_column, new_import_column, logger, verbose_output)
                    nr_target_type_changes += changes_made

                if not error:
                    new_staging_column, new_source_column, new_import_column, changes_made = verify_update_lock_type(
                        new_staging_column, new_source_column, new_import_column, logger, verbose_output)
                    nr_lock_type_changes += changes_made

                new_source_data_list = []
                new_source_data_list.append(new_staging_column)
                new_source_data_list.append(new_source_column)
                new_source_data_list.append(new_import_column)
                column_source_data[column_name].append(new_source_data_list)

            else:
                # field type not ln table and not calculated field
                if column_name != 'Day ID':
                    nr_unknown_fields += 1
                    unknown_fields.append(column_name)

                new_source_data_list = []
                new_source_data_list.append(staging_column)
                new_source_data_list.append(source_column)
                new_source_data_list.append(import_column)
                column_source_data[column_name].append(new_source_data_list)

            if error:
                logger.info(field_type.ljust(16, ' ') + column_name + ' error: ' + error)

        if not error:
            # update source_data

            # go to StagingTable element
            new_source_data = old_source_data.partition(STAGING_TABLE_START)[0]
            old_source_data_remaining = old_source_data.partition(STAGING_TABLE_START)[1] + \
                                        old_source_data.partition(STAGING_TABLE_START)[2]

            # go to Columns element in StagingTable element
            new_source_data = new_source_data + old_source_data_remaining.partition(COLUMNS_START)[0]
            old_source_data_remaining = old_source_data_remaining.partition(COLUMNS_START)[1] + \
                                        old_source_data_remaining.partition(COLUMNS_START)[2]

            # go to first StagingColumn element in Columns element
            new_source_data = new_source_data + old_source_data_remaining.partition(STAGING_COLUMN_START)[0]
            old_source_data_remaining = old_source_data_remaining.partition(STAGING_COLUMN_START)[1] + \
                                        old_source_data_remaining.partition(STAGING_COLUMN_START)[2]

            for column_name in (column_source_data.keys()):
                # get next staging column
                new_source_data = new_source_data + old_source_data_remaining.partition(STAGING_COLUMN_START)[0] + \
                                  column_source_data[column_name][1][0]
                old_source_data_remaining = old_source_data_remaining.partition(STAGING_COLUMN_END)[2]

            # go to sourceFile element
            new_source_data = new_source_data + old_source_data_remaining.partition(SOURCE_FILE_START)[0]
            old_source_data_remaining = old_source_data_remaining.partition(SOURCE_FILE_START)[1] + \
                                        old_source_data_remaining.partition(SOURCE_FILE_START)[2]

            # go to first Columns element in sourceFile element
            new_source_data = new_source_data + old_source_data_remaining.partition(COLUMNS_START)[0]
            source_file_columns = old_source_data_remaining.partition(COLUMNS_START)[1] + \
                                  old_source_data_remaining.partition(COLUMNS_START)[2].partition(COLUMNS_END)[0] + \
                                  old_source_data_remaining.partition(COLUMNS_START)[2].partition(COLUMNS_END)[1]
            old_source_data_remaining = old_source_data_remaining.partition(COLUMNS_END)[2]

            for column_name in (column_source_data.keys()):
                # replace old source column with new source column, when new is not empty
                if column_source_data[column_name][1][1]:
                    if column_source_data[column_name][0][1] != column_source_data[column_name][1][1]:
                        source_file_columns = source_file_columns.replace(column_source_data[column_name][0][1],
                                                                          column_source_data[column_name][1][1])

            new_source_data = new_source_data + source_file_columns

            # go to Import Columns element in sourceFile element
            new_source_data = new_source_data + old_source_data_remaining.partition(IMPORT_COLUMNS_START)[0]
            source_file_import_columns = old_source_data_remaining.partition(IMPORT_COLUMNS_START)[1] + \
                                         old_source_data_remaining.partition(IMPORT_COLUMNS_START)[2].partition(
                                             IMPORT_COLUMNS_END)[0] + \
                                         old_source_data_remaining.partition(IMPORT_COLUMNS_START)[2].partition(
                                             IMPORT_COLUMNS_END)[1]
            old_source_data_remaining = old_source_data_remaining.partition(IMPORT_COLUMNS_END)[2]

            for column_name in (column_source_data.keys()):
                # replace old import column with new import column, when new is not empty
                if column_source_data[column_name][1][2]:
                    old_data = column_source_data[column_name][0][2]
                    new_data = column_source_data[column_name][1][2]
                    if old_data != new_data:
                        source_file_import_columns = source_file_import_columns.replace(old_data, new_data)

            new_source_data = new_source_data + source_file_import_columns
            new_source_data = new_source_data + old_source_data_remaining

            if execution_mode == EXECUTIONMODE_UPDATE:
                soap_session, error = admin_service_saveSourceDataAndHierarchies(soap_session, space_name, space_id,
                                                                                 new_source_data)

    nr_table_changes = nr_datatype_changes + nr_width_changes + nr_target_type_changes + nr_lock_type_changes
    change_list.append(nr_table_changes)
    change_list.append(nr_datatype_changes)
    change_list.append(nr_width_changes)
    change_list.append(nr_target_type_changes)
    change_list.append(nr_lock_type_changes)
    field_list.append(nr_ln_table_fields)
    field_list.append(nr_calculated_fields)
    field_list.append(nr_unknown_fields)
    field_list.append(nr_ln_meta_data_fields)
    field_list.append(unknown_fields)

    return (soap_session, error, change_list, field_list, source_data, new_source_data)


def admin_update_data_source_add_secfilter(source_data, column_name):
    new_data_source = ''
    error = ''
    remaining_source_data = source_data
    element_found = False
    # the security filter is added after the </TargetAggregations> element of the StagingColumn
    xml_target_aggregations_end = '&lt;/TargetAggregations&gt;'
    xml_name = '&lt;Name&gt;' + column_name.strip() + '&lt;/Name&gt;'

    while STAGING_COLUMN_START in remaining_source_data:
        prev_staging_column = remaining_source_data.partition(STAGING_COLUMN_START)[0]
        staging_column_begin = remaining_source_data.partition(STAGING_COLUMN_START)[1] + \
                               remaining_source_data.partition(STAGING_COLUMN_START)[2]
        staging_column = staging_column_begin.partition(STAGING_COLUMN_END)[0] + \
                         staging_column_begin.partition(STAGING_COLUMN_END)[1]
        remaining_source_data = staging_column_begin.partition(STAGING_COLUMN_END)[2]

        if xml_name in staging_column:
            element_found = True
            staging_column_begin = staging_column.partition(xml_target_aggregations_end)[0] + \
                                   staging_column.partition(xml_target_aggregations_end)[1]
            staging_column_begin = staging_column_begin + ADMIN_SEC_FILTER_TEMPLATE
            staging_column = staging_column_begin + staging_column.partition(xml_target_aggregations_end)[2]

            new_data_source = new_data_source + prev_staging_column + staging_column + remaining_source_data
            remaining_source_data = ''
        else:
            new_data_source = new_data_source + prev_staging_column + staging_column

    if not element_found:
        error = 'admin_update_data_source_add_secfilter failed, reason: Column: ' + column_name.strip() + ',  has no element: /TargetAggregations'

    return (new_data_source, error)


def admin_make_space_current(soap_session, space_name, space_id):
    body_sub_elements = {}
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    post_result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = post_result[0]
    error = post_result[2]

    return (soap_session, error)


def admin_get_staging_table(soap_session, space_name, space_id, source_name):
    body_sub_elements = {}
    table_data_list = []
    do_not_proc = ''
    do_not_detect = ''
    processing_groups = ''
    load_groups = ''
    error = ''

    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    soap_session, response_post, error = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    if not error:
        body_sub_elements = {}
        body_sub_elements['stagingTableName'] = 'ST_' + source_name.replace(' ', '_')
        body_sub_elements['sourceFileName'] = source_name

        soap_session, response_post, error = post_web_service(soap_session, 'GetStagingTable', body_sub_elements)
        if not error:
            do_not_proc = \
            response_post.text.partition('<' + DONOTPROCESS + '>')[2].partition('</' + DONOTPROCESS + '>')[0].strip()
            processing_groups = \
            response_post.text.partition('<' + PROCESSING_GROUP + '><string>')[2].partition('</string>')[0].strip()
            load_groups = response_post.text.partition('<' + LOAD_GROUP + '><string>')[2].partition('</string>')[
                0].strip()
            do_not_detect = \
            response_post.text.partition('<' + DONOTDETECT + '>')[2].partition('</' + DONOTDETECT + '>')[0].strip()
            table_data_list.append(do_not_proc)
            table_data_list.append(processing_groups)
            table_data_list.append(load_groups)
            table_data_list.append(do_not_detect)

    return (soap_session, error, table_data_list)


def admin_enable_source(soap_session, space_name, space_id, source_name, execution_mode):
    source_data = ''
    disabled_element_changed = False

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)

    if not error:
        # set Disbled element to false when true
        source_data_disable_start = source_data.partition('&lt;Disabled&gt;')[0] + \
                                    source_data.partition('&lt;Disabled&gt;')[1]
        if source_data.partition('&lt;Disabled&gt;')[2].startswith('true'):
            source_data_disable_end = source_data.partition('&lt;/Disabled&gt;')[1] + \
                                      source_data.partition('&lt;/Disabled&gt;')[2]
            source_data = source_data_disable_start + 'false' + source_data_disable_end
            if execution_mode == get_execution_mode_update():
                soap_session, error = admin_service_saveSourceDataAndHierarchies(soap_session, space_name, space_id,
                                                                                 source_data)
            disabled_element_changed = True

    return (soap_session, error, disabled_element_changed, source_data)


def admin_is_source_disabled(soap_session, space_name, space_id, source_name):
    source_data = ''
    source_disabled = False
    error = ''

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)

    if not error:
        if source_data.partition('&lt;Disabled&gt;')[2].startswith('true'):
            source_disabled = True

    return (soap_session, error, source_disabled)


def admin_set_source_property(soap_session, space_name, space_id, source_name, property_name, new_property_value,
                              in_sourceFile):
    # the property can be placed in the staging table part or in the source file part.
    # if argument in_source = True, the property is placed in the source file part, else the property is plaed in the staging table part.

    source_data = ''
    element_changed = False

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)

    if not error:
        # set propery element to false when true
        xml_start_element = '&lt;' + property_name + '&gt;'
        xml_end_element = '&lt;/' + property_name + '&gt;'
        xml_start_end_without_space_element = '&lt;' + property_name + '/&gt;'  # e.g. &lt;SubGroups/&gt;
        xml_start_end_with_space_element = '&lt;' + property_name + ' /&gt;'  # e.g. &lt;SubGroups /&gt;
        property_element = xml_start_element + new_property_value + xml_end_element

        # if property in SourceData, then assumed property = true
        # else property not in SourceData and assumed property is false
        if xml_start_element in source_data:
            # change property element in SourceData
            source_data_property_start = source_data.partition(xml_start_element)[0]
            source_data_property_end = source_data.partition(xml_end_element)[2]
            source_data = source_data_property_start + property_element + source_data_property_end
            element_changed = True
        else:
            if xml_start_end_with_space_element in source_data or xml_start_end_without_space_element in source_data:
                if xml_start_end_with_space_element in source_data:
                    xml_start_end_element = xml_start_end_with_space_element
                else:
                    xml_start_end_element = xml_start_end_without_space_element

                source_data_property_start = source_data.partition(xml_start_end_element)[0]
                source_data_property_end = source_data.partition(xml_start_end_element)[2]
                source_data = source_data_property_start + property_element + source_data_property_end
                element_changed = True
            else:
                # add property element to end of staging table part in SourceData
                if in_sourceFile:
                    source_data_property_start = source_data.partition(SOURCE_FILE_END)[0]
                    source_data_property_end = source_data.partition(SOURCE_FILE_END)[1] + \
                                               source_data.partition(SOURCE_FILE_END)[2]
                    source_data = source_data_property_start + property_element + source_data_property_end
                else:
                    source_data_property_start = source_data.partition(STAGING_TABLE_END)[0]
                    source_data_property_end = source_data.partition(STAGING_TABLE_END)[1] + \
                                               source_data.partition(STAGING_TABLE_END)[2]
                    source_data = source_data_property_start + property_element + source_data_property_end
                element_changed = True

        if element_changed:
            soap_session, error = admin_service_saveSourceDataAndHierarchies(soap_session, space_name, space_id,
                                                                             source_data)

    return (soap_session, error, element_changed)


def verify_source_column_valid(column_name, ln_table, ln_fields):
    special_column = ''
    is_valid = False
    ln_field = ln_table + '.' + column_name.strip()
    if ln_field in ln_fields:
        is_valid = True
    else:
        # E.g. sequencenumber, deleted, compnr, site_ref_compnr
        if column_name == 'sequencenumber':
            special_column = 'sequencenumber'
            is_valid = True
        else:
            if column_name == 'deleted':
                special_column = 'deleted'
                is_valid = True
            else:
                if column_name.endswith('compnr'):
                    special_column = 'compnr'
                    is_valid = True
                else:
                    # E.g. dsca_1, refe_kw
                    ln_field = ln_table + '.' + column_name[:4].strip()
                    if ln_field in ln_fields and column_name[4] == '_':
                        if column_name[5:].isdigit():
                            special_column = 'digit'
                            is_valid = True
                        else:
                            if column_name[5:] == 'kw':
                                special_column = 'kw'
                                is_valid = True

    return (is_valid, special_column)


def admin_get_source_security_info(soap_session, space_name, space_id, source_name):
    security_info = {}
    source_data = ''

    soap_session, source_data, error = admin_service_GetSourceData(soap_session, space_name, space_id, source_name)

    if not error:

        # get staging columns with SecFilter element
        staging_columns = admin_get_source_data_staging_columns_etl_hidden(source_data)

        # check the SecFilter
        for staging_column in staging_columns:
            column_info = []
            column_security_info = {}

            name_list = admin_get_source_data_elements(staging_column, 'Name')
            if name_list:
                column_name = admin_get_source_data_element_value(name_list[0], 'Name')
            else:
                error = 'admin_get_source_data_elements failed, reason: Column: ' + column_name.strip() + ',  has no element: ' + 'Name'
                break

            security_filter_list = admin_get_source_data_elements(staging_column, 'SecFilter')
            if security_filter_list:
                security_filter_type_list = admin_get_source_data_elements(security_filter_list[0], 'Type')
                if security_filter_type_list:
                    sec_filter_type = admin_get_source_data_element_value(security_filter_type_list[0], 'Type')
                    if sec_filter_type:
                        if sec_filter_type == '0':
                            column_security_info['Type'] = 'None / Variable'
                        else:
                            column_security_info['Type'] = 'Set-based Filter'
                    else:
                        column_security_info['Type'] = 'None / Variable'
                else:
                    error = 'admin_get_source_data_elements failed, reason: Column: ' + column_name.strip() + ',  has security filter without element: ' + 'Type'
                    break

                sec_filter_enabled_list = admin_get_source_data_elements(security_filter_list[0], 'Enabled')
                if sec_filter_enabled_list:
                    sec_filter_enabled = admin_get_source_data_element_value(sec_filter_enabled_list[0], 'Enabled')
                    if sec_filter_enabled:
                        column_security_info['Enabled'] = sec_filter_enabled.capitalize()
                    else:
                        column_security_info['Enabled'] = 'False'
                else:
                    error = 'admin_get_source_data_elements failed, reason: Column: ' + column_name.strip() + ',  has security filter without element: ' + 'Enabled'
                    break

                sec_filter_session_var_list = admin_get_source_data_elements(security_filter_list[0], 'SessionVariable')
                if sec_filter_session_var_list:
                    sec_filter_session_var = admin_get_source_data_element_value(sec_filter_session_var_list[0],
                                                                                 'SessionVariable')
                    if sec_filter_session_var:
                        column_security_info['SessionVariable'] = sec_filter_session_var
                    else:
                        column_security_info['SessionVariable'] = ''
                else:
                    column_security_info['SessionVariable'] = ''

                sec_filter_filter_groups_list = admin_get_source_data_elements(security_filter_list[0], 'FilterGroups')
                if sec_filter_filter_groups_list:
                    sec_filter_filter_groups = sec_filter_filter_groups_list[0]
                    if sec_filter_filter_groups:
                        groups = ''
                        filter_groups = admin_get_source_data_elements(sec_filter_filter_groups, 'string')

                        for filter_group in filter_groups:
                            if groups:
                                groups = groups + ', ' + admin_get_source_data_element_value(filter_group, 'string')
                            else:
                                groups = admin_get_source_data_element_value(filter_group, 'string')

                        column_security_info['FilterGroups'] = groups
                    else:
                        column_security_info['FilterGroups'] = ''
                else:
                    column_security_info['FilterGroups'] = ''

                sec_filter_support_outer_join_list = admin_get_source_data_elements(security_filter_list[0],
                                                                                    'addIsNullJoinCondition')
                if sec_filter_support_outer_join_list:
                    sec_filter_support_outer_join = admin_get_source_data_element_value(
                        sec_filter_support_outer_join_list[0], 'addIsNullJoinCondition')
                    if sec_filter_support_outer_join:
                        column_security_info['addIsNullJoinCondition'] = sec_filter_support_outer_join.capitalize()
                    else:
                        column_security_info['addIsNullJoinCondition'] = 'False'
                else:
                    column_security_info['addIsNullJoinCondition'] = 'False'
            else:
                # no SecFilter element
                column_security_info['Type'] = ''
                column_security_info['Enabled'] = ''
                column_security_info['SessionVariable'] = ''
                column_security_info['FilterGroups'] = ''
                column_security_info['addIsNullJoinCondition'] = ''

            column_info.append(column_security_info)
            # add old staging column
            column_info.append(staging_column)
            security_info[column_name] = column_info

    security_info = OrderedDict(sorted(security_info.items()))

    return (soap_session, security_info, source_data, error)


def admin_empty_data_source_list(soap_session, space_name, space_id, source_name_list, logger, email_content):
    body_sub_elements = {}
    body_sub_elements['spaceID'] = space_id
    body_sub_elements['spaceName'] = space_name

    result = post_web_service(soap_session, 'GetSpaceDetails', body_sub_elements)
    soap_session = result[0]
    error = result[2]
    if not error:
        for empty_num, source_name in enumerate(source_name_list, start=1):
            body_sub_elements = {}
            body_sub_elements['name'] = 'ST_' + source_name.replace(' ', '_')

            result = post_web_service(soap_session, 'EmptyDataSource', body_sub_elements)
            soap_session = result[0]
            if not result[2]:
                logger.info('   empty data source [' + str(empty_num) + ']: ' + source_name.strip())
                email_content = email_content + '   empty data source [' + str(
                    empty_num) + ']: ' + source_name.strip() + '\n'

    return (soap_session, email_content, error)


def admin_get_space_repoint_packages(copy_config, soap_session, array_of_spaces):
    # parent_repoint_packages = []
    repoint_packages = []

    for copy_from_space in copy_config[SPACE_COPY_FROM_SPACES]:
        space_name = copy_config[SPACE_COPY_FROM] + '_' + copy_from_space
        space_id = get_space_id(array_of_spaces, space_name)
        print('Get packages for space: ' + space_name)
        soap_session, package_infos_by_space, error = admin_service_GetSpacePackages(soap_session, space_name, space_id)

        if error:
            break
        else:
            for package_info in package_infos_by_space:
                package_space_name = package_info.find('SpaceName')
                if package_space_name:
                    package_space_name = package_space_name.text
                    package_space_name = package_space_name.replace(copy_config[SPACE_COPY_FROM], '').strip()
                    if package_space_name != copy_from_space:
                        repoint_packages.append(copy_from_space + ',' + package_space_name)

    return (soap_session, repoint_packages, error)


# ------------------
# other
def is_NoneType(object_to_check):
    is_None = False

    if object_to_check == None:
        is_None = True

    return (is_None)


def get_space_id(array_of_user_spaces, space_name):
    space_id = ''

    space_name = space_name.strip()
    for user_space in array_of_user_spaces:
        # take name[0:] and not name, otherwise compare will fail in case of long space names.
        if user_space.name[0:] == space_name:
            space_id = user_space.id
            break

    return space_id


def get_space_owner_id(array_of_user_spaces, from_space):
    owner = ''
    space_id = ''
    j = 0
    while j < len(array_of_user_spaces):
        if array_of_user_spaces[j].name == from_space:
            space_id = array_of_user_spaces[j].id
            owner = array_of_user_spaces[j].owner
            break
        else:
            j = j + 1

    return owner, space_id


def send_mail(subject, recipients, mail_content, mail_server, attachments, mail_sender, mail_password):
    sender = mail_sender
    mail_password = mail_password

    # Create the enclosing message
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['Subject'] = subject
    msg['To'] = recipients

    if mail_content:
        msg.attach(MIMEText(mail_content))

    for file in attachments:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(file))
        msg.attach(part)

    # Send the internal message via our own SMTP server.
    server = smtplib.SMTP(mail_server, 587)
    server.ehlo()
    server.starttls()
    server.login(sender, mail_password)
    server.sendmail(sender, list(recipients.split(",")), msg.as_string())
    server.quit()


def space_exists(space_to_find, all_spaces):
    space_found = False
    for space in all_spaces:
        # take name[0:] and not name, otherwise compare will fail in case of long space names.
        if space_to_find == space.name[0:]:
            space_found = True
            break

    return (space_found)


def verify_get_field_tabledef_info(field_def):
    field_desc = ''
    field_datatype = ''
    field_physical_length = ''

    fields_info = field_def.split(',')
    field_desc = fields_info[0].strip()
    field_datatype = fields_info[1].strip()
    field_physical_length = fields_info[2].strip()

    return (field_desc, field_datatype, field_physical_length)


def verify_get_ln_table_def_info(ln_table_def_files, logger):
    # returns dictionary with table definition information
    #   key: table          values: table description
    #   key: table.field    values: field description, field datatype, field physical length, type

    ln_table = ''
    ln_field = ''
    ln_table_description = ''
    ln_field_description = ''
    ln_data_type = ''
    ln_physical_length = ''
    field_def_info = ''
    ln_table_def_info = {}

    for num, ln_table_def_file in enumerate(ln_table_def_files, start=1):
        if logger:
            logger.info('Loading LN table definition file ' + str(num) + ': ' + ln_table_def_file)

        with open(ln_table_def_file) as f:
            ln_table_found = False
            for line in f:
                if not ln_table_found:
                    if line.startswith('Table     :'):
                        ln_table = line.partition(':')[2][0:10].strip()
                        ln_table_description = line.partition(':')[2][11:100].strip()
                        if ln_table not in ln_table_def_info.keys():
                            # logger.info('ln table = ' + ln_table_description)
                            ln_table_def_info[ln_table] = ln_table_description
                            ln_table_found = True
                else:
                    # retrieve all field and field names for the table
                    if 'Record Length: ' not in line:
                        if line[0:5].strip().isdigit():
                            ln_field = line[5:].partition('|')[0].strip()
                            ln_field_description = line[17:71].strip().replace(',', '')
                            ln_table_field = ln_table + '.' + ln_field
                            ln_data_type = line[129:139].strip()
                            ln_physical_length = line[143:148].strip()
                            # tfgld172.fdim with x for each dimension
                            if 'x' in ln_physical_length:
                                ln_physical_length = ln_physical_length.replace('x', '').strip()

                            field_def_info = ln_field_description + ',' + ln_data_type + ',' + ln_physical_length
                            if ln_table_field not in ln_table_def_info.keys():
                                ln_table_def_info[ln_table_field] = field_def_info
                    else:
                        ln_table_found = False

    ln_table_def_info = OrderedDict(sorted(ln_table_def_info.items()))

    return (ln_table_def_info)


def verify_get_calc_field_def_info(calc_field_def_files, logger):
    # returns dictionary with calculated field definition information
    #   key: table.calculated field   values: domain, type, length

    calc_field_def_info = {}
    line_sep = '|'

    for num, calc_field_def_file in enumerate(calc_field_def_files, start=1):
        if logger:
            logger.info('Loading calculated field definition file ' + str(num) + ': ' + calc_field_def_file)

        with open(calc_field_def_file) as f:
            for line in f:
                if line_sep in line and not line.startswith('Table/Field Name'):
                    table_field = line.partition(line_sep)[0].strip()
                    field_domain = line.partition(line_sep)[2].partition(line_sep)[0].strip()
                    field_type = line.partition(line_sep)[2].partition(line_sep)[2].partition(line_sep)[0].strip()
                    field_length = \
                    line.partition(line_sep)[2].partition(line_sep)[2].partition(line_sep)[2].partition(line_sep)[
                        0].strip()

                    if table_field not in calc_field_def_info.keys():
                        field_info = []
                        field_info.append(field_domain)
                        field_info.append(field_type)
                        field_info.append(field_length)
                        calc_field_def_info[table_field] = field_info

    calc_field_def_info = OrderedDict(sorted(calc_field_def_info.items()))

    return (calc_field_def_info)


def get_column_mappings_in_source_script(logger, column_mappings_by_source, mappings_not_found, source_script,
                                         source_columns, source_name):
    mappings = []
    not_found = []
    nr_mappings = 0
    nr_not_found = 0
    nr_not_mapped = 0
    nr_columns = 0

    for source_num, source_column in enumerate(source_columns):
        nr_columns += 1
        logger.info(source_name + '[' + str(source_num) + ']: ' + source_column.Name)
        if not source_column.Name.startswith('etl') and not source_column.Name.startswith('ims'):
            remainder = source_script
            mapping_found = False
            while remainder:
                line = remainder.partition('\n')[0]

                # filter out columns starting with etl or ims
                if not line.startswith('[etl') and not line.startswith('[ims'):
                    if source_column.Name.startswith('loc_'):
                        if line.lstrip().startswith('[' + source_column.Name.strip() + ']'):
                            if line.count('(') > line.count(')'):
                                multi_line = line.strip()
                                while not line.strip().endswith(')') and remainder:
                                    remainder = remainder.partition('\n')[2]
                                    line = remainder.partition('\n')[0]
                                    multi_line = multi_line + '\n' + line.strip()

                                remainder = ''
                                logger.info('    ' + multi_line)
                                mappings.append(multi_line)
                                mapping_found = True
                            else:
                                remainder = ''
                                logger.info('    ' + line.strip())
                                mappings.append(line.strip())
                                mapping_found = True
                    else:
                        if line.lstrip().startswith('[' + source_column.Name.strip() + ']'):
                            remainder = ''
                            logger.info('    ' + line.strip())
                            mappings.append(line.strip())
                            mapping_found = True

                if remainder:
                    remainder = remainder.partition('\n')[2]

            if not mapping_found:
                not_found.append(source_column.Name)
                nr_not_found += 1
                logger.info('         not found')

        else:
            nr_not_mapped += 1
            logger.info('         not mapped')

    if mappings:
        column_mappings_by_source[source_name] = mappings
        nr_mappings = len(mappings)

    if nr_not_found > 0:
        mappings_not_found[source_name] = not_found
        # error = 'get_column_mappings_in_source_script failed, reason: Not all mappings found in script, missing ' + str(nr_not_found) + ' mapping(s)'

    return (column_mappings_by_source, mappings_not_found, nr_mappings, nr_not_found)


def get_subject_area_content(soap_client, logger, login_token, csa_space_id, subject_area):
    # get the subject area content.
    logger.info('   ')
    logger.info('getting ' + subject_area + ' content ...')

    subject_area_content = soap_client.service.getExtendedSubjectArea(login_token, csa_space_id, subject_area)

    return (subject_area_content)


def create_new_space(soap_client, login_token, space_name, comments):
    # Temporary leave comments empty when creating a new space
    # space_id = soap_client.service.createNewSpace(login_token, space_name, comments, automatic=False )
    space_id = soap_client.service.createNewSpace(login_token, space_name, '', automatic=False)
    spaceProperties = soap_client.service.GetSpaceProperties(login_token, space_id)
    # Set flag for property: Convert Space to new metadata infrastructure
    spaceProperties.IsDocumentRepository = True
    soap_client.service.SetSpaceProperties(login_token, space_id, spaceProperties)

    return (space_id)


def update_copy_config(backup_mode, config_line, copy_config):
    if config_line.startswith('copy_url = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_url = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_URL not in copy_config.keys():
                copy_config[SPACE_COPY_URL] = config_url

    if config_line.startswith('copy_from = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_space_wildcard = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_FROM not in copy_config.keys():
                copy_config[SPACE_COPY_FROM] = config_space_wildcard

    if config_line.startswith('copy_from_spaces = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            if SPACE_COPY_FROM_SPACES in copy_config.keys():
                copy_from_spaces = copy_config[SPACE_COPY_FROM_SPACES]
            else:
                copy_from_spaces = []

            config_spaces = config_line.partition('=')[2].partition(',')[2].strip()
            for config_space in config_spaces.split(','):
                config_space = config_space.strip()
                if config_space not in copy_from_spaces:
                    copy_from_spaces.append(config_space)

            copy_config[SPACE_COPY_FROM_SPACES] = copy_from_spaces

    if config_line.startswith('copy_to = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            if SPACE_COPY_TO_MULTI in copy_config.keys():
                copy_multi_spaces = copy_config[SPACE_COPY_TO_MULTI]
            else:
                copy_multi_spaces = []

            config_spaces = config_line.partition('=')[2].partition(',')[2].strip()
            for config_space in config_spaces.split(','):
                config_space = config_space.strip()
                if config_space not in copy_multi_spaces:
                    copy_multi_spaces.append(config_space)

            copy_config[SPACE_COPY_TO_MULTI] = copy_multi_spaces

    if config_line.startswith('copy_date_time = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_date_time = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_DATE_TIME not in copy_config.keys():
                copy_config[SPACE_COPY_DATE_TIME] = config_date_time

    if config_line.startswith('copy_format = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_format_parts = config_line.partition('=')[2].partition(',')[2].strip()
            copy_formats = []
            for config_format_part in config_format_parts.split(','):
                config_format_part = config_format_part.strip()
                copy_formats.append(config_format_part)

            copy_config[SPACE_COPY_FORMAT] = copy_formats

    if config_line.startswith('copy_keep = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_keep = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_KEEP not in copy_config.keys():
                copy_config[SPACE_COPY_KEEP] = int(config_keep)

    if config_line.startswith('copy_create = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_create = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_CREATE not in copy_config.keys():
                copy_config[SPACE_COPY_CREATE] = int(config_create)

    if config_line.startswith('copy_mode = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_mode = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_MODE not in copy_config.keys():
                copy_config[SPACE_COPY_MODE] = config_mode

    if config_line.startswith('copy_to_suffix = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:
            config_suffix = config_line.partition('=')[2].partition(',')[2].strip()
            if SPACE_COPY_TO_SUFFIX not in copy_config.keys():
                copy_config[SPACE_COPY_TO_SUFFIX] = config_suffix

    if config_line.startswith('copy_options = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        config_spaces_options = config_line.partition('=')[2].partition(',')[2].strip()
        if config_backupmode == backup_mode:
            if SPACE_COPY_OPTIONS in copy_config.keys():
                copy_config_options = copy_config[SPACE_COPY_OPTIONS]
            else:
                copy_config_options = {}

            config_spaces = config_spaces_options.partition(':')[0].strip()
            config_options = config_spaces_options.partition(':')[2].strip()

            for config_space in config_spaces.split(','):
                config_space = config_space.strip()
                if config_space in copy_config_options.keys():
                    options = copy_config_options[config_space]
                else:
                    options = []

                for config_option in config_options.split(','):
                    config_option = config_option.strip()
                    if config_option not in options:
                        options.append(config_option)
                options.sort()

                copy_config_options[config_space] = options

            copy_config[SPACE_COPY_OPTIONS] = copy_config_options

    if config_line.startswith('repoint_packages = '):
        config_repoint_package = config_line.partition('=')[2].strip()

        if SPACE_REPOINT_PACKAGES in copy_config.keys():
            config_repoint_packages = copy_config[SPACE_REPOINT_PACKAGES]
        else:
            config_repoint_packages = []

        if config_repoint_package not in config_repoint_packages:
            config_repoint_packages.append(config_repoint_package)
            copy_config[SPACE_REPOINT_PACKAGES] = config_repoint_packages

    if config_line.startswith('add_user = '):
        config_backupmode = config_line.partition('=')[2].partition(',')[0].strip()
        if config_backupmode == backup_mode:

            if SPACE_ADD_USERS in copy_config.keys():
                config_add_users = copy_config[SPACE_ADD_USERS]
            else:
                config_add_users = {}

            config_add_user = config_line.partition('=')[2].partition(',')[2].partition(',')[0].strip()
            config_add_user_spaces = config_line.partition('=')[2].partition(',')[2].partition(',')[2].strip()

            if config_add_user not in config_add_users.keys():
                if config_add_user_spaces:
                    space_numbers = []
                    for space_number in config_add_user_spaces.split(','):
                        space_number = space_number.strip()
                        if space_number not in space_numbers:
                            space_numbers.append(space_number)

                    config_add_users[config_add_user] = space_numbers
                else:
                    config_add_users[config_add_user] = []

                copy_config[SPACE_ADD_USERS] = config_add_users

    return (copy_config)


def get_define_copy_mode_swap():
    return (SPACE_COPY_MODE_SWAP)


def is_valid_copy_mode(copy_mode):
    copy_mode_valid = False

    if copy_mode == SPACE_COPY_MODE_REPLICATE or copy_mode == SPACE_COPY_MODE_SWAP:
        copy_mode_valid = True

    return (copy_mode_valid)


def get_date_time_prefix(copy_config):
    date_time_prefix = ''
    if SPACE_COPY_DATE_TIME in copy_config[SPACE_COPY_FORMAT]:
        for format_part in copy_config[SPACE_COPY_FORMAT]:
            if format_part == SPACE_COPY_DATE_TIME:
                break
            else:
                if date_time_prefix:
                    date_time_prefix = date_time_prefix + ' ' + copy_config[format_part]
                else:
                    date_time_prefix = copy_config[format_part]

    return (date_time_prefix)


def get_copy_from_to_spaces(copy_config, current_date_time, array_of_spaces):
    date_time = ''
    space_date_times = []
    copy_from_to_space_names = {}
    error = ''

    copy_to_name_template = get_copy_to_template(copy_config)

    if SPACE_COPY_DATE_TIME in copy_to_name_template:
        if current_date_time:
            date_time = current_date_time.strftime(copy_config[SPACE_COPY_DATE_TIME])
        else:
            # determine newest date time
            space_date_times = get_space_date_times(copy_config, array_of_spaces)
            space_date_times.sort(reverse=True)
            if space_date_times:
                date_time = space_date_times[0]
            else:
                date_time_prefix = get_date_time_prefix(copy_config)
                error = 'get_copy_from_to_spaces failed, reason:  No date times for spaces starting with: ' + date_time_prefix

        copy_to_name_template = copy_to_name_template.replace(SPACE_COPY_DATE_TIME, date_time).strip()

    if not error:
        # create copy_frm and copy_to for each SPACE_COPY_FROM_SPACES
        for space in copy_config[SPACE_COPY_FROM_SPACES]:
            space = space.strip()

            copy_from_to_name = []

            if SPACE_COPY_FROM in copy_config.keys():
                copy_from_name = copy_config[SPACE_COPY_FROM] + '_' + space
            else:
                copy_from_name = ''
            # Have to change the copy to name should replace '' with '_'
            copy_to_name = copy_to_name_template.replace(SPACE_COPY_FROM_SPACES, space)

            copy_from_name = copy_from_name.strip()
            copy_to_name = copy_to_name.strip()

            copy_from_to_name.append(copy_from_name)
            copy_from_to_name.append(copy_to_name)
            copy_from_to_space_names[space] = copy_from_to_name

    return (copy_from_to_space_names, error)


def space_copy_format_with_date_time(copy_config):
    has_date_time = False

    if SPACE_COPY_DATE_TIME in copy_config[SPACE_COPY_FORMAT]:
        has_date_time = True

    return (has_date_time)


def get_copy_to_template(copy_config):
    # create copy_to template from SPACE_COPY_FORMAT
    copy_to_name_template = ''
    for format_part in copy_config[SPACE_COPY_FORMAT]:
        format_part = format_part.strip()

        next_copy_to_part = ''
        if format_part == SPACE_COPY_DATE_TIME:
            next_copy_to_part = SPACE_COPY_DATE_TIME
        else:
            if format_part == SPACE_COPY_FROM_SPACES:
                next_copy_to_part = SPACE_COPY_FROM_SPACES
            else:
                next_copy_to_part = copy_config[format_part]

        next_copy_to_part = next_copy_to_part.strip()
        # replace ' ' with '_'
        if copy_to_name_template:
            copy_to_name_template = copy_to_name_template + '_' + next_copy_to_part
        else:
            copy_to_name_template = next_copy_to_part

        copy_to_name_template.strip()

    return (copy_to_name_template)


def get_space_add_users(copy_config):
    add_users = []

    if SPACE_ADD_USERS in copy_config.keys():
        config_add_users = copy_config[SPACE_ADD_USERS]

        for config_add_user in config_add_users.keys():
            user_space_numbers = config_add_users[config_add_user]
            if user_space_numbers:
                to_space = get_space_copy_to(copy_config)
                space_to_multi = get_space_copy_to_multi(copy_config)
                space_num = space_to_multi.index(to_space) + 1
                if str(space_num) in user_space_numbers:
                    if config_add_user not in add_users:
                        add_users.append(config_add_user)
            else:
                if config_add_user not in add_users:
                    add_users.append(config_add_user)

    return (add_users)


def get_space_copy_create(copy_config):
    # create is done only, when filled with '1'
    # otherwise not filled
    copy_create = ''

    if SPACE_COPY_CREATE in copy_config.keys():
        if copy_config[SPACE_COPY_CREATE] == 1:
            copy_create = '1'

    return (copy_create)


def get_space_copy_keep(copy_config):
    # default copy keep, when not spcified n config file.
    copy_keep = 1

    if SPACE_COPY_KEEP in copy_config.keys():
        copy_keep = copy_config[SPACE_COPY_KEEP]

    return (copy_keep)


def get_space_copy_mode(copy_config):
    # default copy mode, when not specified in config file.
    copy_mode = SPACE_COPY_MODE_REPLICATE

    if SPACE_COPY_MODE in copy_config.keys():
        copy_mode = copy_config[SPACE_COPY_MODE]

    return (copy_mode)


def get_space_copy_url(copy_config):
    error = []
    urls = []
    if SPACE_COPY_URL in copy_config.keys():
        urls_list = copy_config[SPACE_COPY_URL].split(',')
        for url in urls_list:
            url = url.strip()
            urls.append(url)

            if not check_url_is_valid(url):
                # error is a list not a string have to change code here
                # error.append('tag: copy_url has invalid url ' + url)
                error = 'tag: copy_url has invalid url ' + url
    else:
        urls.append(ADMIN_BASE_URL)  # Default url when copy_url tag not defined with backup mode
        urls.append('')

    return (urls, error)


def get_space_copy_to(copy_config):
    copy_to = ''

    if SPACE_COPY_TO in copy_config.keys():
        copy_to = copy_config[SPACE_COPY_TO]

    return (copy_to)


def get_space_copy_to_multi(copy_config):
    copy_to_multi = []

    if SPACE_COPY_TO_MULTI in copy_config.keys():
        copy_to_multi = copy_config[SPACE_COPY_TO_MULTI]

    return (copy_to_multi)


def set_space_copy_to(copy_config, copy_to_space):
    copy_config[SPACE_COPY_TO] = copy_to_space.strip()

    return (copy_config)


def get_space_date_times(copy_config, array_of_spaces):
    space_date_times = []
    date_time_prefix = ''

    date_time_prefix = get_date_time_prefix(copy_config)
    spaces_with_prefix = [x for x in array_of_spaces if x.name.startswith(date_time_prefix)]
    date_time_len = len(datetime.now().strftime(copy_config[SPACE_COPY_DATE_TIME]))

    for space_with_prefix in spaces_with_prefix:
        space_date_time = space_with_prefix.name.replace(date_time_prefix, '').strip()[:date_time_len].strip()
        # check the date format is as specfied.
        try:
            datetime.strptime(space_date_time, copy_config[SPACE_COPY_DATE_TIME])
            if space_date_time not in space_date_times:
                space_date_times.append(space_date_time)
        except Exception:
            pass

    # sort oldest date_time to be first
    space_date_times.sort()

    return (space_date_times)


def is_space_available(soap_session, space_name, array_of_spaces):
    result = ''
    is_available = True

    if not space_exists(space_name, array_of_spaces):
        result = 'space ' + space_name + ' does not exist'
        is_available = False
    else:
        space_id = get_space_id(array_of_spaces, space_name)
        soap_session, space_state, result = admin_get_space_state(soap_session, space_name, space_id)
        if result:
            is_available = False
        else:
            if space_state != get_space_state_available():
                is_available = False

            result = 'space ' + space_name + ' has state: ' + space_state

    return (soap_session, is_available, result)


def check_spaces_available(soap_session, spaces_list, array_of_spaces, logger):
    all_spaces_available = True
    for space in spaces_list:
        space = space.strip()
        if space:
            soap_session, is_available, result = is_space_available(soap_session, space, array_of_spaces)

            if not is_available:
                all_spaces_available = False

            logger.info(result)

    return (soap_session, all_spaces_available)


def get_space_copy_options(copy_space, copy_config):
    # the copy options for the web service copySpace is a ';' separated string
    copy_options_string = ''
    copy_options_omitted_string = ''
    copy_options_remove = []
    copy_options_add = []

    all_copy_options_by_space = {}

    if SPACE_COPY_OPTIONS not in copy_config.keys():
        # default all copy options
        all_copy_options_by_space[copy_space] = copy.deepcopy(all_birst_copy_options)
        copy_config[SPACE_COPY_OPTIONS] = all_copy_options_by_space

    config_copy_options = copy_config[SPACE_COPY_OPTIONS]
    if copy_space in config_copy_options.keys():
        for option in config_copy_options[copy_space]:
            option = option.strip()
            if option.startswith('!'):
                # remove option
                if option[1:] not in copy_options_remove:
                    copy_options_remove.append(option[1:])
            else:
                if option not in copy_options_add:
                    copy_options_add.append(option)

        if copy_options_add:
            copy_options = copy_options_add
        else:
            copy_options = copy.deepcopy(all_birst_copy_options)

        if copy_options_remove:
            for option_remove in copy_options_remove:
                if option_remove in copy_options:
                    copy_options.remove(option_remove)
    else:
        # when TAG: copy_options not found, then all copy options
        copy_options = copy.deepcopy(all_birst_copy_options)

    copy_options.sort()
    copy_options_remove.sort()

    # create the ';' separated string for the copy options
    for copy_option in copy_options:
        if copy_options_string:
            copy_options_string = copy_options_string + ';' + copy_option.strip()
        else:
            copy_options_string = copy_option.strip()

    nr_omitted_options = 0
    # create the ';' separated string for the omitted copy options
    for birst_copy_option in all_birst_copy_options:
        if birst_copy_option.strip() not in copy_options:
            nr_omitted_options += 1
            if copy_options_omitted_string:
                copy_options_omitted_string = copy_options_omitted_string + ';' + birst_copy_option.strip()
            else:
                copy_options_omitted_string = birst_copy_option.strip()

    nr_copy_options = len(copy_options)
    return (nr_copy_options, copy_options_string, nr_omitted_options, copy_options_omitted_string)


def validate_copy_config_options(copy_config):
    error = ''
    invalid_options = []

    if SPACE_COPY_OPTIONS in copy_config.keys():
        config_copy_options = copy_config[SPACE_COPY_OPTIONS]
        if config_copy_options.values():
            for config_options in config_copy_options.values():
                for option in config_options:
                    option = option.strip()
                    if option.startswith('!'):
                        option = option[1:]

                    if option not in all_birst_copy_options:
                        if option not in invalid_options:
                            invalid_options.append(option)

        for invalid_option in invalid_options:
            if error:
                error = error + ', ' + invalid_option
            else:
                error = 'Invalid copy option(s): ' + invalid_option

        if not error:
            config_copy_from_spaces = copy_config[SPACE_COPY_FROM_SPACES]
            for config_options_space in config_copy_options.keys():
                if config_options_space not in config_copy_from_spaces:
                    if error:
                        error = error + ', ' + config_options_space
                    else:
                        error = 'copy options space(s) not a copy from space: ' + config_options_space

    return (error)
