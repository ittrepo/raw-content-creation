import os
import json
import requests
import time
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import xmltodict

# Load environment variables
load_dotenv()

# Constants
HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_tracking_file.txt"
SUCCESS_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/successful_done_hotel_id_list.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_hotel_not_found.txt"
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/restel"


REQUEST_DELAY = 1




def get_supplier_own_raw_data(hotel_id):

        return None
    