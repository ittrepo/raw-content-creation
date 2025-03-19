import sys
import os

# Add the project root to sys.path. Adjust the path according to your structure.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from constant_file.for_database import PROVIDER_MAPPING_FOR_DB



print(PROVIDER_MAPPING_FOR_DB['dotw'])