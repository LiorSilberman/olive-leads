import os
import sys

def resource_path(relative_path):
    """
    Constructs an absolute path to the resource. It supports both development and deployment environments.

    Args:
        relative_path (str): The relative path to the resource.

    Returns:
        str: The absolute path to the resource.
    """
    try:
        # If running as a PyInstaller bundle
        base_path = sys._MEIPASS
    except AttributeError:
        # If running as a script
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)