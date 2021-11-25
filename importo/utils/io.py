import hashlib
import io
import os
from urllib import parse

import requests
from django.contrib.staticfiles import finders


def fetch_file(url: str, add_hash=True) -> io.BytesIO:
    response = requests.get(url, verify=False, stream=not add_hash)
    response.raise_for_status()
    file = io.BytesIO(response.content)
    if add_hash:
        file.hash = hashlib.sha1(response.content).hexdigest()
    return file


def filename_from_url(url) -> str:
    """
    Gets the file name from a URL and cleans it up
    "https://example.com/my%20file.jpg?token=here" becomes "my file.jpg"
    """
    url_parsed = parse.urlparse(url)
    return parse.unquote_plus(os.path.split(url_parsed.path).pop())


def static_file_to_bytesio(file_path: str) -> io.BytesIO:
    """
    Find the file matching the provided `file_path` within the projects static files folder,
    reads the file content, and returns it as an in-memory file-like object.

    NOTE: Avoid using this for large files, as system memory is limited.
    """
    full_path = finders.find(file_path)
    if full_path is None:
        raise FileNotFoundError(f"'{file_path}' could not be found in static files.")
    with open(full_path, "rb") as f:
        value = io.BytesIO(f.read())
    return value
