import requests
import os
import logging
from typing import Union, Dict, List, Optional

logger = logging.getLogger(__name__)

NEXTJS_API_BASE_URL = os.getenv('NEXTJS_API_BASE_URL', 'http://localhost:3001/api')

def make_api_request(method: str, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None, internal: bool = False) -> Optional[Union[Dict, List]]:
    """
    Helper function to make requests to the Next.js API.
    'internal' flag adds a special header to mark the request as internal.
    """
    url = f"{NEXTJS_API_BASE_URL}/{endpoint}"

    headers = {
        'Content-Type': 'application/json',
        # Add other default headers here if any
    }
    if internal:
        headers['X-Internal-Request'] = 'true'
        logger.debug(f"Internal request flag set for {method} {endpoint}")

    try:
        # Using requests.request for a unified way to handle methods and headers
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=data if method.upper() not in ['GET', 'DELETE'] else None, # Only include body for relevant methods
            params=params,
            timeout=10 # Standard timeout
        )

        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

        if response.status_code == 204: # No content for successful DELETE or some PUTs
            return None # Or an empty dict/True if preferred for no content

        return response.json()

    except requests.exceptions.HTTPError as http_err:
        # It's useful to log the response content that caused the HTTPError
        response_text = ""
        if http_err.response is not None:
            response_text = http_err.response.text
        logger.error(f"HTTP error occurred: {http_err} - Status: {http_err.response.status_code if http_err.response is not None else 'N/A'} - Response: {response_text}")
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An error occurred: {req_err}")
    except ValueError as json_err: # Includes JSONDecodeError
        logger.error(f"JSON decoding error: {json_err} - Response text: {response.text}")

    return None