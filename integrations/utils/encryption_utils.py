# scripts/backend/integrations/utils/encryption_utils.py
import os
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad
import json

from dotenv import load_dotenv

load_dotenv() 

# It's good practice to define the key size and other constants
AES_KEY_SIZE = 32  # For AES-256
# IV size for CBC is typically the AES block size
IV_SIZE = AES.block_size # This is 16 bytes for AES

def get_encryption_key():
    key_hex = os.environ.get('ENCRYPTION_KEY')
    if not key_hex:
        raise ValueError("ENCRYPTION_KEY environment variable not set.")
    if len(key_hex) != AES_KEY_SIZE * 2: # Each byte is 2 hex chars
        raise ValueError(f"ENCRYPTION_KEY must be {AES_KEY_SIZE * 2} hex characters long for AES-{AES_KEY_SIZE*8}.")
    try:
        key = bytes.fromhex(key_hex)
    except ValueError as e:
        raise ValueError("ENCRYPTION_KEY is not a valid hex string.") from e
    return key

def encrypt(plain_text: str, key: bytes) -> str:
    iv = get_random_bytes(IV_SIZE)
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)

    padded_plain_text = pad(plain_text.encode('utf-8'), AES.block_size)
    ciphertext = cipher.encrypt(padded_plain_text)

    encrypted_payload = {
        'iv': b64encode(iv).decode('utf-8'),
        'ciphertext': b64encode(ciphertext).decode('utf-8')
    }
    return json.dumps(encrypted_payload)

def decrypt(encrypted_payload_str: str, key: bytes) -> str:
    try:
        encrypted_payload = json.loads(encrypted_payload_str)
        iv = b64decode(encrypted_payload['iv'])  # Decode IV from base64
        ciphertext = b64decode(encrypted_payload['ciphertext'])  # base64 string

        cipher = AES.new(key, AES.MODE_CBC, iv=iv)
        decrypted_padded_text = cipher.decrypt(ciphertext)
        decrypted_text = unpad(decrypted_padded_text, AES.block_size).decode('utf-8')
        return decrypted_text
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as e:
        print(f"Decryption failed: {e}")
        raise ValueError("Decryption failed. Payload may be malformed, key incorrect, or data corrupted.") from e



