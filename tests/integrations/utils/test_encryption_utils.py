import unittest
from unittest.mock import patch
import os
import json
from base64 import b64decode

# Ensure the utils module can be imported.
# This might need adjustment based on how the test runner handles paths.
# Assuming 'scripts.backend.integrations.utils' is discoverable.
from scripts.backend.integrations.utils.encryption_utils import (
    get_encryption_key,
    encrypt,
    decrypt,
    AES_KEY_SIZE, # For validating key length
    IV_SIZE # For validating IV in payload
)

class TestEncryptionUtils(unittest.TestCase):

    VALID_HEX_KEY_32_BYTES = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    VALID_KEY_BYTES = bytes.fromhex(VALID_HEX_KEY_32_BYTES)

    @patch.dict(os.environ, {"ENCRYPTION_KEY": VALID_HEX_KEY_32_BYTES})
    def test_get_encryption_key_success(self):
        key = get_encryption_key()
        self.assertEqual(key, self.VALID_KEY_BYTES)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_encryption_key_not_set(self):
        with self.assertRaisesRegex(ValueError, "ENCRYPTION_KEY environment variable not set."):
            get_encryption_key()

    @patch.dict(os.environ, {"ENCRYPTION_KEY": "00112233"}) # Too short
    def test_get_encryption_key_invalid_length(self):
        expected_msg = f"ENCRYPTION_KEY must be {AES_KEY_SIZE * 2} hex characters long for AES-{AES_KEY_SIZE*8}."
        with self.assertRaisesRegex(ValueError, expected_msg):
            get_encryption_key()

    @patch.dict(os.environ, {"ENCRYPTION_KEY": "not_a_hex_string" * 4}) # Correct length, but not hex
    def test_get_encryption_key_invalid_hex(self):
        with self.assertRaisesRegex(ValueError, "ENCRYPTION_KEY is not a valid hex string."):
            get_encryption_key()

    def test_encrypt_decrypt_success(self):
        plain_text = "This is a secret message!"
        encrypted_payload_str = encrypt(plain_text, self.VALID_KEY_BYTES)

        # Verify structure of encrypted_payload_str
        self.assertIsInstance(encrypted_payload_str, str)
        payload = json.loads(encrypted_payload_str)
        self.assertIn("iv", payload)
        self.assertIn("ciphertext", payload)

        # Verify IV is correct length (after base64 decoding)
        iv_bytes = b64decode(payload["iv"])
        self.assertEqual(len(iv_bytes), IV_SIZE) # IV_SIZE is AES.block_size (16 for AES)

        decrypted_text = decrypt(encrypted_payload_str, self.VALID_KEY_BYTES)
        self.assertEqual(decrypted_text, plain_text)

    def test_encrypt_decrypt_empty_string(self):
        plain_text = ""
        encrypted_payload_str = encrypt(plain_text, self.VALID_KEY_BYTES)
        decrypted_text = decrypt(encrypted_payload_str, self.VALID_KEY_BYTES)
        self.assertEqual(decrypted_text, plain_text)

    def test_decrypt_with_different_key(self):
        plain_text = "Another secret."
        encrypted_payload_str = encrypt(plain_text, self.VALID_KEY_BYTES)

        different_hex_key = "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"
        different_key_bytes = bytes.fromhex(different_hex_key)

        # Decryption with a different key should fail, typically due to padding errors or MAC check in GCM (though we use CBC)
        # For CBC with PKCS7 padding, an incorrect key will likely lead to a ValueError during unpadding.
        with self.assertRaisesRegex(ValueError, "Decryption failed. Payload may be malformed, key incorrect, or data corrupted."):
            decrypt(encrypted_payload_str, different_key_bytes)

    def test_decrypt_malformed_json_payload(self):
        malformed_payload_str = "this is not json"
        with self.assertRaisesRegex(ValueError, "Decryption failed."): # Broader message due to json.JSONDecodeError
            decrypt(malformed_payload_str, self.VALID_KEY_BYTES)

    def test_decrypt_payload_missing_iv(self):
        payload = {"ciphertext": "someciphertexthere"}
        malformed_payload_str = json.dumps(payload)
        with self.assertRaisesRegex(ValueError, "Decryption failed."): # Broader message due to KeyError
            decrypt(malformed_payload_str, self.VALID_KEY_BYTES)

    def test_decrypt_payload_missing_ciphertext(self):
        payload = {"iv": "someivhere"}
        malformed_payload_str = json.dumps(payload)
        with self.assertRaisesRegex(ValueError, "Decryption failed."): # Broader message due to KeyError
            decrypt(malformed_payload_str, self.VALID_KEY_BYTES)

    def test_decrypt_corrupted_ciphertext(self):
        plain_text = "Sensitive data"
        encrypted_payload_str = encrypt(plain_text, self.VALID_KEY_BYTES)
        payload = json.loads(encrypted_payload_str)

        # Corrupt ciphertext (e.g., change one character)
        corrupted_ciphertext = list(payload['ciphertext'])
        corrupted_ciphertext[0] = 'Z' if corrupted_ciphertext[0] != 'Z' else 'A'
        payload['ciphertext'] = "".join(corrupted_ciphertext)
        corrupted_payload_str = json.dumps(payload)

        with self.assertRaisesRegex(ValueError, "Decryption failed. Payload may be malformed, key incorrect, or data corrupted."):
            decrypt(corrupted_payload_str, self.VALID_KEY_BYTES)

    def test_decrypt_corrupted_iv(self):
        plain_text = "More sensitive data"
        encrypted_payload_str = encrypt(plain_text, self.VALID_KEY_BYTES)
        payload = json.loads(encrypted_payload_str)

        # Corrupt IV
        corrupted_iv = list(payload['iv'])
        corrupted_iv[0] = 'Z' if corrupted_iv[0] != 'Z' else 'A'
        payload['iv'] = "".join(corrupted_iv)
        corrupted_payload_str = json.dumps(payload)

        # Depending on the corruption, this might lead to unpadding error or garbage output.
        # The current decrypt function wraps this in a generic ValueError.
        with self.assertRaisesRegex(ValueError, "Decryption failed. Payload may be malformed, key incorrect, or data corrupted."):
            decrypt(corrupted_payload_str, self.VALID_KEY_BYTES)

if __name__ == "__main__":
    unittest.main()
