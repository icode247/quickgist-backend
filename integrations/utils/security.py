"""
Security utilities for the blog automation system
"""

import hashlib
import secrets
from cryptography.fernet import Fernet
import base64
import os

class SecurityManager:
    def __init__(self):
        self.key = self._get_or_create_key()
        self.cipher = Fernet(self.key)
    
    def _get_or_create_key(self) -> bytes:
        """Get encryption key from environment or create new one"""
        key_env = os.getenv('ENCRYPTION_KEY')
        if key_env:
            return key_env.encode()
        else:
            # Generate new key (in production, store this securely!)
            return Fernet.generate_key()
    
    def encrypt_password(self, password: str) -> str:
        """Encrypt a password for secure storage"""
        encrypted = self.cipher.encrypt(password.encode())
        return base64.b64encode(encrypted).decode()
    
    def decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt a password for use"""
        encrypted_bytes = base64.b64decode(encrypted_password.encode())
        decrypted = self.cipher.decrypt(encrypted_bytes)
        return decrypted.decode()
    
    def hash_password(self, password: str, salt: str = None) -> tuple:
        """Hash a password with salt"""
        if not salt:
            salt = secrets.token_hex(16)
        
        password_hash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode(),
            salt.encode(),
            100000  # iterations
        )
        
        return base64.b64encode(password_hash).decode(), salt
    
    def verify_password(self, password: str, hashed_password: str, salt: str) -> bool:
        """Verify a password against its hash"""
        password_hash, _ = self.hash_password(password, salt)
        return password_hash == hashed_password
