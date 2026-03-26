"""Kalshi RSA-PSS authentication."""

import os
import re
import base64
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


class KalshiAuth:
    """Handles RSA-PSS signing for Kalshi API requests."""

    def __init__(self):
        self.key_id = os.environ.get('KALSHI_API_KEY_ID', '')
        raw_key = os.environ.get('KALSHI_PRIVATE_KEY', '')
        self._private_key = self._load_private_key(raw_key)

    def _load_private_key(self, raw_key):
        key_str = raw_key.strip().strip('"').strip("'")
        key_str = key_str.replace('\\n', '\n')
        key_str = key_str.replace('\\', '')

        match = re.search(r'-----BEGIN ([A-Z ]+)-----(.+?)-----END \1-----', key_str, re.DOTALL)
        if not match:
            raise ValueError("Could not find PEM header/footer in key")

        key_type = match.group(1)
        header = f"-----BEGIN {key_type}-----"
        footer = f"-----END {key_type}-----"
        base64_content = match.group(2)

        base64_clean = re.sub(r'[^A-Za-z0-9+/=]', '', base64_content)
        lines = [base64_clean[i:i+64] for i in range(0, len(base64_clean), 64)]
        pem_key = header + "\n" + "\n".join(lines) + "\n" + footer

        return serialization.load_pem_private_key(pem_key.encode('utf-8'), password=None)

    def get_headers(self, method, path):
        """Create auth headers with RSA-PSS signature."""
        timestamp = str(int(time.time() * 1000))
        path_without_query = path.split('?')[0]
        message = f"{timestamp}{method}{path_without_query}".encode('utf-8')
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return {
            'KALSHI-ACCESS-KEY': self.key_id,
            'KALSHI-ACCESS-TIMESTAMP': timestamp,
            'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode('utf-8'),
        }
