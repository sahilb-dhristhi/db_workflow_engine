"""
MD5 Checksum utilities for data integrity verification
"""

import hashlib
import base64
from typing import Dict, List, Any, Optional


class MD5CheckSum:
    """Utility class for generating MD5 checksums in base-32 format."""
    
    @staticmethod
    def get_md5(str_code: str) -> str:
        """
        Generate MD5 hash of the input string and return it in base-32 uppercase format.
        
        Args:
            str_code: Input string to generate MD5 hash
            
        Returns:
            MD5 hash in base-32 uppercase format
        """
        try:
            md5_hash = hashlib.md5()
            md5_hash.update(str_code.encode('utf-8'))
            digest_bytes = md5_hash.digest()
            digest_int = int.from_bytes(digest_bytes, byteorder='big')
            digest_base32 = MD5CheckSum._to_base32(digest_int)
            return digest_base32.upper()
        except Exception as e:
            print(f"Error generating MD5: {e}")
            return ""
    
    @staticmethod
    def _to_base32(num: int) -> str:
        """
        Convert an integer to base-32 string representation.
        
        Args:
            num: Integer to convert
            
        Returns:
            Base-32 string representation
        """
        if num == 0:
            return "0"
        
        digits = "0123456789abcdefghijklmnopqrstuv"
        result = []
        
        while num > 0:
            result.append(digits[num % 32])
            num //= 32
        
        return ''.join(reversed(result))
    
    @staticmethod
    def compute_row_checksum(row: Dict[str, Any], columns_to_hash: List[str]) -> str:
        """
        Compute checksum for a database row using specified columns.
        
        Args:
            row: Dictionary representing a database row
            columns_to_hash: List of column names to include in checksum
            
        Returns:
            MD5 checksum string
        """
        concat_str = '||'.join([
            str(row[col]) if row[col] is not None else '' 
            for col in columns_to_hash
        ])
        return MD5CheckSum.get_md5(concat_str)


class MD5CheckSumAlternative:
    """Alternative MD5 checksum implementation using standard base-32 encoding."""
    
    @staticmethod
    def get_md5(str_code: str) -> str:
        """
        Generate MD5 hash of the input string and return it in standard base-32 format.
        
        Args:
            str_code: Input string to generate MD5 hash
            
        Returns:
            MD5 hash in base-32 uppercase format (RFC 4648)
        """
        try:
            md5_hash = hashlib.md5()
            md5_hash.update(str_code.encode('utf-8'))
            digest_bytes = md5_hash.digest()
            digest_base32 = base64.b32encode(digest_bytes).decode('utf-8')
            digest_base32 = digest_base32.rstrip('=')
            return digest_base32
        except Exception as e:
            print(f"Error generating MD5: {e}")
            return ""
    
    @staticmethod
    def compute_row_checksum(row: Dict[str, Any], columns_to_hash: List[str]) -> str:
        """
        Compute checksum for a database row using specified columns.
        
        Args:
            row: Dictionary representing a database row
            columns_to_hash: List of column names to include in checksum
            
        Returns:
            MD5 checksum string
        """
        concat_str = '||'.join([
            str(row[col]) if row[col] is not None else '' 
            for col in columns_to_hash
        ])
        return MD5CheckSumAlternative.get_md5(concat_str)