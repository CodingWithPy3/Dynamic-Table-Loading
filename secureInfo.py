from cryptography.fernet import Fernet
import os


def getAtrributes():
    key = b'FVufWTNRRQjHkUlioXwVKa1YoBQbhYMprKXj1UWGZ4w='
    cipher_suite = Fernet(key)
    return cipher_suite


def encrypt_string(data):
    cipher_suite = getAtrributes()
    encrypted_data = cipher_suite.encrypt(data.encode('utf-8'))
    return encrypted_data


def save_to_file(file_path, data):
    try:
        with open(file_path, 'wb') as f:
            f.write(data)
        return True
    except:
        return False

def read_from_file(file_path):
    try:
        with open(file_path, 'rb') as f:
            return f.read()
    except:
        return False

def decrypt_string(encrypted_data):
    cipher_suite = getAtrributes()
    decrypted_data = cipher_suite.decrypt(encrypted_data).decode('utf-8')
    return decrypted_data
