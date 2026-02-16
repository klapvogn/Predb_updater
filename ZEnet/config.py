import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# IRC Configuration
IRC_SERVER = "YOUR_IRC_SERVER_HERE"
IRC_PORT = 6697
IRC_NICKNAME = "YOUR_BOT_NICKNAME"
IRC_REALNAME = "SOMETHING"
IRC_NICKSERV_PASSWORD = os.getenv("IRC_NICKSERV_PASSWORD")

# Cert
IRC_CERT_FILE = '/path/to/your/cert.pem'

# Channels
CHANNEL_MONITOR = "#CHANNEL1"
CHANNEL_LOG = "#CHANNEL2"

# Target announcer
ANNOUNCER_NICK = "NICKNAME"

# Database Configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME"),
    'autocommit': False
}
