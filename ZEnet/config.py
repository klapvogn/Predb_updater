import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# IRC Configuration
IRC_SERVER = "IRC NETWORK"
IRC_PORT = 6697
IRC_NICKNAME = "NICKNAME"
IRC_REALNAME = "REALNAME"
IRC_NICKSERV_PASSWORD = os.getenv("IRC_NICKSERV_PASSWORD")

# Channels
CHANNEL_MONITOR = "#CHANNEL"
CHANNEL_LOG = "#CHANNEL"

# Target announcer
ANNOUNCER_NICK = "ANNOUNCE_NICK"

# Database Configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME"),
    'autocommit': False
}