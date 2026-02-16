import irc.client
import irc.connection
import ssl
import re
import os
import mysql.connector
import sys
import config
import time
import logging
from difflib import SequenceMatcher

# Configure logging - prevent duplicate handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear any existing handlers
logger.handlers.clear()

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# File handler ONLY
file_handler = logging.FileHandler(config.LOG_FILE)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Prevent propagation to root logger
logger.propagate = False

class PredbBot(irc.client.SimpleIRCClient):
    def __init__(self):
        super().__init__()
        self.target_channel = config.CHANNEL_MONITOR
        self.log_channel = config.CHANNEL_LOG
        self.announcer_nick = config.ANNOUNCER_NICK
        self.db_config = config.DB_CONFIG
        #self.nickserv_pass = config.IRC_NICKSERV_PASSWORD     
        
        self.announce_pattern = re.compile(
            r'\(GENRE\)\s+\(([^)]+)\)\s+\((?:\x03\d{0,2})?([^)]+)\)'
        )
        self.color_code_pattern = re.compile(r'\x03\d{0,2}')
    
    def strip_color_codes(self, text):
        return self.color_code_pattern.sub('', text)

    def similarity_ratio(self, s1, s2):
        """Calculate similarity ratio between two strings (0.0 to 1.0)"""
        return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()
    
    def find_matching_release(self, releasename_spam, genre):
        """
        Find the best matching release in database using fuzzy matching.
        Returns (matched_releasename, similarity_score) or (None, 0)
        """
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Get all releases that might match (filter by similar length)
            min_len = len(releasename_spam) - 10
            max_len = len(releasename_spam) + 10
            
            # First try: exact genre match OR no genre set
            cursor.execute(
                """SELECT releasename FROM releases 
                WHERE LENGTH(releasename) BETWEEN %s AND %s
                AND (genre IS NULL OR genre = '' OR genre = %s)
                ORDER BY unixtime DESC
                LIMIT 500""",
                (min_len, max_len, genre)
            )
            
            candidates = cursor.fetchall()
            
            # If no candidates with matching genre, try without genre filter
            if not candidates:
                cursor.execute(
                    """SELECT releasename FROM releases 
                    WHERE LENGTH(releasename) BETWEEN %s AND %s
                    ORDER BY unixtime DESC
                    LIMIT 500""",
                    (min_len, max_len)
                )
                candidates = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            if not candidates:
                return None, 0
            
            # Find best match using fuzzy matching
            best_match = None
            best_score = 0
            
            for (db_release,) in candidates:
                score = self.similarity_ratio(releasename_spam, db_release)
                if score > best_score:
                    best_score = score
                    best_match = db_release
            
            return best_match, best_score
            
        except mysql.connector.Error as err:
            msg = f"\x0304[DB ERROR]\x03 Finding match: {err}"
            logger.info(msg)
            self.connection.privmsg(self.log_channel, msg)
            return None, 0
               
    def on_welcome(self, c, e):
        logger.info(f"Connected to {c.get_server_name()}")

        # === Certificate Authentication Setup ===
        # Step 1: Uncomment the IDENTIFY block below and restart the bot
        # Step 2: After successful login, uncomment "CERT ADD" and restart again
        # Step 3: Verify with /whois <nickname> - you should see the certificate fingerprint
        # Step 4: Comment out both blocks and restart - cert auth should now work automatically
        # Step 5: Verify again with /whois <nickname> to confirm cert-only authentication

        # Step 1 & 4: Password authentication (disable after cert is registered)
        #if self.nickserv_pass:
        #    c.privmsg("NickServ", f"IDENTIFY {self.nickserv_pass}")
        #    logger.info("Sent NickServ IDENTIFY")

        # Step 2: Register certificate (enable only once, then disable)
        #c.privmsg("NickServ", "CERT ADD")
        #logger.info("Sent CERT ADD to NickServ")
        
        c.join(self.target_channel)
        c.join(self.log_channel)
        logger.info(f"Joined {self.target_channel}")
        logger.info(f"Joined {self.log_channel}")
        logger.info(f"Monitoring announces from {self.announcer_nick}")
    
    def on_notice(self, c, e):
        # Handle NickServ responses via NOTICE
        source = e.source.nick if hasattr(e.source, 'nick') else str(e.source) if e.source else "Unknown"
        message = e.arguments[0] if e.arguments else ""
        
        logger.info(f"[NOTICE from {source}] {message}")
        
        if source and "nickserv" in source.lower():
            logger.info(f"[NickServ] {message}")
            self.connection.privmsg(self.log_channel, f"[\x0303NickServ\x03] {message}")
            
            if any(keyword in message.lower() for keyword in [
                "identified", "recognized", "authenticated", "logged in"
            ]):
                logger.info("NickServ: Authentication confirmed")                
    
    def on_pubmsg(self, c, e):
        nick = e.source.nick
        message = e.arguments[0]
        
        if nick != self.announcer_nick:
            return
            
        self.process_announce(message)
    
    def process_announce(self, message):
        clean_message = self.strip_color_codes(message)
        
        match = self.announce_pattern.search(clean_message)
        if not match:
            return
            
        releasename_spam = match.group(1)
        genre_raw = match.group(2)
        genre = genre_raw.replace("/", "_")
        
        msg = f"[+] Found: {releasename_spam}"
        logger.info(msg)
        self.connection.privmsg(self.log_channel, f"[\x0303+\x03] \x0303Found\x03: {releasename_spam}")
        
        msg = f"    Genre: {genre_raw} -> {genre}"
        logger.info(msg)
        self.connection.privmsg(self.log_channel, f"    \x0303Genre\x03: \x0304{genre_raw}\x03 -> \x0303{genre}\x03")
        time.sleep(60)  # Wait 5 seconds - adjust as needed
        
        self.update_database_fuzzy(releasename_spam, genre)
    
    def update_database_fuzzy(self, releasename_spam, genre, max_attempts=5):
        """Update database using fuzzy matching with retry logic"""
        attempt = 0
        wait_times = [10, 20, 30, 40, 50]
        threshold = 0.85  # Similarity threshold (85% match required)
        
        while attempt < max_attempts:
            # Find best matching release
            matched_release, score = self.find_matching_release(releasename_spam, genre)
            
            if matched_release and score >= threshold:
                # Found a good match - update it
                try:
                    conn = mysql.connector.connect(**self.db_config)
                    cursor = conn.cursor()
                    
                    cursor.execute(
                        "UPDATE releases SET genre = %s WHERE releasename = %s",
                        (genre, matched_release)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                    msg = f"\x0303[DB] Updated\x03 (match: {score:.2%}): {matched_release} -> \x0303{genre}\x03"
                    logger.info(msg)
                    self.connection.privmsg(self.log_channel, msg)
                    
                    if score < 0.95:  # Show comparison if not exact match
                        msg = f"    \x0308Spam format\x03: {releasename_spam}"
                        logger.info(msg)
                        self.connection.privmsg(self.log_channel, msg)
                    
                    return  # Success
                    
                except mysql.connector.Error as err:
                    msg = f"\x0304[DB ERROR]\x03 {matched_release}: {err}"
                    logger.info(msg)
                    self.connection.privmsg(self.log_channel, msg)
                    return
            else:
                # No match found - wait and retry
                attempt += 1
                if attempt < max_attempts:
                    wait = wait_times[attempt - 1]
                    msg = f"[\x0308?\x03] No match found (best: {score:.2%}), retrying in {wait}s ({attempt}/{max_attempts}): \x0308{releasename_spam}\x03"
                    logger.info(msg)
                    self.connection.privmsg(self.log_channel, msg)
                    time.sleep(wait)
                else:
                    msg = f"[\x0304-\x03] No match after {max_attempts} attempts (best: {score:.2%}): \x0304{releasename_spam}\x03"
                    logger.info(msg)
                    self.connection.privmsg(self.log_channel, msg)
                    if matched_release:
                        msg = f"    \x0308Best candidate\x03: {matched_release}"
                        logger.info(msg)
                        self.connection.privmsg(self.log_channel, msg)

def main():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    
    # Load client certificate and private key
    try:
        # If key is in the same file as cert, keyfile can be omitted
        if hasattr(config, 'IRC_KEY_FILE') and config.IRC_KEY_FILE:
            ssl_context.load_cert_chain(
                certfile=config.IRC_CERT_FILE,
                keyfile=config.IRC_KEY_FILE
            )
        else:
            # Key and cert in same file
            ssl_context.load_cert_chain(certfile=config.IRC_CERT_FILE)
        
        logger.info("Loaded client certificate for authentication")
    except Exception as e:
        logger.error(f"Failed to load certificate: {e}")
        sys.exit(1)
    
    bot = PredbBot()
    
    try:
        bot.connect(
            config.IRC_SERVER,
            config.IRC_PORT,
            config.IRC_NICKNAME,
            ircname=config.IRC_REALNAME,
            connect_factory=irc.connection.Factory(wrapper=ssl_context.wrap_socket)
        )
        logger.info(f"Connecting to {config.IRC_SERVER}:{config.IRC_PORT} - Logged in as: {config.IRC_NICKNAME} with TLS and client cert...")
        bot.start()
        
    except irc.client.ServerConnectionError as e:
        logger.error(f"Connection failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()