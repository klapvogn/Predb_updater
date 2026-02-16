import irc.client
import irc.connection
import ssl
import re
import mysql.connector
import sys
import config
import time

class PredbBot(irc.client.SimpleIRCClient):
    def __init__(self):
        super().__init__()
        self.target_channel = config.CHANNEL_MONITOR
        self.log_channel = config.CHANNEL_LOG
        self.announcer_nick = config.ANNOUNCER_NICK
        self.db_config = config.DB_CONFIG
        self.nickserv_pass = config.IRC_NICKSERV_PASSWORD
        
        self.announce_pattern = re.compile(
            r'\(GENRE\)\s+\(([^)]+)\)\s+\((?:\x03\d{0,2})?([^)]+)\)'
        )
        self.color_code_pattern = re.compile(r'\x03\d{0,2}')
    
    def strip_color_codes(self, text):
        return self.color_code_pattern.sub('', text)
    
    def normalize_release_name(self, name):
        return name.replace('(', '').replace(')', '')
    
    def on_welcome(self, c, e):
        print(f"[+] Connected to {c.get_server_name()}")
        
        # Identify with NickServ if password provided
        if self.nickserv_pass:
            c.privmsg("NickServ", f"IDENTIFY {self.nickserv_pass}")
            print(f"[+] Sent NickServ IDENTIFY")
        
        c.join(self.target_channel)
        c.join(self.log_channel)
        print(f"[+] Joined {self.target_channel}")
        print(f"[+] Joined {self.log_channel}")
        print(f"[+] Monitoring announces from {self.announcer_nick}")
    
    def on_notice(self, c, e):
        # Handle NickServ responses
        source = e.source.nick if e.source else None
        message = e.arguments[0]
        
        if source and "nickserv" in source.lower():
            print(f"[NickServ] {message}")
            if "identified" in message.lower() or "recognized" in message.lower():
                print("[+] NickServ authentication successful")
    
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
            
        releasename_raw = match.group(1)
        genre_raw = match.group(2)
        genre = genre_raw.replace("/", "_")
        releasename = self.normalize_release_name(releasename_raw)
        
        msg = f"[\x0303+\x03] \x0303Found\x03: {releasename_raw}"
        print(f"    {msg}")
        self.connection.privmsg(self.log_channel, msg)
        
        msg = f"    \x0303Normalized\x03: {releasename}"
        print(f"    {msg}")
        self.connection.privmsg(self.log_channel, msg)
        
        msg = f"    \x0303Genre\x03: \x0304{genre_raw}\x03 -> \x0303{genre}\x03"
        print(f"    {msg}")
        self.connection.privmsg(self.log_channel, msg)
        time.sleep(5)  # Wait 5 seconds - adjust as needed
        
        self.update_database(releasename, genre)
    
    def update_database(self, releasename, genre):
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT id FROM releases WHERE releasename = %s", 
                (releasename,)
            )
            result = cursor.fetchone()
            
            if result:
                cursor.execute(
                    "UPDATE releases SET genre = %s WHERE releasename = %s",
                    (genre, releasename)
                )
                conn.commit()
                msg = f"\x0303[DB] Updated genre for\x03: {releasename} -> \x0303{genre}\x03"
                print(f"    {msg}")
                self.connection.privmsg(self.log_channel, msg)
            else:
                msg = f"[\x0304-\x03] Release not in database: \x0304{releasename}\x03"
                print(f"    {msg}")
                self.connection.privmsg(self.log_channel, msg)
            
            cursor.close()
            conn.close()
            
        except mysql.connector.Error as err:
            msg = f"\x0304[DB ERROR]\x03 {releasename}: {err}"
            print(f"    {msg}")
            self.connection.privmsg(self.log_channel, msg)

def main():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    
    bot = PredbBot()
    
    try:
        bot.connect(
            config.IRC_SERVER,
            config.IRC_PORT,
            config.IRC_NICKNAME,
            ircname=config.IRC_REALNAME,
            connect_factory=irc.connection.Factory(wrapper=ssl_context.wrap_socket)
        )
        print(f"[*] Connecting to {config.IRC_SERVER}:{config.IRC_PORT} with TLS...")
        bot.start()
        
    except irc.client.ServerConnectionError as e:
        print(f"[!] Connection failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[!] Shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()