#!/bin/bash
# predb.net Updater - Tmux Management Script
# Usage: ./manage_predb_updater.sh [start|stop|restart|status|attach|logs]

SCRIPT_DIR="$HOME/apps/predb.net"
LOG_DIR="$HOME/apps/predb.net/logs"
TMUX_SESSION="predb_club_updater"
PHP_SCRIPT="predb_club_updater.php"
LOG_FILE="$LOG_DIR/predb_club_updater.log"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if session exists
session_exists() {
    tmux has-session -t $TMUX_SESSION 2>/dev/null
    return $?
}

# Function to start the updater
start_updater() {
    if session_exists; then
        echo -e "${YELLOW}Session '$TMUX_SESSION' already exists.${NC}"
        echo "Use './manage_predb_net_updater.sh status' to check status"
        return 1
    fi
    
    echo -e "${GREEN}Starting PreDB.NET Updater...${NC}"
    
    # Create log directory if it doesn't exist
    mkdir -p "$LOG_DIR"
    
    # Create new tmux session in detached mode
    tmux new-session -d -s $TMUX_SESSION -c "$SCRIPT_DIR"
    
    # Rename the window
    tmux rename-window -t $TMUX_SESSION:0 'PreDBNETUpdater'
    
    # Start the updater
    tmux send-keys -t $TMUX_SESSION:0 "cd $SCRIPT_DIR" C-m
    tmux send-keys -t $TMUX_SESSION:0 "clear" C-m
    tmux send-keys -t $TMUX_SESSION:0 "echo '=== PreDB.NET Updater Started at $(date) ==='" C-m
    tmux send-keys -t $TMUX_SESSION:0 "php $PHP_SCRIPT" C-m
    
    sleep 2
    
    if session_exists; then
        echo -e "${GREEN}✓ PreDB.NET Updater started successfully${NC}"
        echo ""
        echo "Commands:"
        echo "  Attach to session:  ./manage_predb_net_updater.sh attach"
        echo "  View logs:         ./manage_predb_net_updater.sh logs"
        echo "  Check status:      ./manage_predb_net_updater.sh status"
        echo "  Stop updater:      ./manage_predb_net_updater.sh stop"
    else
        echo -e "${RED}✗ Failed to start updater${NC}"
        return 1
    fi
}

# Function to stop the updater
stop_updater() {
    if ! session_exists; then
        echo -e "${YELLOW}Session '$TMUX_SESSION' is not running.${NC}"
        return 1
    fi
    
    echo -e "${YELLOW}Stopping PreDB.NET Updater...${NC}"
    
    # Send Ctrl+C to stop gracefully
    tmux send-keys -t $TMUX_SESSION:0 C-c
    
    # Wait a bit for graceful shutdown
    sleep 3
    
    # Kill the session
    tmux kill-session -t $TMUX_SESSION 2>/dev/null
    
    if ! session_exists; then
        echo -e "${GREEN}✓ PreDB.NET Updater stopped${NC}"
    else
        echo -e "${RED}✗ Failed to stop updater${NC}"
        return 1
    fi
}

# Function to restart the updater
restart_updater() {
    echo -e "${YELLOW}Restarting PreDB.NET Updater...${NC}"
    stop_updater
    sleep 2
    start_updater
}

# Function to show status
show_status() {
    if session_exists; then
        echo -e "${GREEN}● PreDB.NET Updater is RUNNING${NC}"
        echo ""
        echo "Session: $TMUX_SESSION"
        echo "Script:  $PHP_SCRIPT"
        echo "Logs:    $LOG_FILE"
        echo ""
        
        # Show recent log entries
        if [ -f "$LOG_FILE" ]; then
            echo "Last 5 log entries:"
            echo "-------------------"
            tail -n 5 "$LOG_FILE"
        fi
        
        echo ""
        echo "To attach: ./manage_predb_net_updater.sh attach"
    else
        echo -e "${RED}○ PreDB.NET Updater is NOT RUNNING${NC}"
        echo ""
        echo "To start: ./manage_predb_net_updater.sh start"
    fi
}

# Function to attach to session
attach_session() {
    if ! session_exists; then
        echo -e "${RED}✗ Session '$TMUX_SESSION' is not running.${NC}"
        echo "Start it first with: ./manage_predb_net_updater.sh start"
        return 1
    fi
    
    echo "Attaching to session '$TMUX_SESSION'..."
    echo "Press Ctrl+b then d to detach"
    sleep 1
    tmux attach -t $TMUX_SESSION
}

# Function to show logs
show_logs() {
    if [ ! -f "$LOG_FILE" ]; then
        echo -e "${YELLOW}Log file not found: $LOG_FILE${NC}"
        return 1
    fi
    
    echo "Showing logs (press Ctrl+C to exit)..."
    echo ""
    tail -f "$LOG_FILE"
}

# Function to show help
show_help() {
    echo "PreDB.NET Updater - Tmux Management Script"
    echo ""
    echo "Usage: ./manage_predb_net_updater.sh [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start      Start the updater in a tmux session"
    echo "  stop       Stop the updater gracefully"
    echo "  restart    Restart the updater"
    echo "  status     Show current status"
    echo "  attach     Attach to the tmux session"
    echo "  logs       Show live logs (tail -f)"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./manage_predb_net_updater.sh start"
    echo "  ./manage_predb_net_updater.sh status"
    echo "  ./manage_predb_net_updater.sh logs"
    echo ""
    echo "Inside tmux session:"
    echo "  Ctrl+b d   Detach from session"
    echo "  Ctrl+c     Stop the updater"
}

# Main script logic
case "$1" in
    start)
        start_updater
        ;;
    stop)
        stop_updater
        ;;
    restart)
        restart_updater
        ;;
    status)
        show_status
        ;;
    attach)
        attach_session
        ;;
    logs)
        show_logs
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}Invalid command: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac

exit 0