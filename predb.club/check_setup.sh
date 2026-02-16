#!/bin/bash
# Diagnostic script to check PreDB.club updater setup

echo "=========================================="
echo "PreDB.club Updater - Setup Diagnostic"
echo "=========================================="
echo ""

SCRIPT_DIR="$HOME/apps/predb.club"
LOG_DIR="$HOME/apps/predb.club/logs"
TMUX_SESSION="predb_club_updater"

# Check current directory
echo "1. Current Directory:"
echo "   $(pwd)"
echo ""

# Check if script directory exists
echo "2. Script Directory ($SCRIPT_DIR):"
if [ -d "$SCRIPT_DIR" ]; then
    echo "   ✓ Directory exists"
    echo ""
    echo "   PHP files in this directory:"
    ls -lh "$SCRIPT_DIR"/*.php 2>/dev/null | awk '{print "     " $9 " (" $5 ")"}'
    
    if [ ! -f "$SCRIPT_DIR/predb_club_updater.php" ]; then
        echo ""
        echo "   ✗ predb_club_updater.php NOT FOUND"
        echo ""
        echo "   ACTION REQUIRED:"
        echo "   Please upload predb_club_updater.php to: $SCRIPT_DIR"
    else
        echo ""
        echo "   ✓ predb_club_updater.php found"
    fi
else
    echo "   ✗ Directory does not exist"
    echo ""
    echo "   ACTION REQUIRED:"
    echo "   Create directory: mkdir -p $SCRIPT_DIR"
fi
echo ""

# Check log directory
echo "3. Log Directory ($LOG_DIR):"
if [ -d "$LOG_DIR" ]; then
    echo "   ✓ Directory exists"
    
    # Check if writable
    if [ -w "$LOG_DIR" ]; then
        echo "   ✓ Directory is writable"
    else
        echo "   ✗ Directory is NOT writable"
        echo "   ACTION REQUIRED: chmod 755 $LOG_DIR"
    fi
else
    echo "   ✗ Directory does not exist"
    echo "   Will be created automatically when script runs"
fi
echo ""

# Check tmux
echo "4. Tmux Installation:"
if command -v tmux &> /dev/null; then
    TMUX_VERSION=$(tmux -V)
    echo "   ✓ tmux is installed ($TMUX_VERSION)"
else
    echo "   ✗ tmux is NOT installed"
    echo ""
    echo "   ACTION REQUIRED:"
    echo "   Install tmux: sudo apt-get update && sudo apt-get install -y tmux"
fi
echo ""

# Check tmux session
echo "5. Tmux Session Status:"
if command -v tmux &> /dev/null; then
    if tmux has-session -t $TMUX_SESSION 2>/dev/null; then
        echo "   ✓ Session '$TMUX_SESSION' is RUNNING"
        echo ""
        echo "   To view: tmux attach -t $TMUX_SESSION"
        echo "   To stop: ./stop_predb_club_updater.sh"
    else
        echo "   ○ Session '$TMUX_SESSION' is not running"
        echo ""
        echo "   To start: ./start_predb_club_updater.sh"
    fi
else
    echo "   ⚠ Cannot check (tmux not installed)"
fi
echo ""

# Check PHP
echo "6. PHP Installation:"
if command -v php &> /dev/null; then
    PHP_VERSION=$(php -v | head -n 1)
    echo "   ✓ PHP is installed"
    echo "   $PHP_VERSION"
    
    # Check required extensions
    echo ""
    echo "   Required PHP extensions:"
    
    if php -m | grep -q mysqli; then
        echo "   ✓ mysqli"
    else
        echo "   ✗ mysqli (required for database connection)"
    fi
    
    if php -m | grep -q curl; then
        echo "   ✓ curl"
    else
        echo "   ✗ curl (required for API calls)"
    fi
    
    if php -m | grep -q json; then
        echo "   ✓ json"
    else
        echo "   ✗ json (required for API responses)"
    fi
else
    echo "   ✗ PHP is NOT installed"
    echo ""
    echo "   ACTION REQUIRED:"
    echo "   Install PHP: sudo apt-get install php php-cli php-mysqli php-curl php-json"
fi
echo ""

# Check database connection
echo "7. Database Connection:"
if command -v php &> /dev/null; then
    php -r "
    \$conn = @new mysqli('localhost', 'predb', 'jBLPdZTT6xru[7wV', 'predb');
    if (\$conn->connect_error) {
        echo '   ✗ Cannot connect to database\n';
        echo '   Error: ' . \$conn->connect_error . '\n';
    } else {
        echo '   ✓ Database connection successful\n';
        
        // Check if releases table exists
        \$result = \$conn->query(\"SHOW TABLES LIKE 'releases'\");
        if (\$result && \$result->num_rows > 0) {
            echo '   ✓ releases table exists\n';
            
            // Check for required columns
            \$result = \$conn->query(\"DESCRIBE releases\");
            \$columns = [];
            while (\$row = \$result->fetch_assoc()) {
                \$columns[] = \$row['Field'];
            }
            
            echo '\n   Column checks:\n';
            echo '   ' . (in_array('genre', \$columns) ? '✓' : '✗') . ' genre\n';
            echo '   ' . (in_array('size', \$columns) ? '✓' : '✗') . ' size\n';
            echo '   ' . (in_array('files', \$columns) ? '✓' : '✗') . ' files\n';
        } else {
            echo '   ✗ releases table not found\n';
        }
        \$conn->close();
    }
    " 2>&1
else
    echo "   ⚠ Cannot check (PHP not installed)"
fi
echo ""

# Summary
echo "=========================================="
echo "Setup Summary"
echo "=========================================="
echo ""

# Check all requirements
ALL_GOOD=true

if [ ! -f "$SCRIPT_DIR/predb_club_updater.php" ]; then
    echo "✗ Missing predb_club_updater.php - Upload to $SCRIPT_DIR"
    ALL_GOOD=false
fi

if ! command -v tmux &> /dev/null; then
    echo "✗ tmux not installed - Run: sudo apt-get install tmux"
    ALL_GOOD=false
fi

if ! command -v php &> /dev/null; then
    echo "✗ PHP not installed - Run: sudo apt-get install php php-cli php-mysqli php-curl"
    ALL_GOOD=false
fi

if [ "$ALL_GOOD" = true ]; then
    echo "✓ All requirements met!"
    echo ""
    echo "Ready to start:"
    echo "  ./start_predb_club_updater.sh"
    echo ""
    echo "Or use the management script:"
    echo "  ./manage_predb_updater.sh start"
fi

echo ""