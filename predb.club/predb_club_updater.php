<?php
// Configuration
$verbose = false; // Set to false when running as systemd service to avoid double logging
$logDir = ($_SERVER['HOME'] ?? '/home/' . get_current_user()) . '/apps/predb.club/logs';
$batchSize = 500;
$idleTime = 300; // 5 minutes
$checkInterval = 60; // Check every minute during idle
// DEBUG: Check if we're being included or run twice
error_log("SCRIPT START - PID: " . getmypid() . " - Time: " . microtime(true));
// END

class PreDBClubUpdater {
    private $db;
    private $apiBaseUrl = 'https://predb.club/api/v1/';
    private $maxRetries = 3;
    private $retryDelay = 2;
    private $logDir;
    private $configDir;
    private $apiLogFile;
    private $statsLogFile;
    private $verbose;
    private $processedReleasesFile;
    private $rateLimitDelay = 2; // 30 requests per 60s = ~2 seconds between requests
    
    public function __construct($config = [], $verbose = true, $logDir = null, $configDir = null) {
        // Set config directory
        $this->configDir = $configDir ?? __DIR__ . '/config';
        
        // Load config file if exists and no config passed
        if (empty($config) && file_exists($this->configDir . '/config.php')) {
            $fileConfig = require $this->configDir . '/config.php';
            $config = array_merge($fileConfig, $config);
        }
        
        // Set log directory
        $this->logDir = $logDir ?? __DIR__ . '/logs';
        $this->verbose = $verbose;
        
        // Ensure log directory exists
        if (!is_dir($this->logDir)) {
            mkdir($this->logDir, 0755, true);
        }
        
        // Set up log files
        $this->apiLogFile = $this->logDir . '/predb_club_updater.log';
        $this->statsLogFile = $this->logDir . '/predb_club_stats.log';
        $this->processedReleasesFile = $this->logDir . '/predb_club_processed.json';
        
        // Get database credentials from config
        $dbHost = $config['db']['host'] ?? 'localhost';
        $dbUser = $config['db']['user'] ?? '';
        $dbPass = $config['db']['pass'] ?? '';
        $dbName = $config['db']['name'] ?? '';
        
        $this->apiLog("=== PreDB.club Updater Started ===");
        $this->apiLog("Database: {$dbName}@{$dbHost}");
        
        try {
            $this->db = new mysqli($dbHost, $dbUser, $dbPass, $dbName);
            $this->db->set_charset("utf8mb4");
            $this->apiLog("Database connection established successfully");
        } catch (Exception $e) {
            $this->apiLog("DATABASE CONNECTION FAILED: " . $e->getMessage(), 'ERROR');
            throw $e;
        }
        
        $this->initializeProcessedReleases();
    }
    
    /**
     * Log API-related messages
     */
    private function apiLog($message, $level = 'INFO') {
        $timestamp = date('Y-m-d H:i:s');
        $pid = getmypid();  // Add this
        $logMessage = "[{$timestamp}] [PID:{$pid}] [{$level}] {$message}\n";  // Modified
        
        file_put_contents($this->apiLogFile, $logMessage, FILE_APPEND | LOCK_EX);
        
        if ($this->verbose) {
            echo $logMessage;
        }
    }
    
    /**
     * Log statistics
     */
    private function statsLog($message) {
        $timestamp = date('Y-m-d H:i:s');
        $logMessage = "[{$timestamp}] {$message}\n";
        
        file_put_contents($this->statsLogFile, $logMessage, FILE_APPEND | LOCK_EX);
    }
    
    /**
     * Get API key from config file
     */
    private function getApiKey() {
        // Config file path (outside web root, secure location)
        $configFile = $this->configDir . '/config.php';
        
        if (file_exists($configFile)) {
            $config = require $configFile;
            if (isset($config['api_key']) && !empty($config['api_key'])) {
                return $config['api_key'];
            }
        }
        
        // Fallback: try environment variable if config not found
        $envKey = getenv('PREDB_API_KEY');
        if ($envKey !== false && !empty($envKey)) {
            return $envKey;
        }
        
        return null;
    }  
    /**
     * Initialize the processed releases tracking file
     */
    private function initializeProcessedReleases() {
        if (!file_exists($this->processedReleasesFile)) {
            $initialData = [
                'processed_ids' => [], 
                'last_run' => null,
                'releases' => [],
                'failed_releases' => []
            ];
            $this->saveProcessedData($initialData);
            $this->apiLog("Created new processed releases tracking file");
        } else {
            $data = $this->loadProcessedData();
            if ($data === null) {
                $this->apiLog("WARNING: Corrupted processed releases file detected, recreating...", 'WARNING');
                $this->resetProcessedReleases();
            } else {
                $count = count($data['processed_ids'] ?? []);
                $this->apiLog("Loaded {$count} previously processed releases from tracking file");
            }
        }
    }
    
    /**
     * Load processed data with error handling
     */
    private function loadProcessedData() {
        if (!file_exists($this->processedReleasesFile)) {
            return null;
        }
        
        $fileContent = @file_get_contents($this->processedReleasesFile);
        if ($fileContent === false) {
            return null;
        }
        
        $data = json_decode($fileContent, true);
        return is_array($data) ? $data : null;
    }
    
    /**
     * Save processed data with atomic write
     */
    private function saveProcessedData($data) {
        $tempFile = $this->processedReleasesFile . '.tmp';
        $json = json_encode($data, JSON_PRETTY_PRINT);
        
        if (file_put_contents($tempFile, $json, LOCK_EX) !== false) {
            return rename($tempFile, $this->processedReleasesFile);
        }
        
        return false;
    }
    
    /**
     * Get list of already processed release IDs
     */
    private function getProcessedReleaseIds() {
        $data = $this->loadProcessedData();
        return (is_array($data) && isset($data['processed_ids']) && is_array($data['processed_ids'])) 
            ? $data['processed_ids'] 
            : [];
    }
    
    /**
     * Mark a release as processed
     */
    private function markReleaseAsProcessed($releaseId, $releaseName, $genre = null, $size = null, $files = null, $status = 'SUCCESS') {
        $data = $this->loadProcessedData() ?? [
            'processed_ids' => [], 
            'last_run' => null, 
            'releases' => [],
            'failed_releases' => []
        ];
        
        // Ensure required keys exist
        foreach (['processed_ids', 'releases', 'failed_releases'] as $key) {
            if (!isset($data[$key]) || !is_array($data[$key])) {
                $data[$key] = [];
            }
        }
        
        // Add to processed list if not already there
        if (!in_array($releaseId, $data['processed_ids'])) {
            $data['processed_ids'][] = $releaseId;
            $data['last_run'] = date('Y-m-d H:i:s');
            
            $releaseInfo = [
                'name' => $releaseName,
                'genre' => $genre,
                'size' => $size,
                'files' => $files,
                'status' => $status,
                'processed_at' => date('Y-m-d H:i:s')
            ];
            
            // Store in appropriate array
            if ($status === 'SUCCESS') {
                $data['releases'][$releaseId] = $releaseInfo;
            } else {
                $data['failed_releases'][$releaseId] = $releaseInfo;
            }
            
            $this->saveProcessedData($data);
            $this->apiLog("Marked release ID {$releaseId} as processed with status: {$status}");
        }
    }
    /**
     * Get list of successfully processed release IDs
     */
    private function getSuccessfulReleaseIds() {
        $data = $this->loadProcessedData();
        if (!is_array($data) || !isset($data['releases'])) {
            return [];
        }
        // Return array of keys (release IDs) from successful releases
        return array_keys($data['releases']);
    }    

    /**
     * Check if a release has already been processed
     */
    private function isReleaseProcessed($releaseId) {
        return in_array($releaseId, $this->getProcessedReleaseIds());
    }
    
    /**
     * Get releases without genre/size/files data
     */
    public function getReleasesWithoutData($limit = 100) {
        $this->apiLog("Searching for up to {$limit} releases without genre/size/files data...");
        
        // Get successful IDs for filtering
        $successfulIds = $this->getSuccessfulReleaseIds();
        $this->apiLog("Excluding " . count($successfulIds) . " already successfully processed releases");
        
        // Simple query without complex NOT IN
        $query = "SELECT id, releasename FROM releases 
                WHERE (genre IS NULL OR genre = '' OR size IS NULL OR size = 0 OR files IS NULL OR files = 0)
                ORDER BY id DESC 
                LIMIT " . ($limit * 3);  // Fetch more to account for filtering
        
        $result = $this->db->query($query);
        
        $releases = [];
        $count = 0;
        while ($row = $result->fetch_assoc()) {
            // Skip if already successfully processed
            if (in_array($row['id'], $successfulIds)) {
                continue;
            }
            
            $releases[] = [
                'id' => $row['id'],
                'releasename' => $row['releasename']
            ];
            $count++;
            
            if ($count >= $limit) {
                break;
            }
        }
        
        $this->apiLog("Found {$count} releases needing data update");
        
        return $releases;
    }
    
    /**
     * Get data from PreDB.club API
     */
    private function getDataFromAPI($releaseName) {
        $this->apiLog("Fetching data for: {$releaseName}");
        
        for ($attempt = 1; $attempt <= $this->maxRetries; $attempt++) {
            try {
                // URL encode the release name for the query parameter
                $encodedRelease = urlencode($releaseName);
                $url = $this->apiBaseUrl . "?q=" . $encodedRelease;
                
                $this->apiLog("API Request (Attempt {$attempt}): {$url}");
                
                $ch = curl_init();
                curl_setopt($ch, CURLOPT_URL, $url);
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_TIMEOUT, 30);
                curl_setopt($ch, CURLOPT_USERAGENT, 'curl/7.68.0');
                curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
                
                $response = curl_exec($ch);
                $this->apiLog("RAW RESPONSE: " . substr($response, 0, 500)); // Log first 500 chars
                $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                $curlError = curl_error($ch);
                curl_close($ch);
                
                if ($curlError) {
                    $this->apiLog("cURL Error (Attempt {$attempt}): {$curlError}", 'WARNING');
                    sleep($this->retryDelay);
                    continue;
                }
                
                if ($httpCode !== 200) {
                    $this->apiLog("HTTP {$httpCode} (Attempt {$attempt})", 'WARNING');
                    sleep($this->retryDelay);
                    continue;
                }
                
                $data = json_decode($response, true);
                /*
                DEBUG
                */
                if (isset($data['data']['rows'])) {
                    $this->apiLog("API returned " . count($data['data']['rows']) . " rows");
                    foreach ($data['data']['rows'] as $idx => $row) {
                        $this->apiLog("Row {$idx}: name='{$row['name']}'");
                        $this->apiLog("  size={$row['size']}, files={$row['files']}");
                        // Check if names match with various comparisons
                        if (isset($row['name'])) {
                            $this->apiLog("  strcmp: " . strcmp($row['name'], $releaseName));
                            $this->apiLog("  case-insensitive: " . (strcasecmp($row['name'], $releaseName) === 0 ? 'MATCH' : 'NO MATCH'));
                        }
                    }
                }
                /*
                END
                */
                if (!$data || !isset($data['status'])) {
                    $this->apiLog("Invalid JSON response (Attempt {$attempt})", 'WARNING');
                    sleep($this->retryDelay);
                    continue;
                }
                
                if ($data['status'] === 'error') {
                    $this->apiLog("API Error: " . ($data['message'] ?? 'Unknown error'), 'WARNING');
                    return ['success' => false, 'genre' => null, 'size' => null, 'files' => null];
                }
                
                if ($data['status'] === 'success' && isset($data['data']['rows']) && !empty($data['data']['rows'])) {
                    // Try to find exact match first, otherwise use first result
                    $release = null;
                    $exactMatch = false;
                    
                    // Look for exact match
                    foreach ($data['data']['rows'] as $row) {
                        if (isset($row['name']) && $row['name'] === $releaseName) {
                            $release = $row;
                            $exactMatch = true;
                            break;
                        }
                    }
                    
                    // If no exact match, use first result
                    if (!$release) {
                        $release = $data['data']['rows'][0];
                    }
                    
                    $matchType = $exactMatch ? "EXACT MATCH" : "FUZZY MATCH";
                    $apiReleaseName = $release['name'] ?? 'Unknown';
                    
                    // Check for genre in both top-level and nested media object
                    $genre = null;
                    if (!empty($release['genre'])) {
                        $genre = $release['genre'];
                    } elseif (!empty($release['media']['genre'])) {
                        $genre = $release['media']['genre'];
                    }
                    
                    // Transform genre format: replace ", " with "_" 
                    if ($genre !== null) {
                        $genre = str_replace([', ', '/'], '_', $genre);
                    }
                    
                    $size = isset($release['size']) ? $release['size'] : null; // Size is actually in MB from API (despite docs saying KB)
                    $files = isset($release['files']) ? $release['files'] : null;
                    
                    // Size is already in MB from the API
                    $sizeMB = $size;
                    
                    $this->apiLog("✓ Found data [{$matchType}]");
                    $this->apiLog("  API Release: {$apiReleaseName}");
                    $this->apiLog("  Genre: " . ($genre ?? 'N/A') . ", Size: " . ($sizeMB !== null ? $sizeMB . " MB" : 'N/A') . ", Files: " . ($files !== null ? $files : 'N/A'));
                    
                    return [
                        'success' => true,
                        'genre' => $genre,
                        'size' => $sizeMB, // Store MB directly
                        'files' => $files
                    ];
                }
                
                $this->apiLog("No matching release found in API");
                return ['success' => false, 'genre' => null, 'size' => null, 'files' => null];
                
            } catch (Exception $e) {
                $this->apiLog("Exception (Attempt {$attempt}): " . $e->getMessage(), 'ERROR');
                if ($attempt < $this->maxRetries) {
                    sleep($this->retryDelay);
                }
            }
        }
        
        $this->apiLog("All API attempts failed", 'ERROR');
        return ['success' => false, 'genre' => null, 'size' => null, 'files' => null];
    }
    
    /**
     * Update release data in database
     */
    private function updateReleaseData($releaseId, $releaseName, $genre, $size, $files) {
        $updates = [];
        $types = '';
        $params = [];
        
        // Build dynamic update query based on what data we have
        if ($genre !== null) {
            $updates[] = "genre = ?";
            $types .= 's';
            $params[] = $genre;
        }
        
        if ($size !== null) {
            $updates[] = "size = ?";
            $types .= 'd'; // Changed to 'd' for decimal/float since we're storing MB with decimals
            $params[] = $size;
        }
        
        if ($files !== null) {
            $updates[] = "files = ?";
            $types .= 'i';
            $params[] = $files;
        }
        
        if (empty($updates)) {
            $this->apiLog("No data to update for release ID {$releaseId}", 'WARNING');
            return false;
        }
        
        $updateStr = implode(', ', $updates);
        $query = "UPDATE releases SET {$updateStr} WHERE id = ?";
        $types .= 'i';
        $params[] = $releaseId;
        
        $stmt = $this->db->prepare($query);
        if (!$stmt) {
            $this->apiLog("Failed to prepare update query: " . $this->db->error, 'ERROR');
            return false;
        }
        
        $stmt->bind_param($types, ...$params);
        
        if ($stmt->execute()) {
            $this->apiLog("✓ Updated release ID {$releaseId} successfully");
            $stmt->close();
            return true;
        } else {
            $this->apiLog("Failed to update release ID {$releaseId}: " . $stmt->error, 'ERROR');
            $stmt->close();
            return false;
        }
    }
    
    /**
     * Process releases in batch
     */
    public function processReleases($batchSize = 100) {
        $releases = $this->getReleasesWithoutData($batchSize);
        
        if (empty($releases)) {
            $this->apiLog("No releases to process");
            return ['processed' => 0, 'updated' => 0, 'failed' => 0, 'remaining' => 0];
        }
        
        $processed = 0;
        $updated = 0;
        $failed = 0;
        
        foreach ($releases as $release) {
            $releaseId = $release['id'];
            $releaseName = $release['releasename'];
            
            // Skip only if successfully processed
            $data = $this->loadProcessedData();
            if (isset($data['releases'][$releaseId])) {
                $this->apiLog("Skipping already processed release ID {$releaseId}");
                continue;
            }
            
            $this->apiLog("Processing [ID: {$releaseId}]: {$releaseName}");
            
            $apiResult = $this->getDataFromAPI($releaseName);
            
            if ($apiResult['success']) {
                if ($this->updateReleaseData($releaseId, $releaseName, $apiResult['genre'], $apiResult['size'], $apiResult['files'])) {
                    $this->markReleaseAsProcessed($releaseId, $releaseName, $apiResult['genre'], $apiResult['size'], $apiResult['files'], 'SUCCESS');
                    $updated++;
                } else {
                    $this->markReleaseAsProcessed($releaseId, $releaseName, null, null, null, 'UPDATE_FAILED');
                    $failed++;
                }
            } else {
                $this->markReleaseAsProcessed($releaseId, $releaseName, null, null, null, 'NO_DATA_FOUND');
                $failed++;
            }
            
            $processed++;
            
            // Rate limiting - respect the 30 requests per 60 seconds limit
            sleep($this->rateLimitDelay);
        }
        
        // Get remaining count
        $remaining = $this->getRemainingCount();
        
        $this->statsLog("Processed: {$processed}, Updated: {$updated}, Failed: {$failed}, Remaining: {$remaining}");
        
        return [
            'processed' => $processed,
            'updated' => $updated,
            'failed' => $failed,
            'remaining' => $remaining
        ];
    }
    
    /**
     * Get count of remaining releases to process
     */
    private function getRemainingCount() {
        $successfulIds = $this->getSuccessfulReleaseIds();
        
        $query = "SELECT id FROM releases 
                WHERE (genre IS NULL OR genre = '' OR size IS NULL OR size = 0 OR files IS NULL OR files = 0)";
        
        $result = $this->db->query($query);
        
        $count = 0;
        while ($row = $result->fetch_assoc()) {
            if (!in_array($row['id'], $successfulIds)) {
                $count++;
            }
        }
        
        return $count;
    }
    
    /**
     * Get statistics
     */
    public function getStatistics() {
        $stats = [];
        
        // Total releases
        $result = $this->db->query("SELECT COUNT(*) as count FROM releases");
        $stats['total_releases'] = $result->fetch_assoc()['count'];
        
        // Releases with complete data
        $result = $this->db->query("SELECT COUNT(*) as count FROM releases WHERE genre IS NOT NULL AND genre != '' AND size IS NOT NULL AND size > 0 AND files IS NOT NULL AND files > 0");
        $stats['with_complete_data'] = $result->fetch_assoc()['count'];
        
        // Releases without complete data
        $result = $this->db->query("SELECT COUNT(*) as count FROM releases WHERE (genre IS NULL OR genre = '' OR size IS NULL OR size = 0 OR files IS NULL OR files = 0)");
        $stats['without_complete_data'] = $result->fetch_assoc()['count'];
        
        // Completion percentage
        $stats['completion_percentage'] = $stats['total_releases'] > 0 
            ? round(($stats['with_complete_data'] / $stats['total_releases']) * 100, 2) 
            : 0;
        
        // Get processed data
        $data = $this->loadProcessedData();
        $processedIds = $data['processed_ids'] ?? [];
        $stats['total_processed'] = count($processedIds);
        $stats['successful'] = count($data['releases'] ?? []);
        $stats['failed'] = count($data['failed_releases'] ?? []);
        $stats['last_run'] = $data['last_run'] ?? 'Never';
        
        $this->apiLog("=== Statistics ===");
        $this->apiLog("Total releases: {$stats['total_releases']}");
        $this->apiLog("With complete data: {$stats['with_complete_data']} ({$stats['completion_percentage']}%)");
        $this->apiLog("Without complete data: {$stats['without_complete_data']}");
        $this->apiLog("Processed by script: {$stats['total_processed']}");
        $this->apiLog("  - Successful: {$stats['successful']}");
        $this->apiLog("  - Failed: {$stats['failed']}");
        
        return $stats;
    }
    
    /**
     * Clean up old processed IDs to prevent memory issues
     */
    public function cleanupProcessedIds($keepLast = 50000) {
        $data = $this->loadProcessedData();
        if (!$data || !isset($data['processed_ids'])) {
            return;
        }
        
        $totalIds = count($data['processed_ids']);
        if ($totalIds <= $keepLast) {
            $this->apiLog("Cleanup not needed: {$totalIds} IDs (limit: {$keepLast})");
            return;
        }
        
        // Keep only the last N IDs
        $data['processed_ids'] = array_slice($data['processed_ids'], -$keepLast);
        
        // Also clean up the releases arrays
        $keptIds = array_flip($data['processed_ids']);
        $data['releases'] = array_intersect_key($data['releases'] ?? [], $keptIds);
        $data['failed_releases'] = array_intersect_key($data['failed_releases'] ?? [], $keptIds);
        
        $this->saveProcessedData($data);
        $removed = $totalIds - count($data['processed_ids']);
        $this->apiLog("Cleaned up {$removed} old processed IDs (kept last {$keepLast})");
    }
    
    /**
     * Reset processed releases
     */
    public function resetProcessedReleases() {
        $initialData = [
            'processed_ids' => [], 
            'last_run' => null,
            'releases' => [],
            'failed_releases' => []
        ];
        $this->saveProcessedData($initialData);
        $this->apiLog("Reset processed releases tracking");
    }
    
    /**
     * Retry failed releases
     */
    public function retryFailedReleases($maxRetries = 50) {
        $data = $this->loadProcessedData();
        $failedReleases = $data['failed_releases'] ?? [];
        
        if (empty($failedReleases)) {
            $this->apiLog("No failed releases to retry");
            return ['processed' => 0, 'updated' => 0, 'failed' => 0];
        }
        
        $this->apiLog("Retrying " . min($maxRetries, count($failedReleases)) . " failed releases");
        
        $processed = 0;
        $updated = 0;
        $failed = 0;
        
        foreach (array_slice($failedReleases, 0, $maxRetries, true) as $releaseId => $info) {
            // Remove from processed IDs to allow retry
            $processedIds = $data['processed_ids'];
            if (($key = array_search($releaseId, $processedIds)) !== false) {
                unset($processedIds[$key]);
                $data['processed_ids'] = array_values($processedIds);
                unset($data['failed_releases'][$releaseId]);
                $this->saveProcessedData($data);
            }
            
            $this->apiLog("Retrying release [ID: {$releaseId}]: {$info['name']}");
            
            $apiResult = $this->getDataFromAPI($info['name']);
            
            if ($apiResult['success']) {
                if ($this->updateReleaseData($releaseId, $info['name'], $apiResult['genre'], $apiResult['size'], $apiResult['files'])) {
                    $this->markReleaseAsProcessed($releaseId, $info['name'], $apiResult['genre'], $apiResult['size'], $apiResult['files'], 'SUCCESS');
                    $updated++;
                } else {
                    $this->markReleaseAsProcessed($releaseId, $info['name'], null, null, null, 'UPDATE_FAILED');
                    $failed++;
                }
            } else {
                $this->markReleaseAsProcessed($releaseId, $info['name'], null, null, null, 'NO_DATA_FOUND');
                $failed++;
            }
            
            $processed++;
            sleep($this->rateLimitDelay); // Rate limiting
        }
        
        return ['processed' => $processed, 'updated' => $updated, 'failed' => $failed];
    }
    
    public function __destruct() {
        $this->apiLog("=== PreDB.club Updater Finished ===");
        if ($this->db) {
            $this->db->close();
        }
    }
}

// Signal handling for graceful shutdown (CLI only)
$shutdownRequested = false;

if (php_sapi_name() === 'cli' && function_exists('pcntl_async_signals')) {
    pcntl_async_signals(true);  // Use async signals (PHP 7.1+) - more reliable than declare(ticks=1)
    
    pcntl_signal(SIGINT, function() use (&$shutdownRequested) {
        global $shutdownRequested;
        $shutdownRequested = true;
        error_log("⚠️  SIGINT received. Shutdown requested.");
    });
    
    pcntl_signal(SIGTERM, function() use (&$shutdownRequested) {
        global $shutdownRequested;
        $shutdownRequested = true;
        error_log("⚠️  SIGTERM received. Shutdown requested.");
    });
}

try {
    $updater = new PreDBClubUpdater(
        [],  // Empty array - will load from config file
        $verbose,
        $logDir
    );
    
    $runCount = 0;
    
    error_log("🎬 PreDB.club Continuous Updater Started");
    
    while (true) {
        // Check for shutdown signal
        if ($shutdownRequested) {
            error_log("🛑 Graceful shutdown initiated.");
            break;
        }
        
        $runCount++;
        
        // Get statistics
        $stats = $updater->getStatistics();
        
        if ($stats && $stats['without_complete_data'] > 0) {
            error_log("📊 Run #{$runCount} - " . date('H:i:s') . " - Releases without complete data: {$stats['without_complete_data']}");
            
            // Process batch
            $result = $updater->processReleases($batchSize);
            
            error_log("Processed: {$result['processed']}, Updated: {$result['updated']}, Failed: {$result['failed']}, Remaining: {$result['remaining']}");

            if ($runCount % 10 === 0) {
                $updater->cleanupProcessedIds(50000);
            }            
            
            // Brief pause if work remains
            if ($result['remaining'] > 0) {
                sleep(3);
            }
        } else {
            // No work to do, idle
            error_log("😴 No releases needing updates. Idling for " . round($idleTime / 60, 1) . " minutes...");
            
            // Idle with periodic checks for shutdown signal
            for ($i = 0; $i < $idleTime; $i += $checkInterval) {
                if ($shutdownRequested) {
                    break 2;
                }
                
                // Sleep in smaller intervals to check for signals
                for ($j = 0; $j < $checkInterval; $j++) {
                    if ($shutdownRequested) {
                        break 3;
                    }
                    sleep(1);
                }
            }
            
            error_log("Idle period complete, checking for new releases...");
        }
        
        // Memory management
        if (function_exists('gc_mem_caches')) {
            gc_mem_caches();
        }
        
        // Limit memory usage
        $memoryUsage = round(memory_get_usage(true) / 1024 / 1024, 2);
        if ($memoryUsage > 256) {
            error_log("Memory usage high ({$memoryUsage}MB), restarting script...");
            break;
        }
    }
    
    // Final statistics
    $finalStats = $updater->getStatistics();
    if ($finalStats) {
        error_log("=== FINAL STATISTICS ===");
        error_log("Total runs: {$runCount}");
        error_log("Total releases: {$finalStats['total_releases']}");
        error_log("With complete data: {$finalStats['with_complete_data']} ({$finalStats['completion_percentage']}%)");
        error_log("Without complete data: {$finalStats['without_complete_data']}");
    }
    
    error_log("👋 Script terminated gracefully at " . date('Y-m-d H:i:s'));
    
} catch (Exception $e) {
    error_log("PreDB.club Updater Fatal Error: " . $e->getMessage());
    exit(1);
}
?>