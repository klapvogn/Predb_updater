<?php
// Configuration
$verbose = true;
$logDir = ($_SERVER['HOME'] ?? '/home/' . get_current_user()) . '/apps/predb.club/logs';
$batchSize = 500;
$idleTime = 300; // 5 minutes
$checkInterval = 60; // Check every minute during idle
// END

class ReleaseGenreUpdater {
    private $db;
    private $apiBaseUrl = 'https://api.predb.net/?release=';
    private $maxRetries = 3;
    private $retryDelay = 2;
    private $logDir;
    private $configDir;
    private $apiLogFile;
    private $statsLogFile;
    private $verbose;
    private $processedReleasesFile;
    
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
        $this->apiLogFile = $this->logDir . '/api_genre_updater.log';
        $this->statsLogFile = $this->logDir . '/api_stats.log';
        $this->processedReleasesFile = $this->logDir . '/processed_releases.json';
        
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
        $logMessage = "[{$timestamp}] [{$level}] {$message}\n";
        
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
    private function markReleaseAsProcessed($releaseId, $releaseName, $genre = null, $status = 'SUCCESS') {
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
     * Check if a release has already been processed
     */
    private function isReleaseProcessed($releaseId) {
        return in_array($releaseId, $this->getProcessedReleaseIds());
    }
    
    /**
     * Get releases without genre with improved filtering and large list handling
     */
    public function getReleasesWithoutGenre($limit = 100) {
        $this->apiLog("Searching for up to {$limit} releases without genre (Video/Audio/Bluray)...");
        
        $releases = [];
        $processedIds = $this->getProcessedReleaseIds();
        
        // Base query
        $query = "SELECT id, releasename FROM releases 
                WHERE (genre IS NULL OR genre = '') 
                AND (releasename LIKE '%720p%' OR releasename LIKE '%1080p%' OR releasename LIKE '%2160p%'
                    OR releasename LIKE '%FLAC%' OR releasename LIKE '%MP3%'
                    OR releasename LIKE '%BLURAY%' OR releasename LIKE '%MBLURAY%'
                )";
        
        // Handle processed IDs more efficiently for large lists
        if (!empty($processedIds)) {
            $this->apiLog("Excluding " . count($processedIds) . " already processed releases");
            
            // For very large lists, use a subquery with a derived table for large exclusion lists
            if (count($processedIds) > 10000) {
                // Limit the exclusion to a reasonable size (last 10,000 processed)
                $recentProcessedIds = array_slice($processedIds, -10000);
                $placeholders = implode(',', array_fill(0, min(10000, count($recentProcessedIds)), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                
                // Add LIMIT clause
                $query .= " LIMIT ?";
                
                $stmt = $this->db->prepare($query);
                if (!$stmt) {
                    $this->apiLog("Failed to prepare statement: " . $this->db->error, 'ERROR');
                    return [];
                }
                
                // Bind parameters: all IDs first, then the limit
                $types = str_repeat('i', min(10000, count($recentProcessedIds))) . 'i';
                $params = array_merge($recentProcessedIds, [$limit]);
                $stmt->bind_param($types, ...$params);
            } else {
                // For smaller lists, use the normal approach
                $placeholders = implode(',', array_fill(0, count($processedIds), '?'));
                $query .= " AND id NOT IN ({$placeholders}) LIMIT ?";
                
                $stmt = $this->db->prepare($query);
                if (!$stmt) {
                    $this->apiLog("Failed to prepare statement: " . $this->db->error, 'ERROR');
                    return [];
                }
                
                $types = str_repeat('i', count($processedIds)) . 'i';
                $params = array_merge($processedIds, [$limit]);
                $stmt->bind_param($types, ...$params);
            }
        } else {
            $query .= " LIMIT ?";
            $stmt = $this->db->prepare($query);
            if (!$stmt) {
                $this->apiLog("Failed to prepare statement: " . $this->db->error, 'ERROR');
                return [];
            }
            $stmt->bind_param('i', $limit);
        }
        
        $stmt->execute();
        $result = $stmt->get_result();
        
        if ($result && $result->num_rows > 0) {
            $this->apiLog("Found {$result->num_rows} releases without genre");
            while ($row = $result->fetch_assoc()) {
                $releases[] = $row;
            }
        } else {
            $this->apiLog("No new releases found without genre", 'INFO');
        }
        
        $stmt->close();
        
        return $releases;
    }
    
    /**
     * Fetch genre from API with retry and detailed logging
     */
    public function getGenreFromAPI($releaseName) {
        $this->apiLog("API REQUEST: Starting fetch for release: {$releaseName}");
        
        for ($attempt = 1; $attempt <= $this->maxRetries; $attempt++) {
            $this->apiLog("API ATTEMPT: {$attempt}/{$this->maxRetries}");
            
            $encodedRelease = urlencode($releaseName);
            $apiUrl = $this->apiBaseUrl . $encodedRelease;
            
            $this->apiLog("API URL: {$apiUrl}");
            
            $startTime = microtime(true);
            
            $ch = curl_init();
            curl_setopt_array($ch, [
                CURLOPT_URL => $apiUrl,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => 15,
                CURLOPT_CONNECTTIMEOUT => 5,
                CURLOPT_USERAGENT => 'Mozilla/5.0 (compatible; ReleaseGenreUpdater/2.0)',
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_SSL_VERIFYPEER => true,
                CURLOPT_HEADER => true,
                CURLOPT_VERBOSE => false,
            ]);
            
            $response = curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $curlError = curl_error($ch);
            $headerSize = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
            $responseTime = round((microtime(true) - $startTime) * 1000, 2);
            
            // Separate headers and body
            $headers = substr($response, 0, $headerSize);
            $body = substr($response, $headerSize);
            
            curl_close($ch);
            
            $this->apiLog("API RESPONSE: HTTP {$httpCode} in {$responseTime}ms");
            
            // Log cURL errors
            if ($curlError) {
                $this->apiLog("CURL ERROR: {$curlError}", 'ERROR');
                
                if ($attempt < $this->maxRetries) {
                    $this->apiLog("Retrying in {$this->retryDelay} seconds...", 'WARNING');
                    sleep($this->retryDelay);
                    continue;
                }
                
                return [
                    'success' => false,
                    'error' => $curlError,
                    'error_type' => 'curl_error'
                ];
            }
            
            // Log HTTP errors
            if ($httpCode !== 200) {
                $this->apiLog("HTTP ERROR: Status code {$httpCode}", 'ERROR');
                
                // Handle rate limiting (429) with exponential backoff
                if ($httpCode === 429) {
                    $waitTime = $this->retryDelay * $attempt;
                    $this->apiLog("Rate limited (429) - waiting {$waitTime} seconds before retry...", 'WARNING');
                    if ($attempt < $this->maxRetries) {
                        sleep($waitTime);
                        continue;
                    }
                }
                
                if ($httpCode >= 500 && $attempt < $this->maxRetries) {
                    $this->apiLog("Server error, retrying in {$this->retryDelay} seconds...", 'WARNING');
                    sleep($this->retryDelay);
                    continue;
                }
                
                if ($httpCode === 404) {
                    $this->apiLog("Release not found in PreDB (404)", 'WARNING');
                    return [
                        'success' => false,
                        'error' => 'Not found in PreDB',
                        'error_type' => 'not_found'
                    ];
                }
                
                return [
                    'success' => false,
                    'error' => "HTTP {$httpCode}",
                    'error_type' => 'http_error'
                ];
            }
            
            if (empty($body)) {
                $this->apiLog("API ERROR: Empty response body", 'ERROR');
                return [
                    'success' => false,
                    'error' => 'Empty response',
                    'error_type' => 'empty_response'
                ];
            }
            
            // Log response size
            $responseSize = strlen($body);
            $this->apiLog("API RESPONSE SIZE: {$responseSize} bytes");
            
            // Parse JSON response
            $data = json_decode($body, true);
            
            if (json_last_error() !== JSON_ERROR_NONE) {
                $jsonError = json_last_error_msg();
                $this->apiLog("JSON PARSE ERROR: {$jsonError}", 'ERROR');
                
                return [
                    'success' => false,
                    'error' => "JSON parse error: {$jsonError}",
                    'error_type' => 'json_error'
                ];
            }
            
            $this->apiLog("JSON parsed successfully");
            
            // Extract genre
            $genre = $this->extractGenre($data);
            
            if ($genre) {
                $this->apiLog("GENRE EXTRACTED: '{$genre}'", 'SUCCESS');
                return [
                    'success' => true,
                    'genre' => $genre
                ];
            } else {
                $this->apiLog("No genre found in API response", 'WARNING');
                
                return [
                    'success' => false,
                    'error' => 'Genre not found in response',
                    'error_type' => 'not_found'
                ];
            }
        }
        
        $this->apiLog("All retry attempts exhausted", 'ERROR');
        return [
            'success' => false,
            'error' => 'Max retries reached',
            'error_type' => 'max_retries'
        ];
    }
    
    /**
     * Extract genre from API response (from GenreUpdaterTask)
     */
    private function extractGenre($data) {
        $this->apiLog("Attempting to extract genre from API response");
        
        // Check new API format with 'data' array
        if (isset($data['data']) && is_array($data['data']) && count($data['data']) > 0) {
            $this->apiLog("Found 'data' array with " . count($data['data']) . " entries");
            
            $releaseData = $data['data'][0];
            
            if (isset($releaseData['genre']) && !empty($releaseData['genre'])) {
                $genre = $releaseData['genre'];
                $this->apiLog("Extracted genre from data[0]['genre']: '{$genre}'");
                return $genre;
            }
        }
        
        // Check old API format with direct 'genre' key
        if (isset($data['genre']) && !empty($data['genre'])) {
            $genre = $data['genre'];
            $this->apiLog("Extracted genre from direct 'genre' key: '{$genre}'");
            return $genre;
        }
        
        $this->apiLog("No genre found in any expected format", 'WARNING');
        return null;
    }
    
    /**
     * Update release genre in database (from GenreUpdaterTask)
     */
    public function updateReleaseGenre($releaseId, $releaseName, $genre) {
        $this->apiLog("DATABASE UPDATE: Attempting to update ID {$releaseId} with genre '{$genre}'");
        
        try {
            // Normalize genre (replace / and - with _)
            $normalizedGenre = str_replace(['/', '-'], '_', $genre);
            
            if ($normalizedGenre !== $genre) {
                $this->apiLog("Genre normalized: '{$genre}' -> '{$normalizedGenre}'");
            }
            
            // Check current genre status
            $stmt = $this->db->prepare("SELECT genre FROM releases WHERE id = ?");
            $stmt->bind_param("i", $releaseId);
            $stmt->execute();
            $result = $stmt->get_result();
            $current = $result->fetch_assoc();
            
            if (!$current) {
                $this->apiLog("Release ID {$releaseId} not found in database", 'ERROR');
                return false;
            }
            
            // Skip if genre already exists
            if (!empty($current['genre'])) {
                $this->apiLog("Genre already exists: '{$current['genre']}' - skipping", 'WARNING');
                return false;
            }
            
            // Update the genre using ID
            $stmt = $this->db->prepare("UPDATE releases SET genre = ? WHERE id = ?");
            $stmt->bind_param("si", $normalizedGenre, $releaseId);
            $stmt->execute();
            
            $rowCount = $stmt->affected_rows;
            
            if ($rowCount > 0) {
                $this->apiLog("DATABASE UPDATE SUCCESS: {$rowCount} row(s) updated", 'SUCCESS');
                return true;
            } else {
                $this->apiLog("DATABASE UPDATE: No rows affected", 'WARNING');
                return false;
            }
            
        } catch (Exception $e) {
            $this->apiLog("DATABASE ERROR: " . $e->getMessage(), 'ERROR');
            return false;
        }
    }
    
    /**
     * Get current statistics (from GenreUpdaterTask)
     */
    public function getStatistics() {
        try {
            $this->apiLog("=== Gathering Statistics ===");
            
            // Total releases
            $result = $this->db->query("SELECT COUNT(*) as count FROM releases");
            $totalReleases = $result->fetch_assoc()['count'];
            
            // Releases with genre
            $result = $this->db->query("SELECT COUNT(*) as count FROM releases WHERE genre IS NOT NULL AND genre != ''");
            $withGenre = $result->fetch_assoc()['count'];
            
            // Releases without genre
            $withoutGenre = $totalReleases - $withGenre;
            
            // Eligible releases (video/audio formats) without genre
            $result = $this->db->query("SELECT COUNT(*) as count FROM releases 
                                       WHERE (genre IS NULL OR genre = '') 
                                       AND (releasename LIKE '%720p%' OR releasename LIKE '%1080p%' OR releasename LIKE '%2160p%'
                                           OR releasename LIKE '%FLAC%' OR releasename LIKE '%MP3%'
                                           OR releasename LIKE '%BLURAY%' OR releasename LIKE '%MBLURAY%'
                                       )");
            $eligibleWithoutGenre = $result->fetch_assoc()['count'];
            
            $stats = [
                'total_releases' => $totalReleases,
                'with_genre' => $withGenre,
                'without_genre' => $withoutGenre,
                'eligible_without_genre' => $eligibleWithoutGenre,
                'completion_percentage' => $totalReleases > 0 ? round(($withGenre / $totalReleases) * 100, 2) : 0
            ];
            
            $this->apiLog("Total Releases: {$stats['total_releases']}");
            $this->apiLog("With Genre: {$stats['with_genre']}");
            $this->apiLog("Without Genre: {$stats['without_genre']}");
            $this->apiLog("Eligible Without Genre: {$stats['eligible_without_genre']}");
            $this->apiLog("Completion: {$stats['completion_percentage']}%");
            
            $this->statsLog(sprintf(
                "STATS | Total=%d | WithGenre=%d | WithoutGenre=%d | Eligible=%d | Completion=%.2f%%",
                $stats['total_releases'], $stats['with_genre'], $stats['without_genre'],
                $stats['eligible_without_genre'], $stats['completion_percentage']
            ));
            
            return $stats;
            
        } catch (Exception $e) {
            $this->apiLog("Error gathering statistics: " . $e->getMessage(), 'ERROR');
            return null;
        }
    }
    
    /**
     * Main processing method with improved error handling (from GenreUpdaterTask)
     */
    public function processReleases($batchSize = 100) {
        $startTime = microtime(true);
        
        $this->apiLog("=== Starting Genre Update Task ===");
        $this->apiLog("Batch size: {$batchSize}");
        
        $releases = $this->getReleasesWithoutGenre($batchSize);
        
        if (empty($releases)) {
            $this->apiLog("No releases to process", 'INFO');
            return ['processed' => 0, 'updated' => 0, 'failed' => 0, 'remaining' => 0];
        }
        
        $this->apiLog("Processing " . count($releases) . " releases", 'INFO');
        
        $updated = 0;
        $failed = 0;
        $apiErrors = 0;
        $notFound = 0;
        $dbErrors = 0;
        
        foreach ($releases as $index => $release) {
            $releaseNum = $index + 1;
            
            $this->apiLog("--- [{$releaseNum}/" . count($releases) . "] Processing ID: {$release['id']}, Name: {$release['releasename']} ---");
            
            $apiResult = $this->getGenreFromAPI($release['releasename']);
            
            if ($apiResult['success']) {
                $genre = $apiResult['genre'];
                $this->apiLog("API SUCCESS: Genre found = '{$genre}'", 'SUCCESS');
                
                $updateResult = $this->updateReleaseGenre($release['id'], $release['releasename'], $genre);
                
                if ($updateResult) {
                    $updated++;
                    $this->markReleaseAsProcessed($release['id'], $release['releasename'], $genre, 'SUCCESS');
                    $this->apiLog("DATABASE UPDATE: Success", 'SUCCESS');
                } else {
                    $dbErrors++;
                    $failed++;
                    $this->markReleaseAsProcessed($release['id'], $release['releasename'], null, 'UPDATE_FAILED');
                    $this->apiLog("DATABASE UPDATE: Failed", 'ERROR');
                }
            } else {
                $failed++;
                
                if ($apiResult['error_type'] === 'not_found') {
                    $notFound++;
                    $this->markReleaseAsProcessed($release['id'], $release['releasename'], null, 'NO_GENRE_FOUND');
                    $this->apiLog("API RESULT: Genre not found for release", 'WARNING');
                } else {
                    $apiErrors++;
                    $this->markReleaseAsProcessed($release['id'], $release['releasename'], null, 'API_ERROR');
                    $this->apiLog("API ERROR: {$apiResult['error']}", 'ERROR');
                }
            }
            
            // Progress log every 5 releases
            if ($releaseNum % 5 === 0) {
                $this->apiLog("PROGRESS: {$releaseNum}/" . count($releases) . " processed (Updated: {$updated}, Failed: {$failed})");
            }
            
            sleep(3); // Rate limiting
        }
        
        $endTime = microtime(true);
        $duration = round($endTime - $startTime, 2);
        
        // Final summary
        $summary = "=== Genre Update Task Complete ===";
        $this->apiLog($summary);
        $this->apiLog("Total Processed: " . count($releases));
        $this->apiLog("Successfully Updated: {$updated}");
        $this->apiLog("Failed: {$failed}");
        $this->apiLog("  - Not Found in API: {$notFound}");
        $this->apiLog("  - API Errors: {$apiErrors}");
        $this->apiLog("  - Database Errors: {$dbErrors}");
        $this->apiLog("Duration: {$duration} seconds");
        $this->apiLog("Average: " . round($duration / count($releases), 2) . " seconds per release");
        
        // Stats log
        $this->statsLog(sprintf(
            "BATCH_COMPLETE | Processed=%d | Updated=%d | Failed=%d | NotFound=%d | APIErrors=%d | DBErrors=%d | Duration=%.2fs",
            count($releases), $updated, $failed, $notFound, $apiErrors, $dbErrors, $duration
        ));
        
        $remaining = max(0, $this->countReleasesWithoutGenre() - count($releases));
        
        return [
            'processed' => count($releases),
            'updated' => $updated,
            'failed' => $failed,
            'not_found' => $notFound,
            'api_errors' => $apiErrors,
            'db_errors' => $dbErrors,
            'duration' => $duration,
            'remaining' => $remaining
        ];
    }
    
    /**
     * Quick count of remaining releases without genre
     */
    private function countReleasesWithoutGenre() {
        $processedIds = $this->getProcessedReleaseIds();
        
        $query = "SELECT COUNT(*) as count FROM releases 
                WHERE (genre IS NULL OR genre = '') 
                AND (releasename LIKE '%720p%' OR releasename LIKE '%1080p%' OR releasename LIKE '%2160p%'
                    OR releasename LIKE '%FLAC%' OR releasename LIKE '%MP3%'
                    OR releasename LIKE '%BLURAY%' OR releasename LIKE '%MBLURAY%'
                )";
        
        if (!empty($processedIds)) {
            if (count($processedIds) > 10000) {
                // Use a subquery with a derived table for large exclusion lists
                $recentProcessedIds = array_slice($processedIds, -10000);
                $placeholders = implode(',', array_fill(0, min(10000, count($recentProcessedIds)), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                
                $stmt = $this->db->prepare($query);
                $types = str_repeat('i', min(10000, count($recentProcessedIds)));
                $stmt->bind_param($types, ...$recentProcessedIds);
            } else {
                $placeholders = implode(',', array_fill(0, count($processedIds), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                
                $stmt = $this->db->prepare($query);
                $types = str_repeat('i', count($processedIds));
                $stmt->bind_param($types, ...$processedIds);
            }
            $stmt->execute();
            $result = $stmt->get_result();
        } else {
            $result = $this->db->query($query);
        }
        
        $row = $result->fetch_assoc();
        return $row['count'] ?? 0;
    }

     /**
     * Clean up old processed IDs to keep the list manageable
     */
    public function cleanupProcessedIds($keepLast = 50000) {
        $data = $this->loadProcessedData();
        
        if (empty($data['processed_ids'])) {
            $this->apiLog("No processed IDs to clean up");
            return;
        }
        
        $currentCount = count($data['processed_ids']);
        
        if ($currentCount > $keepLast) {
            // Keep only the most recent IDs
            $data['processed_ids'] = array_slice($data['processed_ids'], -$keepLast);
            
            // Also clean up the releases and failed_releases arrays
            $data['releases'] = array_slice($data['releases'], -$keepLast, $keepLast, true);
            $data['failed_releases'] = array_slice($data['failed_releases'], -$keepLast, $keepLast, true);
            
            $this->saveProcessedData($data);
            
            $this->apiLog("Cleaned up processed IDs: kept {$keepLast} most recent (from {$currentCount})");
        }
    }

    /**
     * Get releases without genre - alternative approach using exclusion by time
     */
    public function getReleasesWithoutGenreAlternative($limit = 100) {
        $this->apiLog("Searching for releases without genre (alternative method)...");
        
        // Get the timestamp of the last processed release (if any)
        $data = $this->loadProcessedData();
        $lastRun = $data['last_run'] ?? null;
        
        $query = "SELECT id, releasename FROM releases 
                WHERE (genre IS NULL OR genre = '') 
                AND (releasename LIKE '%720p%' OR releasename LIKE '%1080p%' OR releasename LIKE '%2160p%'
                    OR releasename LIKE '%FLAC%' OR releasename LIKE '%MP3%'
                    OR releasename LIKE '%BLURAY%' OR releasename LIKE '%MBLURAY%'
                )";
        
        // If we have a last run time, get releases added after that
        // This assumes releases are generally inserted in chronological order
        if ($lastRun) {
            $query .= " AND added > ?";
            $query .= " ORDER BY id ASC LIMIT ?"; // Get oldest first
            
            $stmt = $this->db->prepare($query);
            $stmt->bind_param('si', $lastRun, $limit);
        } else {
            $query .= " ORDER BY id ASC LIMIT ?"; // Get oldest first
            $stmt = $this->db->prepare($query);
            $stmt->bind_param('i', $limit);
        }
        
        if (!$stmt) {
            $this->apiLog("Failed to prepare statement: " . $this->db->error, 'ERROR');
            return [];
        }
        
        $stmt->execute();
        $result = $stmt->get_result();
        
        $releases = [];
        if ($result && $result->num_rows > 0) {
            $this->apiLog("Found {$result->num_rows} releases without genre");
            while ($row = $result->fetch_assoc()) {
                $releases[] = $row;
            }
        } else {
            $this->apiLog("No new releases found without genre", 'INFO');
        }
        
        $stmt->close();
        
        return $releases;
    }
    
    /**
     * Get processing statistics
     */
    public function getProcessingStats() {
        $data = $this->loadProcessedData() ?? [];
        $processedCount = count($data['processed_ids'] ?? []);
        $successCount = count($data['releases'] ?? []);
        $failedCount = count($data['failed_releases'] ?? []);
        $lastRun = $data['last_run'] ?? 'Never';
        
        $this->apiLog("Processing Statistics:");
        $this->apiLog("  - Total processed: {$processedCount}");
        $this->apiLog("  - Successful: {$successCount}");
        $this->apiLog("  - Failed: {$failedCount}");
        $this->apiLog("  - Last run: {$lastRun}");
        
        return [
            'total_processed' => $processedCount,
            'successful' => $successCount,
            'failed' => $failedCount,
            'last_run' => $lastRun
        ];
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
            
            $apiResult = $this->getGenreFromAPI($info['name']);
            
            if ($apiResult['success']) {
                if ($this->updateReleaseGenre($releaseId, $info['name'], $apiResult['genre'])) {
                    $this->markReleaseAsProcessed($releaseId, $info['name'], $apiResult['genre'], 'SUCCESS');
                    $updated++;
                } else {
                    $this->markReleaseAsProcessed($releaseId, $info['name'], null, 'UPDATE_FAILED');
                    $failed++;
                }
            } else {
                $this->markReleaseAsProcessed($releaseId, $info['name'], null, 'NO_GENRE_FOUND');
                $failed++;
            }
            
            $processed++;
            sleep(3); // Rate limiting
        }
        
        return ['processed' => $processed, 'updated' => $updated, 'failed' => $failed];
    }
    
    public function __destruct() {
        $this->apiLog("=== Genre Updater Finished ===");
        if ($this->db) {
            $this->db->close();
        }
    }
}

// Signal handling for graceful shutdown (CLI only)
if (php_sapi_name() === 'cli') {
    declare(ticks = 1);
    
    $shutdownRequested = false;
    
    pcntl_signal(SIGINT, function() use (&$shutdownRequested) {
        $shutdownRequested = true;
        echo "\n⚠️  Shutdown requested. Finishing current batch...\n";
    });
    
    pcntl_signal(SIGTERM, function() use (&$shutdownRequested) {
        $shutdownRequested = true;
        echo "\n⚠️  Termination requested. Finishing current batch...\n";
    });
}

try {
    $updater = new ReleaseGenreUpdater(
        [],  // Empty array - will load from config file
        $verbose,
        $logDir
    );
    
    $runCount = 0;
    
    echo "🎬 Continuous Genre Updater Started\n";
    echo "Press Ctrl+C to stop gracefully\n\n";
    
    while (true) {
        // Check for shutdown signal
        if (isset($shutdownRequested) && $shutdownRequested) {
            echo "\n🛑 Graceful shutdown initiated.\n";
            break;
        }
        
        $runCount++;
        $batchStartTime = time();
        
        // Log run start - using echo instead of apiLog()
        echo "=== Starting run #{$runCount} ===\n";
        
        // Get statistics
        $stats = $updater->getStatistics();
        
        if ($stats && $stats['eligible_without_genre'] > 0) {
            echo "\n📊 Run #{$runCount} - " . date('H:i:s') . "\n";
            echo "Eligible releases: {$stats['eligible_without_genre']}\n";
            
            // Process batch
            $result = $updater->processReleases($batchSize);
            
            echo "Processed: {$result['processed']}, Updated: {$result['updated']}, Failed: {$result['failed']}\n";
            echo "Remaining: {$result['remaining']}\n";

            if ($runCount % 10 === 0) {
                echo "🔄 Cleaning up processed IDs (keeping last 50,000)...\n";
                $updater->cleanupProcessedIds(50000);
            }            
            
            // Brief pause if work remains
            if ($result['remaining'] > 0) {
                echo "Continuing in 3 seconds...\n";
                sleep(3);
            }
        } else {
            // No work to do, idle
            echo "\n😴 No eligible releases found. Idling for " . round($idleTime / 60, 1) . " minutes...\n";
            
            // Idle with periodic checks for shutdown signal
            for ($i = 0; $i < $idleTime; $i += $checkInterval) {
                if (isset($shutdownRequested) && $shutdownRequested) {
                    break 2; // Break out of both loops
                }
                
                $remaining = $idleTime - $i;
                $minutes = floor($remaining / 60);
                $seconds = $remaining % 60;
                
                echo "\rNext check in: {$minutes}m {$seconds}s" . str_repeat(" ", 20);
                
                // Sleep in smaller intervals to check for signals
                for ($j = 0; $j < $checkInterval; $j++) {
                    if (isset($shutdownRequested) && $shutdownRequested) {
                        break 3; // Break out of all loops
                    }
                    sleep(1);
                }
            }
            
            echo "\r" . str_repeat(" ", 50) . "\r";
            
            // Quick check for any new releases before next run
            echo "Idle period complete, checking for new releases...\n"; // Changed from apiLog() to echo
        }
        
        // Memory management
        if (function_exists('gc_mem_caches')) {
            gc_mem_caches();
        }
        
        // Limit memory usage
        $memoryUsage = round(memory_get_usage(true) / 1024 / 1024, 2);
        if ($memoryUsage > 256) { // If using more than 256MB
            echo "Memory usage high ({$memoryUsage}MB), restarting script...\n";
            // Using echo instead of apiLog() here too
            break;
        }
    }
    
    // Final statistics
    $finalStats = $updater->getStatistics();
    if ($finalStats) {
        echo "\n" . str_repeat("=", 60) . "\n";
        echo "FINAL STATISTICS\n";
        echo str_repeat("=", 60) . "\n";
        echo "Total runs: {$runCount}\n";
        echo "Total releases: {$finalStats['total_releases']}\n";
        echo "With genre: {$finalStats['with_genre']} ({$finalStats['completion_percentage']}%)\n";
        echo "Eligible without genre: {$finalStats['eligible_without_genre']}\n";
        echo str_repeat("=", 60) . "\n";
    }
    
    echo "\n👋 Script terminated gracefully at " . date('Y-m-d H:i:s') . "\n";
    
} catch (Exception $e) {
    error_log("Genre Updater Fatal Error: " . $e->getMessage());
    echo "\n❌ Fatal Error: " . $e->getMessage() . "\n";
    exit(1);
}
?>