<?php
/**
 * ThePornDB API Integration
 * Fetches and updates URL for XXX releases
 * Similar structure to theporndb.net updater
 * 
 * Improvements:
 * - Auto-learning performer/studio mappings
 * - Multi-strategy search (parse, performer+site, date range, title)
 * - Fuzzy title matching
 * - Exponential backoff retry logic
 * - Result caching with negative result tracking
 * - Query sanitization for API compatibility
 * - Performer name variation generation
 * - Enhanced scoring with tag matching
 * - Dry-run mode protection (no mappings saved in dry-run)
 */

// Configuration
$verbose = false; // Set to false when running as systemd service to avoid double logging
$logDir = ($_SERVER['HOME'] ?? '/home/' . get_current_user()) . '/apps/theporndb.net/logs';
$batchSize = 100;
$idleTime = 300; // 5 minutes
$checkInterval = 60; // Check every minute during idle
$dryRun = false; // SET TO true FOR DRY RUN MODE
// DEBUG: Check if we're being included or run twice
error_log("SCRIPT START - PID: " . getmypid() . " - Time: " . microtime(true));
// END

class ThePornDBUpdater {
    private $db;
    private $apiBaseUrl = 'https://api.theporndb.net';
    private $maxRetries = 3;
    private $retryDelay = 2;
    private $logDir;
    private $configDir;
    private $apiLogFile;
    private $statsLogFile;
    private $verbose;
    private $processedReleasesFile;
    private $rateLimitDelay = 2; // Be nice to the API
    private $minScore = 60; // Minimum score to consider a match valid
    private $dryRun = false; // Dry run mode - don't actually update database
    private $minQueryLength = 3; // Minimum search query length
    private $noResultsCache = []; // Cache queries that returned no results
    
    // Auto-learning mappings
    private $performerMappings = [];
    private $studioMappingsLearned = []; // NEW: Track learned studios separately
    private $mappingsFile;
    private $searchCache = [];
    private $cacheMaxSize = 1000;
    
    // Studio name mappings for better matching
    private $studioMappings = [
        // VR Studios
        'MilfVR' => ['MILF VR', 'MilfVR', 'Milf VR'],
        'WankzVR' => ['WANKZ VR', 'WankzVR', 'Wankz VR'],
        'VRBangers' => ['VR Bangers', 'VRBangers'],
        'BaDoinkVR' => ['BaDoink VR', 'BaDoinkVR'],
        'RealityLovers' => ['Reality Lovers', 'RealityLovers'],
        'VirtualRealPorn' => ['Virtual Real Porn', 'VirtualRealPorn', 'VRP'],
        'CzechVR' => ['Czech VR', 'CzechVR'],
        'VRCosplayX' => ['VR Cosplay X', 'VRCosplayX', 'VRCosplay X'],
        'SexBabesVR' => ['Sex Babes VR', 'SexBabesVR'],
        'VRLatina' => ['VR Latina', 'VRLatina'],
        'StockingsVR' => ['Stockings VR', 'StockingsVR'],
        'LethalHardcoreVR' => ['Lethal Hardcore VR', 'LethalHardcoreVR'],
        '18VR' => ['18 VR', '18VR'],
        'POVR' => ['POV R', 'POVR', 'POV VR'],
        'VRHush' => ['VR Hush', 'VRHush'],
        'SLROriginals' => ['SLR Originals', 'SLROriginals'],
        'VRConk' => ['VR Conk', 'VRConk'],
        
        // Brazzers Network
        'BrazzersExxtra' => ['Brazzers Exxtra', 'Brazzers', 'BrazzersExxtra'],
        'BigWetButts' => ['Big Wet Butts', 'BigWetButts'],
        'BigTitsAtSchool' => ['Big Tits At School', 'BigTitsAtSchool'],
        'BigTitsAtWork' => ['Big Tits At Work', 'BigTitsAtWork'],
        'BigTitsInSports' => ['Big Tits In Sports', 'BigTitsInSports'],
        'BigTitsRoundAsses' => ['Big Tits Round Asses', 'BigTitsRoundAsses'],
        'DoctorAdventures' => ['Doctor Adventures', 'DoctorAdventures'],
        'HotAndMean' => ['Hot And Mean', 'HotAndMean'],
        'MilfsLikeItBig' => ['Milfs Like It Big', 'MilfsLikeItBig'],
        'MomsInControl' => ['Moms In Control', 'MomsInControl'],
        'PornstarsLikeItBig' => ['Pornstars Like It Big', 'PornstarsLikeItBig'],
        'RealWifeStories' => ['Real Wife Stories', 'RealWifeStories'],
        'ShesGonnaSquirt' => ['Shes Gonna Squirt', 'ShesGonnaSquirt'],
        'TeensLikeItBig' => ['Teens Like It Big', 'TeensLikeItBig'],
        'ZZSeries' => ['ZZ Series', 'ZZSeries'],
        
        // Reality Kings
        'RealityKings' => ['Reality Kings', 'RealityKings'],
        'MikeInBrazil' => ['Mike In Brazil', 'MikeInBrazil'],
        'MonsterCurves' => ['Monster Curves', 'MonsterCurves'],
        'MomsLickTeens' => ['Moms Lick Teens', 'MomsLickTeens'],
        'WeLiveTogether' => ['We Live Together', 'WeLiveTogether'],
        'MomsBangTeens' => ['Moms Bang Teens', 'MomsBangTeens'],
        'StreetBlowjobs' => ['Street Blowjobs', 'StreetBlowjobs'],
        'EuroSexParties' => ['Euro Sex Parties', 'EuroSexParties'],
        'FirstTimeAuditions' => ['First Time Auditions', 'FirstTimeAuditions'],
        '8thStreetLatinas' => ['8th Street Latinas', '8thStreetLatinas'],
        
        // Naughty America
        'NaughtyAmerica' => ['Naughty America', 'NaughtyAmerica'],
        'MyFirstSexTeacher' => ['My First Sex Teacher', 'MyFirstSexTeacher'],
        'MyFriendsHotMom' => ['My Friends Hot Mom', 'MyFriendsHotMom'],
        'MyDadsHotGirlfriend' => ['My Dads Hot Girlfriend', 'MyDadsHotGirlfriend'],
        'MySistersHotFriend' => ['My Sisters Hot Friend', 'MySistersHotFriend'],
        'NeighborAffair' => ['Neighbor Affair', 'NeighborAffair'],
        'DiaryOfANanny' => ['Diary Of A Nanny', 'DiaryOfANanny'],
        'FastTimes' => ['Fast Times', 'FastTimes'],
        'LatinAdultery' => ['Latin Adultery', 'LatinAdultery'],
        'SeducedByACougar' => ['Seduced By A Cougar', 'SeducedByACougar'],
        'ThunderCock' => ['Thunder Cock', 'ThunderCock'],
        'TonightsGirlfriend' => ['Tonights Girlfriend', 'TonightsGirlfriend'],
        
        // Team Skeet Network
        'TeamSkeet' => ['Team Skeet', 'TeamSkeet'],
        'ExxxtraSmall' => ['Exxxtra Small', 'ExxxtraSmall'],
        'TeenPies' => ['Teen Pies', 'TeenPies'],
        'BadMilfs' => ['Bad Milfs', 'BadMilfs'],
        'DadCrush' => ['Dad Crush', 'DadCrush'],
        'DaughterSwap' => ['Daughter Swap', 'DaughterSwap'],
        'StepSiblings' => ['Step Siblings', 'StepSiblings'],
        'SisLovesMe' => ['Sis Loves Me', 'SisLovesMe'],
        'MyBabysittersClub' => ['My Babysitters Club', 'MyBabysittersClub'],
        'InnocentHigh' => ['Innocent High', 'InnocentHigh'],
        
        // FakeHub Network
        'FakehubOriginals' => ['Fakehub Originals', 'FakeHub Originals', 'Fake Hub'],
        'FakeTaxi' => ['Fake Taxi', 'FakeTaxi'],
        'FakeAgent' => ['Fake Agent', 'FakeAgent'],
        'FakeAgentUK' => ['Fake Agent UK', 'FakeAgentUK'],
        'FakeHospital' => ['Fake Hospital', 'FakeHospital'],
        'FakeCop' => ['Fake Cop', 'FakeCop'],
        'FakeDrivingSchool' => ['Fake Driving School', 'FakeDrivingSchool'],
        'FakeHostel' => ['Fake Hostel', 'FakeHostel'],
        'PublicAgent' => ['Public Agent', 'PublicAgent'],
        'FemaleAgent' => ['Female Agent', 'FemaleAgent'],
        'FakeShooting' => ['Fake Shooting', 'FakeShooting'],
        
        // Bang Bros
        'BangBros' => ['Bang Bros', 'BangBros'],
        'AssParade' => ['Ass Parade', 'AssParade'],
        'BackroomMILF' => ['Backroom MILF', 'BackroomMILF'],
        'BangBus' => ['Bang Bus', 'BangBus'],
        'BigMouthfuls' => ['Big Mouthfuls', 'BigMouthfuls'],
        'BigTitCreampie' => ['Big Tit Creampie', 'BigTitCreampie'],
        'BrownBunnies' => ['Brown Bunnies', 'BrownBunnies'],
        'MonstersOfCock' => ['Monsters Of Cock', 'MonstersOfCock'],
        'MILFLessons' => ['MILF Lessons', 'MILFLessons'],
        'PublicBang' => ['Public Bang', 'PublicBang'],
        
        // Evil Angel
        'EvilAngel' => ['Evil Angel', 'EvilAngel'],
        'LeWood' => ['LeWood', 'Le Wood'],
        'JulesJordan' => ['Jules Jordan', 'JulesJordan'],
        'ArchAngelVideo' => ['ArchAngel Video', 'ArchAngelVideo'],
        
        // Vixen Network
        'Vixen' => ['Vixen'],
        'Blacked' => ['Blacked'],
        'BlackedRaw' => ['Blacked Raw', 'BlackedRaw'],
        'Tushy' => ['Tushy'],
        'TushyRaw' => ['Tushy Raw', 'TushyRaw'],
        'Deeper' => ['Deeper'],
        'Slayed' => ['Slayed'],
        
        // Digital Playground
        'DigitalPlayground' => ['Digital Playground', 'DigitalPlayground'],
        
        // Mofos
        'Mofos' => ['Mofos'],
        'PublicPickups' => ['Public Pickups', 'PublicPickups'],
        'PervsOnPatrol' => ['Pervs On Patrol', 'PervsOnPatrol'],
        'DontBreakMe' => ['Dont Break Me', 'DontBreakMe'],
        'ShesBrandNew' => ['Shes Brand New', 'ShesBrandNew'],
        'IKnowThatGirl' => ['I Know That Girl', 'IKnowThatGirl'],
        'MilfsLikeItBlack' => ['Milfs Like It Black', 'MilfsLikeItBlack'],
        'ShareMyBF' => ['Share My BF', 'ShareMyBF'],
        'StrandedTeens' => ['Stranded Teens', 'StrandedTeens'],
        
        // Pure Taboo & Adult Time
        'PureTaboo' => ['Pure Taboo', 'PureTaboo'],
        'AdultTime' => ['Adult Time', 'AdultTime'],
        'GirlsWay' => ['Girls Way', 'GirlsWay'],
        'ModernDaySins' => ['Modern Day Sins', 'ModernDaySins'],
        'AllHerLuv' => ['All Her Luv', 'AllHerLuv'],
        'MissaX' => ['MissaX', 'Missa X'],
        
        // Hustler
        'Hustler' => ['Hustler'],
        
        // Wicked Pictures
        'WickedPictures' => ['Wicked Pictures', 'WickedPictures'],
        
        // 21Sextury Network
        '21Sextury' => ['21 Sextury', '21Sextury'],
        '21Naturals' => ['21 Naturals', '21Naturals'],
        '21FootArt' => ['21 Foot Art', '21FootArt'],
        'AssholeFever' => ['Asshole Fever', 'AssholeFever'],
        'ClubSandy' => ['Club Sandy', 'ClubSandy'],
        'DPFanatics' => ['DP Fanatics', 'DPFanatics'],
        'Gapeland' => ['Gapeland'],
        'GrandpasFuckTeens' => ['Grandpas Fuck Teens', 'GrandpasFuckTeens'],
        'LezCuties' => ['Lez Cuties', 'LezCuties'],
        'TeachMeFisting' => ['Teach Me Fisting', 'TeachMeFisting'],
        
        // Twistys
        'Twistys' => ['Twistys'],
        'TwistysHard' => ['Twistys Hard', 'TwistysHard'],
        'MomKnowsBest' => ['Mom Knows Best', 'MomKnowsBest'],
        'WhenGirlsPlay' => ['When Girls Play', 'WhenGirlsPlay'],
        
        // DDF Network
        'DDFNetwork' => ['DDF Network', 'DDFNetwork'],
        'HandsOnHardcore' => ['Hands On Hardcore', 'HandsOnHardcore'],
        'HotLegsAndFeet' => ['Hot Legs And Feet', 'HotLegsAndFeet'],
        'OnlyBlowjob' => ['Only Blowjob', 'OnlyBlowjob'],
        'EuroBabesHD' => ['Euro Babes HD', 'EuroBabesHD'],
        
        // Penthouse
        'Penthouse' => ['Penthouse'],
        'PenthouseGold' => ['Penthouse Gold', 'PenthouseGold'],
        
        // Property Sex
        'PropertySex' => ['Property Sex', 'PropertySex'],
        
        // Passion HD
        'PassionHD' => ['Passion HD', 'PassionHD'],
        'Lubed' => ['Lubed'],
        'Exotic4K' => ['Exotic 4K', 'Exotic4K'],
        'Tiny4K' => ['Tiny 4K', 'Tiny4K'],
        'POVD' => ['POV D', 'POVD'],
        'CastingCouchX' => ['Casting Couch X', 'CastingCouchX'],
        'PureMature' => ['Pure Mature', 'PureMature'],
        'FantasyHD' => ['Fantasy HD', 'FantasyHD'],
        'MassageCreep' => ['Massage Creep', 'MassageCreep'],
        
        // Nubiles Network
        'Nubiles' => ['Nubiles'],
        'NubileFilms' => ['Nubile Films', 'NubileFilms'],
        'NubilesET' => ['Nubiles ET', 'NubilesET'],
        'NFBusty' => ['NF Busty', 'NFBusty'],
        'PetiteHDPorn' => ['Petite HD Porn', 'PetiteHDPorn'],
        'StepSiblingsCaught' => ['Step Siblings Caught', 'StepSiblingsCaught'],
        'BrattySis' => ['Bratty Sis', 'BrattySis'],
        'MomsTeachSex' => ['Moms Teach Sex', 'MomsTeachSex'],
        'MyFamilyPies' => ['My Family Pies', 'MyFamilyPies'],
        'PrincessCum' => ['Princess Cum', 'PrincessCum'],
        
        // Misc Popular Studios
        'MYLKED' => ['MYLKED'],
        'GilfAF' => ['GilfAF', 'Gilf AF'],
        'SeeHimFuck' => ['See Him Fuck', 'SeeHimFuck'],
        'Swallowed' => ['Swallowed'],
        'AllAnal' => ['All Anal', 'AllAnal'],
        'TrueAnal' => ['True Anal', 'TrueAnal'],
        'SpyFam' => ['Spy Fam', 'SpyFam'],
        'EroticaX' => ['Erotica X', 'EroticaX'],
        'HardX' => ['Hard X', 'HardX'],
        'DarkX' => ['Dark X', 'DarkX'],
        'LesbianX' => ['Lesbian X', 'LesbianX'],
        'NewSensations' => ['New Sensations', 'NewSensations'],
        'SweetSinner' => ['Sweet Sinner', 'SweetSinner'],
        'ZeroTolerance' => ['Zero Tolerance', 'ZeroTolerance'],
        
        // Czech Network
        'CzechCasting' => ['Czech Casting', 'CzechCasting'],
        'CzechMassage' => ['Czech Massage', 'CzechMassage'],
        'CzechStreets' => ['Czech Streets', 'CzechStreets'],
        'CzechFantasy' => ['Czech Fantasy', 'CzechFantasy'],
        'CzechGangBang' => ['Czech Gang Bang', 'CzechGangBang'],
        'CzechSolarium' => ['Czech Solarium', 'CzechSolarium'],
        
        // Trans Studios
        'TransAngels' => ['Trans Angels', 'TransAngels'],
        'TSPlayground' => ['TS Playground', 'TSPlayground'],
        'EvilAngelTS' => ['Evil Angel TS', 'EvilAngelTS'],
        
        // Amateur/Casting
        'BackroomCastingCouch' => ['Backroom Casting Couch', 'BackroomCastingCouch'],
        'NetVideoGirls' => ['Net Video Girls', 'NetVideoGirls'],
        'ExploitedCollegeGirls' => ['Exploited College Girls', 'ExploitedCollegeGirls'],
        
        // Japanese Studios
        'Caribbeancom' => ['Caribbeancom', 'Caribbean Com'],
        '1Pondo' => ['1Pondo', '1 Pondo'],
        'TokyoHot' => ['Tokyo Hot', 'TokyoHot'],
        'HEYZO' => ['HEYZO', 'Heyzo'],
        'Pacopacomama' => ['Pacopacomama'],
        'Muramura' => ['Muramura'],
    ];
    
    public function __construct($config = [], $verbose = true, $logDir = null, $configDir = null, $dryRun = false) {
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
        $this->dryRun = $dryRun;
        
        // Ensure log directory exists
        if (!is_dir($this->logDir)) {
            mkdir($this->logDir, 0755, true);
        }
        
        // Set up log files
        $this->apiLogFile = $this->logDir . '/tpdb_updater.log';
        $this->statsLogFile = $this->logDir . '/tpdb_stats.log';
        $this->processedReleasesFile = $this->logDir . '/tpdb_processed.json';
        
        // In dry run mode, use separate files
        if ($this->dryRun) {
            $this->apiLogFile = $this->logDir . '/tpdb_updater_dryrun.log';
            $this->processedReleasesFile = $this->logDir . '/tpdb_processed_dryrun.json';
        }
        
        // Initialize auto-learning system
        $this->mappingsFile = $this->configDir . '/learned_mappings.json';
        $this->loadLearnedMappings();
        
        // Get database credentials from config
        $dbHost = $config['db']['host'] ?? 'localhost';
        $dbUser = $config['db']['user'] ?? '';
        $dbPass = $config['db']['pass'] ?? '';
        $dbName = $config['db']['name'] ?? '';
        
        $modeStr = $this->dryRun ? "DRY RUN MODE" : "LIVE MODE";
        $this->apiLog("=== ThePornDB Updater Started [{$modeStr}] ===");
        $this->apiLog("Database: {$dbName}@{$dbHost}");
        
        if ($this->dryRun) {
            $this->apiLog("⚠️  DRY RUN MODE - No database changes will be made!");
            $this->apiLog("⚠️  Using separate log files: *_dryrun.log");
        }
        
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
     * Load learned mappings from file
     */
    private function loadLearnedMappings() {
        if (file_exists($this->mappingsFile)) {
            $data = json_decode(file_get_contents($this->mappingsFile), true);
            if (is_array($data)) {
                $this->performerMappings = $data['performers'] ?? [];
                $this->studioMappingsLearned = $data['studios'] ?? [];
                
                // Merge learned studios with static ones (learned take precedence)
                foreach ($this->studioMappingsLearned as $canonical => $variations) {
                    if (!isset($this->studioMappings[$canonical])) {
                        $this->studioMappings[$canonical] = $variations;
                    } else {
                        // Merge variations, keeping unique values
                        $this->studioMappings[$canonical] = array_unique(array_merge(
                            $this->studioMappings[$canonical],
                            $variations
                        ));
                    }
                }
                
                $this->apiLog("Loaded " . count($this->performerMappings) . " learned performer mappings");
                $this->apiLog("Loaded " . count($this->studioMappingsLearned) . " learned studio mappings");
            }
        }
    }
    
    /**
     * Save learned mappings to file
     */
    private function saveLearnedMappings() {
        if ($this->dryRun) {
            return; // Don't save in dry-run mode
        }
        
        $data = [
            'performers' => $this->performerMappings,
            'studios' => $this->studioMappingsLearned,
            'last_updated' => date('Y-m-d H:i:s')
        ];
        
        if (!is_dir($this->configDir)) {
            mkdir($this->configDir, 0755, true);
        }
        
        file_put_contents($this->mappingsFile, json_encode($data, JSON_PRETTY_PRINT));
    }
    
    /**
     * Learn mappings from successful matches
     */
    private function learnFromMatch($parsedPerformer, $apiPerformers, $parsedStudio = null, $apiSite = null) {
        // Learn performer mapping
        if (!empty($parsedPerformer) && !empty($apiPerformers)) {
            $parsedNormalized = strtolower(str_replace(['.', '_'], ' ', $parsedPerformer));
            
            foreach ($apiPerformers as $apiPerformer) {
                $apiName = $apiPerformer['name'] ?? '';
                if (empty($apiName)) continue;
                
                $apiLower = strtolower($apiName);
                
                // If not already learned and similar enough
                if (!isset($this->performerMappings[$parsedPerformer])) {
                    similar_text($parsedNormalized, $apiLower, $sim);
                    
                    if ($sim > 60 || strpos($apiLower, $parsedNormalized) !== false || 
                        strpos($parsedNormalized, $apiLower) !== false) {
                        $this->performerMappings[$parsedPerformer] = $apiName;
                        $this->saveLearnedMappings();
                        $this->apiLog("🧠 Learned performer mapping: '$parsedPerformer' => '$apiName'");
                        break;
                    }
                }
            }
        }
        
        // NEW: Learn studio mapping
        if (!empty($parsedStudio) && !empty($apiSite)) {
            $this->learnStudioMapping($parsedStudio, $apiSite);
        }
    }
    
    /**
     * NEW: Learn studio mapping from successful match
     */
    private function learnStudioMapping($parsedStudio, $apiSite) {
        // Check if we already know this mapping
        if (isset($this->studioMappingsLearned[$parsedStudio])) {
            // Check if API site is already in our variations
            if (!in_array($apiSite, $this->studioMappingsLearned[$parsedStudio])) {
                $this->studioMappingsLearned[$parsedStudio][] = $apiSite;
                $this->saveLearnedMappings();
                $this->apiLog("🧠 Updated studio mapping: '$parsedStudio' => added '$apiSite'");
            }
            return;
        }
        
        // Check if it matches any existing static mapping
        $foundCanonical = null;
        foreach ($this->studioMappings as $canonical => $variations) {
            if (in_array($apiSite, $variations) || $apiSite === $canonical) {
                $foundCanonical = $canonical;
                break;
            }
        }
        
        if ($foundCanonical) {
            // Add to existing canonical mapping
            if (!in_array($parsedStudio, $this->studioMappings[$foundCanonical])) {
                $this->studioMappings[$foundCanonical][] = $parsedStudio;
                
                // Also track in learned mappings for persistence
                if (!isset($this->studioMappingsLearned[$foundCanonical])) {
                    $this->studioMappingsLearned[$foundCanonical] = [$parsedStudio];
                } else {
                    $this->studioMappingsLearned[$foundCanonical][] = $parsedStudio;
                }
                
                $this->saveLearnedMappings();
                $this->apiLog("🧠 Learned studio variation: '$parsedStudio' => '$foundCanonical' (via '$apiSite')");
            }
        } else {
            // Completely new studio - create new mapping
            $this->studioMappingsLearned[$parsedStudio] = [$parsedStudio, $apiSite];
            $this->studioMappings[$parsedStudio] = [$parsedStudio, $apiSite];
            $this->saveLearnedMappings();
            $this->apiLog("🧠 Learned NEW studio: '$parsedStudio' => '$apiSite'");
        }
    }
    
    /**
     * Log API-related messages
     */
    private function apiLog($message, $level = 'INFO') {
        $timestamp = date('Y-m-d H:i:s');
        $prefix = $this->dryRun ? '[DRY-RUN]' : '';
        $logMessage = "[{$timestamp}] {$prefix}[{$level}] {$message}\n";
        
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
        $prefix = $this->dryRun ? '[DRY-RUN]' : '';
        $logMessage = "[{$timestamp}] {$prefix} {$message}\n";
        
        file_put_contents($this->statsLogFile, $logMessage, FILE_APPEND | LOCK_EX);
    }
    
    /**
     * Get API key from config file
     */
    private function getApiKey() {
        $configFile = $this->configDir . '/config.php';
        
        if (file_exists($configFile)) {
            $config = require $configFile;
            if (isset($config['tpdb_api_key']) && !empty($config['tpdb_api_key'])) {
                return $config['tpdb_api_key'];
            }
        }
        
        // Fallback: try environment variable
        $envKey = getenv('TPDB_API_KEY');
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
    private function markReleaseAsProcessed($releaseId, $releaseName, $tpdbUrl = null, $score = null, $status = 'SUCCESS') {
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
                'url' => $tpdbUrl,
                'score' => $score,
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
     * Get XXX releases without URL
     */
    public function getReleasesWithoutURL($limit = 100) {
        $this->apiLog("Searching for up to {$limit} XXX releases without URL...");
        
        $releases = [];
        $processedIds = $this->getProcessedReleaseIds();
        
        // Base query - looking for XXX releases without URL
        $query = "SELECT id, releasename FROM releases 
                WHERE (url IS NULL OR url = '') 
                AND releasename LIKE '%.XXX.%'
                AND releasename NOT LIKE '%iMAGESET%'
                AND releasename NOT LIKE '%IMAGESET%'";
        
        // Handle processed IDs
        if (!empty($processedIds)) {
            $this->apiLog("Excluding " . count($processedIds) . " already processed releases");
            
            // For very large lists, limit exclusion to recent ones
            if (count($processedIds) > 10000) {
                $recentProcessedIds = array_slice($processedIds, -10000);
                $placeholders = implode(',', array_fill(0, min(10000, count($recentProcessedIds)), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                $bindIds = $recentProcessedIds;
            } else {
                $placeholders = implode(',', array_fill(0, count($processedIds), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                $bindIds = $processedIds;
            }
            
            $query .= " ORDER BY id DESC LIMIT ?";
            
            $stmt = $this->db->prepare($query);
            if (!$stmt) {
                $this->apiLog("Failed to prepare query: " . $this->db->error, 'ERROR');
                return [];
            }
            
            $types = str_repeat('i', count($bindIds)) . 'i';
            $bindParams = array_merge($bindIds, [$limit]);
            $stmt->bind_param($types, ...$bindParams);
            
            $stmt->execute();
            $result = $stmt->get_result();
        } else {
            $query .= " ORDER BY id DESC LIMIT ?";
            $stmt = $this->db->prepare($query);
            $stmt->bind_param('i', $limit);
            $stmt->execute();
            $result = $stmt->get_result();
        }
        
        while ($row = $result->fetch_assoc()) {
            $releases[] = [
                'id' => $row['id'],
                'releasename' => $row['releasename']
            ];
        }
        
        $count = count($releases);
        $this->apiLog("Found {$count} releases needing URL update");
        
        if (isset($stmt)) {
            $stmt->close();
        }
        
        return $releases;
    }
    
    /**
     * Parse release name to extract studio, date, and performer
     */
    private function parseReleaseName($release) {
        // Normalize separators - handle both dots and underscores
        $normalized = str_replace(['_', '-'], '.', $release);
        $parts = explode('.', $normalized);

        // Clean up studio name (remove .com, .net, etc.)
        $studio = $parts[0] ?? '';
        $studio = preg_replace('/\.(com|net|org|co\.uk)$/i', '', $studio);

        // Find date position - look for YY.MM.DD pattern
        $date = null;
        $dateIndex = -1;

        for ($i = 1; $i < count($parts) - 2; $i++) {
            $year = intval($parts[$i]);
            $month = intval($parts[$i + 1]);
            $day = intval($parts[$i + 2]);

            // Check if this looks like a date (YY.MM.DD)
            if ($year >= 0 && $year <= 99 && $month >= 1 && $month <= 12 && $day >= 1 && $day <= 31) {
                // Handle 2-digit year
                if ($year < 50) {
                    $year += 2000;
                } else {
                    $year += 1900;
                }

                $date = sprintf("%04d-%02d-%02d", $year, $month, $day);
                $dateIndex = $i;
                break;
            }
        }

        // If no date found, try the old method (parts 1,2,3)
        if ($date === null && count($parts) >= 4) {
            try {
                $year = intval($parts[1]);
                $month = intval($parts[2]);
                $day = intval($parts[3]);

                if ($year > 0 && $month > 0 && $day > 0) {
                    if ($year < 50) {
                        $year += 2000;
                    } else {
                        $year += 1900;
                    }
                    $date = sprintf("%04d-%02d-%02d", $year, $month, $day);
                    $dateIndex = 1;
                }
            } catch (Exception $e) {
                // Date parsing failed
            }
        }

        // Extract performer and title
        $performer = '';
        $title = '';
        $stopWords = ['XXX', 'VR180', '1080p', '2160p', '3600p', '4K', '720p', 'MP4', 'WEB', 'HEVC', 'H264', 'x264', 'x265', 'RAR', 'ZIP', 'iMAGESET', 'IMAGESET', 'PNG', 'JPG'];

        if ($dateIndex > 0) {
            // Content starts after date (3 parts for date)
            $contentStart = $dateIndex + 3;

            // Find where stop words begin
            $stopIndex = null;
            for ($i = $contentStart; $i < count($parts); $i++) {
                if (in_array(strtoupper($parts[$i]), $stopWords)) {
                    $stopIndex = $i;
                    break;
                }
            }

            if ($stopIndex === null) {
                $stopIndex = count($parts);
            }

            // Extract content parts
            $contentParts = array_slice($parts, $contentStart, $stopIndex - $contentStart);
            $contentCount = count($contentParts);

            if ($contentCount == 0) {
                $performer = '';
                $title = '';
            } elseif ($contentCount <= 2) {
                // Likely just performer name
                $performer = implode(' ', $contentParts);
            } elseif ($contentCount == 3) {
                // Could be "First Last Middle" or "First Last" + 1 word title
                $performer = implode(' ', array_slice($contentParts, 0, 2));
                $title = $contentParts[2];
            } else {
                // Performer is first 1-2 words, rest is title
                $performer = implode(' ', array_slice($contentParts, 0, 2));
                $title = implode(' ', array_slice($contentParts, 2));
            }
        }

        // Clean up performer name
        $performer = preg_replace('/^(Mrs|Mr|Ms|Dr|Miss)\.?\s*/i', '', $performer);
        $performer = trim($performer);

        return [
            'studio' => $studio,
            'date' => $date,
            'performer' => $performer,
            'title' => $title,
            'raw' => $release
        ];
    }
    
    /**
     * Normalize studio name with learned mappings
     */
    private function normalizeStudioName($studio) {
        // Check learned mappings first (they take precedence)
        if (isset($this->studioMappingsLearned[$studio])) {
            return $this->studioMappingsLearned[$studio];
        }
        
        // Fall back to static mappings
        foreach ($this->studioMappings as $canonical => $variations) {
            if (in_array($studio, $variations) || $studio === $canonical) {
                return $variations;
            }
        }
        
        // Return as-is if no mapping found
        return [$studio];
    }
    
    /**
     * Sanitize search query for API compatibility
     */
    private function sanitizeSearchQuery($query) {
        // Remove special characters that might break API search
        $query = preg_replace('/[^\w\s\-]/', ' ', $query);
        // Collapse multiple spaces
        $query = preg_replace('/\s+/', ' ', $query);
        return trim($query);
    }
    
    /**
     * Normalize performer name with auto-learning support
     */
    private function normalizePerformerName($performer) {
        // Check learned mappings first
        if (isset($this->performerMappings[$performer])) {
            return $this->performerMappings[$performer];
        }
        
        // Apply basic normalization
        $normalized = $performer;
        
        // Convert CamelCase to spaces (e.g., "AbellaDanger" -> "Abella Danger")
        $normalized = preg_replace('/([a-z])([A-Z])/', '$1 $2', $normalized);
        
        // Convert dots/underscores to spaces
        $normalized = str_replace(['.', '_'], ' ', $normalized);
        
        return trim($normalized);
    }
    
    /**
     * Get variations of performer name for better matching
     */
    private function getPerformerVariations($performer) {
        if (empty($performer)) {
            return [];
        }
        
        $variations = [$performer];
        
        // Add normalized version
        $normalized = $this->normalizePerformerName($performer);
        if ($normalized !== $performer) {
            $variations[] = $normalized;
        }
        
        // Handle "FirstLast" -> "First Last" and vice versa
        if (strpos($performer, ' ') === false) {
            // No space, add CamelCase split
            $camelSplit = preg_replace('/([a-z])([A-Z])/', '$1 $2', $performer);
            if ($camelSplit !== $performer) {
                $variations[] = $camelSplit;
            }
        } else {
            // Has space, add joined version
            $variations[] = str_replace(' ', '', $performer);
        }
        
        // Handle common suffixes (Jr, Sr, II, III, IV)
        $noSuffix = preg_replace('/\s+(Jr|Sr|II|III|IV)\.?$/i', '', $performer);
        if ($noSuffix !== $performer) {
            $variations[] = $noSuffix;
        }
        
        return array_unique(array_filter($variations));
    }
    
    /**
     * Calculate similarity between two strings
     */
    private function similarityRatio($str1, $str2) {
        similar_text(strtolower($str1), strtolower($str2), $percent);
        return $percent / 100;
    }
    
    /**
     * Extract potential title from release name
     */
    private function extractTitleFromRelease($releaseName, $parsedInfo) {
        // Remove studio
        $clean = preg_replace('/^' . preg_quote($parsedInfo['studio'], '/') . '\./i', '', $releaseName);
        
        // Remove date (YY.MM.DD format)
        $clean = preg_replace('/\d{2}\.\d{2}\.\d{2}\./', '', $clean);
        
        // Remove performer
        if (!empty($parsedInfo['performer'])) {
            $performerVariations = [
                $parsedInfo['performer'],
                str_replace(' ', '.', $parsedInfo['performer']),
                str_replace(' ', '_', $parsedInfo['performer'])
            ];
            foreach ($performerVariations as $variation) {
                $clean = str_ireplace($variation . '.', '', $clean);
            }
        }
        
        // Remove technical tags
        $technicalTags = ['XXX', 'VR180', '1080p', '2160p', '3600p', '4K', '720p', 
                          'MP4', 'WEB', 'HEVC', 'H264', 'x264', 'x265', 'RAR', 'ZIP'];
        foreach ($technicalTags as $tag) {
            $clean = str_ireplace($tag, '', $clean);
        }
        
        return trim(str_replace('.', ' ', $clean));
    }
    
    /**
     * Make API request with exponential backoff and caching
     */
    private function makeApiRequest($url, $apiKey, $maxRetries = 3) {
        // Check cache first
        $cacheKey = md5($url);
        $cached = $this->getCachedSearch($cacheKey);
        if ($cached !== null) {
            $this->apiLog("Cache hit for: " . substr($url, 0, 100) . "...");
            return $cached;
        }
        
        $attempt = 0;
        $lastError = null;
        
        while ($attempt < $maxRetries) {
            try {
                $ch = curl_init();
                curl_setopt_array($ch, [
                    CURLOPT_URL => $url,
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_HTTPHEADER => [
                        'Authorization: Bearer ' . $apiKey,
                        'Accept: application/json',
                        'Content-Type: application/json'
                    ],
                    CURLOPT_TIMEOUT => 30,
                    CURLOPT_FOLLOWLOCATION => true,
                    CURLOPT_SSL_VERIFYPEER => true,
                    CURLOPT_USERAGENT => 'PreDB-TPDb-Updater/1.1',
                    CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1  // Force HTTP/1.1 to avoid HTTP/2 protocol errors
                ]);
                
                $response = curl_exec($ch);
                $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                $error = curl_error($ch);
                curl_close($ch);
                
                if ($error) {
                    throw new Exception("cURL Error: $error");
                }
                
                if ($httpCode === 429) {
                    // Rate limited - wait longer
                    $waitTime = pow(2, $attempt) * 5; // 5, 10, 20 seconds
                    $this->apiLog("Rate limited, waiting {$waitTime}s...", 'WARNING');
                    sleep($waitTime);
                    $attempt++;
                    continue;
                }
                
                if ($httpCode !== 200) {
                    throw new Exception("HTTP $httpCode");
                }
                
                $data = json_decode($response, true);
                if (json_last_error() !== JSON_ERROR_NONE) {
                    throw new Exception("JSON decode error: " . json_last_error_msg());
                }
                
                $results = $data['data'] ?? [];
                
                // Cache successful results
                $this->setCachedSearch($cacheKey, $results);
                
                return $results;
                
            } catch (Exception $e) {
                $lastError = $e;
                $attempt++;
                if ($attempt < $maxRetries) {
                    $backoff = pow(2, $attempt); // Exponential backoff: 2, 4, 8 seconds
                    $this->apiLog("Attempt $attempt failed: " . $e->getMessage() . ", retrying in {$backoff}s...", 'WARNING');
                    sleep($backoff);
                }
            }
        }
        
        $this->apiLog("All attempts failed: " . $lastError->getMessage(), 'ERROR');
        return [];
    }
    
    /**
     * Search cache methods
     */
    private function getCachedSearch($key) {
        return $this->searchCache[$key] ?? null;
    }
    
    private function setCachedSearch($key, $results) {
        if (count($this->searchCache) >= $this->cacheMaxSize) {
            // Remove oldest 10% of entries
            $this->searchCache = array_slice($this->searchCache, intval($this->cacheMaxSize * 0.1), null, true);
        }
        $this->searchCache[$key] = $results;
    }
    
    /**
     * Search by parse endpoint (treats input as filename)
     */
    
    
    
    private function searchScene($studio, $performer, $date = null, $rawRelease = null) {
        $apiKey = $this->getApiKey();
        if (!$apiKey) {
            $this->apiLog("No API key found! Check config.php or TPDB_API_KEY environment variable", 'ERROR');
            return [];
        }

        $allResults = [];
        $seenIds = [];

        // Build different search queries
        $searchQueries = [];

        // Primary searches: performer + studio combinations
        if (!empty($performer) && !empty($studio)) {
            $performerVars = $this->getPerformerVariations($performer);
            foreach ($performerVars as $pVar) {
                $searchQueries[] = "$studio $pVar";
                $searchQueries[] = "$pVar $studio";
            }
        }

        // Performer-only searches
        if (!empty($performer)) {
            $performerVars = $this->getPerformerVariations($performer);
            foreach ($performerVars as $pVar) {
                $searchQueries[] = $pVar;
            }
        }

        // Studio + date searches
        if (!empty($studio) && !empty($date)) {
            $searchQueries[] = "$studio $date";
            
            // Try studio + year only for scenes where performer wasn't parsed well
            if (empty($performer) || strlen($performer) < 3) {
                $year = substr($date, 0, 4);
                $searchQueries[] = "$studio $year";
            }
        }
        
        // Title-based search fallback
        if (!empty($rawRelease)) {
            $title = $this->extractTitleFromRelease($rawRelease, [
                'studio' => $studio, 
                'performer' => $performer, 
                'date' => $date
            ]);
            if (!empty($title) && strlen($title) > 5) {
                $searchQueries[] = $title;
            }
        }

        // Sanitize and deduplicate queries
        $normalizedQueries = [];
        foreach ($searchQueries as $query) {
            $sanitized = $this->sanitizeSearchQuery($query);
            if (strlen($sanitized) < $this->minQueryLength) {
                continue;
            }
            $normalized = strtolower(trim($sanitized));
            $normalizedQueries[$normalized] = $sanitized;
        }
        $searchQueries = array_values($normalizedQueries);

        foreach ($searchQueries as $query) {
            if (empty(trim($query))) continue;
            
            // Check negative results cache
            $queryCacheKey = md5(strtolower($query));
            if (isset($this->noResultsCache[$queryCacheKey])) {
                $this->apiLog("Skipping query with no previous results: $query");
                continue;
            }

            $this->apiLog("Searching ThePornDB: $query");

            for ($attempt = 1; $attempt <= $this->maxRetries; $attempt++) {
                try {
                    $url = $this->apiBaseUrl . '/scenes?' . http_build_query(['q' => $query]);

                    $ch = curl_init();
                    curl_setopt($ch, CURLOPT_URL, $url);
                    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                    curl_setopt($ch, CURLOPT_HTTPHEADER, [
                        'Authorization: Bearer ' . $apiKey,
                        'Accept: application/json'
                    ]);
                    curl_setopt($ch, CURLOPT_TIMEOUT, 30);
                    curl_setopt($ch, CURLOPT_USERAGENT, 'PreDB-TPDb-Updater/1.0');

                    $response = curl_exec($ch);
                    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                    $curlError = curl_error($ch);
                    curl_close($ch);

                    if ($curlError) {
                        $this->apiLog("cURL Error (Attempt {$attempt}): {$curlError}", 'WARNING');
                        if ($attempt < $this->maxRetries) {
                            sleep($this->retryDelay);
                            continue;
                        }
                        break;
                    }

                    if ($httpCode !== 200) {
                        $this->apiLog("HTTP {$httpCode} (Attempt {$attempt})", 'WARNING');
                        if ($attempt < $this->maxRetries) {
                            sleep($this->retryDelay);
                            continue;
                        }
                        break;
                    }

                    $data = json_decode($response, true);

                    if (!$data) {
                        $this->apiLog("Invalid JSON response (Attempt {$attempt})", 'WARNING');
                        if ($attempt < $this->maxRetries) {
                            sleep($this->retryDelay);
                            continue;
                        }
                        break;
                    }

                    if (isset($data['data']) && !empty($data['data'])) {
                        $this->apiLog("Found " . count($data['data']) . " results");

                        foreach ($data['data'] as $scene) {
                            $sceneId = $scene['id'] ?? null;
                            if ($sceneId && !in_array($sceneId, $seenIds)) {
                                $allResults[] = $scene;
                                $seenIds[] = $sceneId;
                            }
                        }
                        break; // Success, exit retry loop
                    } else {
                        $this->apiLog("No results for: $query");
                        // Cache this query as having no results
                        $this->noResultsCache[$queryCacheKey] = true;
                        break; // No results, exit retry loop
                    }

                } catch (Exception $e) {
                    $this->apiLog("Exception (Attempt {$attempt}): " . $e->getMessage(), 'ERROR');
                    if ($attempt < $this->maxRetries) {
                        sleep($this->retryDelay);
                    }
                }
            }

            // Rate limiting between searches
            usleep(500000); // 0.5 second
        }

        $this->apiLog("Total unique results from all queries: " . count($allResults));
        return $allResults;
    }
    /**
     * Score how well a scene matches the release with improved matching
     */
    private function scoreMatch($scene, $parsedInfo) {
        $score = 0;
        $details = [];
        
        $sceneDate = $scene['date'] ?? '';
        $site = $scene['site']['name'] ?? '';
        
        // Date matching (50 points)
        if ($sceneDate === $parsedInfo['date']) {
            $score += 50;
            $details[] = "Exact date match";
        } elseif ($sceneDate && $parsedInfo['date']) {
            try {
                $releaseDate = new DateTime($parsedInfo['date']);
                $sceneDateTime = new DateTime($sceneDate);
                $daysDiff = abs($releaseDate->diff($sceneDateTime)->days);
                
                if ($daysDiff <= 7) {
                    $score += 25;
                    $details[] = "Date close ($daysDiff days)";
                }
            } catch (Exception $e) {
                // Invalid date
            }
        }
        
        // Studio/Site matching (30 points)
        $studioVariations = $this->normalizeStudioName($parsedInfo['studio']);
        if (in_array($site, $studioVariations) || in_array($parsedInfo['studio'], $studioVariations)) {
            $score += 30;
            $details[] = "Exact studio match";
        } else {
            $studioSim = $this->similarityRatio($site, $parsedInfo['studio']);
            if ($studioSim > 0.8) {
                $score += intval(30 * $studioSim);
                $details[] = sprintf("Studio similar (%.0f%%)", $studioSim * 100);
            }
        }
        
        // Performer matching (20 points) with improved logic
        $performers = [];
        if (isset($scene['performers'])) {
            foreach ($scene['performers'] as $p) {
                $performers[] = strtolower($p['name'] ?? '');
            }
        }
        
        $parsedPerformer = strtolower($this->normalizePerformerName($parsedInfo['performer']));
        $parsedOriginal = strtolower($parsedInfo['performer']);
        
        // Get all performer variations for better matching
        $performerVariations = array_map('strtolower', $this->getPerformerVariations($parsedInfo['performer']));
        
        // Check learned mappings first (higher score)
        if (isset($this->performerMappings[$parsedInfo['performer']])) {
            $mappedName = strtolower($this->performerMappings[$parsedInfo['performer']]);
            if (in_array($mappedName, $performers)) {
                $score += 25; // Higher score for learned match
                $details[] = "Learned performer match ({$this->performerMappings[$parsedInfo['performer']]})";
            }
        }
        
        // Check for exact match with any variation
        $foundExactMatch = false;
        foreach ($performerVariations as $variation) {
            if (in_array($variation, $performers)) {
                $score += 20;
                $details[] = "Exact performer match";
                $foundExactMatch = true;
                break;
            }
        }
        
        if (!$foundExactMatch) {
            // Check substring matches with variations
            foreach ($performers as $performer) {
                $matchFound = false;
                
                foreach ($performerVariations as $variation) {
                    if (strpos($variation, $performer) !== false || 
                        strpos($performer, $variation) !== false) {
                        $score += 18;
                        $details[] = "Performer substring match ($performer)";
                        $matchFound = true;
                        break;
                    }
                }
                
                if ($matchFound) break;
                
                // Word-level matching for CamelCase scenarios
                $parsedWords = explode(' ', $parsedPerformer);
                $apiWords = explode(' ', $performer);
                $wordMatches = 0;
                
                foreach ($parsedWords as $pWord) {
                    if (strlen($pWord) > 2) {
                        foreach ($apiWords as $aWord) {
                            similar_text($pWord, $aWord, $sim);
                            if ($sim > 80) $wordMatches++;
                        }
                    }
                }
                
                if ($wordMatches >= min(2, count($parsedWords))) {
                    $score += 15;
                    $details[] = "Performer word match ($performer)";
                    break;
                }
                
                // Overall similarity - check against all variations
                $bestSim = 0;
                foreach ($performerVariations as $variation) {
                    $sim = $this->similarityRatio($variation, $performer);
                    $bestSim = max($bestSim, $sim);
                }
                
                if ($bestSim > 0.8) {
                    $score += intval(20 * $bestSim);
                    $details[] = sprintf("Performer similar ($performer, %.0f%%)", $bestSim * 100);
                    break;
                }
            }
        }
        
        // NEW: Title similarity scoring (15 points)
        $sceneTitle = $scene['title'] ?? '';
        $releaseName = $parsedInfo['raw'] ?? '';
        
        $potentialTitle = $this->extractTitleFromRelease($releaseName, $parsedInfo);
        
        if (!empty($potentialTitle) && !empty($sceneTitle)) {
            $titleSim = $this->similarityRatio($potentialTitle, $sceneTitle);
            if ($titleSim > 0.8) {
                $score += intval(15 * $titleSim);
                $details[] = sprintf("Title match (%.0f%%)", $titleSim * 100);
            } elseif ($titleSim > 0.6) {
                $score += intval(10 * $titleSim);
                $details[] = sprintf("Title partial match (%.0f%%)", $titleSim * 100);
            }
        }
        
        // NEW: Duration metadata bonus (5 points)
        if (isset($scene['duration']) && $scene['duration'] > 0) {
            $score += 5;
            $details[] = "Has duration metadata";
        }
        
        // NEW: Release group/tag matching (5 points)
        if (isset($scene['tags']) && is_array($scene['tags'])) {
            foreach ($scene['tags'] as $tag) {
                $tagName = is_array($tag) ? ($tag['name'] ?? '') : $tag;
                if (!empty($tagName) && stripos($releaseName, $tagName) !== false) {
                    $score += 5;
                    $details[] = "Tag match ({$tagName})";
                    break;
                }
            }
        }
        
        return [$score, $details];
    }
    
    /**
     * Find best matching scene from results
     */
    private function findBestMatch($results, $parsedInfo) {
        if (empty($results)) {
            return null;
        }
        
        $scoredResults = [];
        
        foreach ($results as $scene) {
            list($score, $details) = $this->scoreMatch($scene, $parsedInfo);
            $scoredResults[] = [
                'score' => $score,
                'details' => $details,
                'scene' => $scene
            ];
        }
        
        // Sort by score
        usort($scoredResults, function($a, $b) {
            return $b['score'] - $a['score'];
        });
        
        // Log results
        foreach ($scoredResults as $i => $result) {
            $scene = $result['scene'];
            $sceneId = $scene['id'] ?? 'N/A';
            $title = $scene['title'] ?? 'N/A';
            $matchLevel = $result['score'] >= 80 ? 'EXCELLENT' : 
                         ($result['score'] >= 60 ? 'GOOD' : 
                         ($result['score'] >= 40 ? 'POSSIBLE' : 'WEAK'));
            
            $this->apiLog(sprintf(
                "  Result #%d: [Score: %d/100] %s - %s (ID: %s)",
                $i + 1,
                $result['score'],
                $matchLevel,
                $title,
                $sceneId
            ));
        }
        
        // Return best match if score >= minScore
        $bestResult = $scoredResults[0];
        if ($bestResult['score'] >= $this->minScore) {
            $this->apiLog("Best match selected with score: " . $bestResult['score']);
            
            // Learn from this successful match
            $this->learnFromMatch(
                $parsedInfo['performer'],
                $bestResult['scene']['performers'] ?? [],
                $parsedInfo['studio'],
                $bestResult['scene']['site']['name'] ?? null
            );
            
            return $bestResult;
        }
        
        $this->apiLog("No confident match found (best score: " . $bestResult['score'] . ")");
        return null;
    }
    
    /**
     * Update release URL in database
     */
    private function updateReleaseURL($releaseId, $tpdbUrl) {
        if ($this->dryRun) {
            $this->apiLog("🔍 DRY RUN: Would update release ID {$releaseId} with URL: {$tpdbUrl}");
            return true; // Simulate success in dry run
        }
        
        $query = "UPDATE releases SET url = ? WHERE id = ?";
        
        $stmt = $this->db->prepare($query);
        if (!$stmt) {
            $this->apiLog("Failed to prepare update query: " . $this->db->error, 'ERROR');
            return false;
        }
        
        $stmt->bind_param('si', $tpdbUrl, $releaseId);
        
        if ($stmt->execute()) {
            $this->apiLog("✓ Updated release ID {$releaseId} with URL: {$tpdbUrl}");
            $stmt->close();
            return true;
        } else {
            $this->apiLog("Failed to update release ID {$releaseId}: " . $stmt->error, 'ERROR');
            $stmt->close();
            return false;
        }
    }
    
    /**
     * Log failed matches for debugging
     */
    private function logFailedMatch($releaseName, $parsedInfo, $results, $bestScore) {
        $debugFile = $this->logDir . '/failed_matches_' . date('Y-m-d') . '.log';
        $debugData = [
            'timestamp' => date('Y-m-d H:i:s'),
            'release' => $releaseName,
            'parsed' => $parsedInfo,
            'top_results' => array_slice($results, 0, 3),
            'best_score' => $bestScore,
            'min_score_threshold' => $this->minScore
        ];
        file_put_contents($debugFile, json_encode($debugData) . "\n", FILE_APPEND);
    }
    
    /**
     * Process releases in batch with improved search
     */
    public function processReleases($batchSize = 100) {
        $releases = $this->getReleasesWithoutURL($batchSize);
        
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
            
            // Skip if already processed
            if ($this->isReleaseProcessed($releaseId)) {
                $this->apiLog("Skipping already processed release ID {$releaseId}");
                continue;
            }
            
            $this->apiLog("\n" . str_repeat("=", 80));
            $this->apiLog("Processing [ID: {$releaseId}]: {$releaseName}");
            $this->apiLog(str_repeat("=", 80));
            
            // Parse release name
            $parsed = $this->parseReleaseName($releaseName);
            $this->apiLog("Parsed - Studio: {$parsed['studio']}, Date: {$parsed['date']}, Performer: {$parsed['performer']}");
            
            // Search ThePornDB with multiple strategies
            $results = $this->searchScene($parsed['studio'], $parsed['performer'], $parsed['date'], $parsed['raw'], $parsed['title'] ?? null);
            
            if (empty($results)) {
                $this->apiLog("No results found from any search strategy");
                $this->markReleaseAsProcessed($releaseId, $releaseName, null, null, 'NO_RESULTS');
                $failed++;
                $processed++;
                sleep($this->rateLimitDelay);
                continue;
            }
            
            // Find best match
            $bestMatch = $this->findBestMatch($results, $parsed);
            
            if ($bestMatch) {
                $scene = $bestMatch['scene'];
                $sceneId = $scene['id'];
                $tpdbUrl = "https://theporndb.net/scenes/$sceneId";
                $score = $bestMatch['score'];
                
                if ($this->updateReleaseURL($releaseId, $tpdbUrl)) {
                    $this->markReleaseAsProcessed($releaseId, $releaseName, $tpdbUrl, $score, 'SUCCESS');
                    $updated++;
                } else {
                    $this->markReleaseAsProcessed($releaseId, $releaseName, null, $score, 'UPDATE_FAILED');
                    $failed++;
                }
            } else {
                $this->logFailedMatch($releaseName, $parsed, $results, $bestMatch['score'] ?? 0);
                $this->markReleaseAsProcessed($releaseId, $releaseName, null, null, 'NO_MATCH');
                $failed++;
            }
            
            $processed++;
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
        $processedIds = $this->getProcessedReleaseIds();
        
        $query = "SELECT COUNT(*) as count FROM releases 
                WHERE (url IS NULL OR url = '') 
                AND releasename LIKE '%.XXX.%'";
        
        if (!empty($processedIds)) {
            if (count($processedIds) > 10000) {
                $recentProcessedIds = array_slice($processedIds, -10000);
                $placeholders = implode(',', array_fill(0, count($recentProcessedIds), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                $bindIds = $recentProcessedIds;
            } else {
                $placeholders = implode(',', array_fill(0, count($processedIds), '?'));
                $query .= " AND id NOT IN ({$placeholders})";
                $bindIds = $processedIds;
            }
            
            $stmt = $this->db->prepare($query);
            $types = str_repeat('i', count($bindIds));
            $stmt->bind_param($types, ...$bindIds);
            $stmt->execute();
            $result = $stmt->get_result();
        } else {
            $result = $this->db->query($query);
        }
        
        $row = $result->fetch_assoc();
        return $row['count'] ?? 0;
    }
    
    /**
     * Get statistics
     */
    public function getStatistics() {
        $stats = [];
        
        // Total XXX releases
        $result = $this->db->query("SELECT COUNT(*) as count FROM releases WHERE releasename LIKE '%.XXX.%'");
        $stats['total_xxx_releases'] = $result->fetch_assoc()['count'];
        
        // XXX releases with URL
        $result = $this->db->query("SELECT COUNT(*) as count FROM releases WHERE releasename LIKE '%.XXX.%' AND url IS NOT NULL AND url != ''");
        $stats['with_url'] = $result->fetch_assoc()['count'];
        
        // XXX releases without URL
        $result = $this->db->query("SELECT COUNT(*) as count FROM releases WHERE releasename LIKE '%.XXX.%' AND (url IS NULL OR url = '')");
        $stats['without_url'] = $result->fetch_assoc()['count'];
        
        // Completion percentage
        $stats['completion_percentage'] = $stats['total_xxx_releases'] > 0 
            ? round(($stats['with_url'] / $stats['total_xxx_releases']) * 100, 2) 
            : 0;
        
        // Get processed data
        $data = $this->loadProcessedData();
        $processedIds = $data['processed_ids'] ?? [];
        $stats['total_processed'] = count($processedIds);
        $stats['successful'] = count($data['releases'] ?? []);
        $stats['failed'] = count($data['failed_releases'] ?? []);
        $stats['last_run'] = $data['last_run'] ?? 'Never';
        
        // Learned mappings stats
        $stats['learned_performers'] = count($this->performerMappings);
        $stats['learned_studios'] = count($this->studioMappingsLearned);
        
        $this->apiLog("=== Statistics ===");
        $this->apiLog("Total XXX releases: {$stats['total_xxx_releases']}");
        $this->apiLog("With URL: {$stats['with_url']} ({$stats['completion_percentage']}%)");
        $this->apiLog("Without URL: {$stats['without_url']}");
        $this->apiLog("Processed by script: {$stats['total_processed']}");
        $this->apiLog("  - Successful: {$stats['successful']}");
        $this->apiLog("  - Failed: {$stats['failed']}");
        $this->apiLog("Learned mappings: {$stats['learned_performers']} performers, {$stats['learned_studios']} studios");
        
        return $stats;
    }
    
    /**
     * Clean up old processed IDs
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
        
        $data['processed_ids'] = array_slice($data['processed_ids'], -$keepLast);
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
     * Retry failed releases with improved search
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
            // Remove from processed to allow retry
            $processedIds = $data['processed_ids'];
            if (($key = array_search($releaseId, $processedIds)) !== false) {
                unset($processedIds[$key]);
                $data['processed_ids'] = array_values($processedIds);
                unset($data['failed_releases'][$releaseId]);
                $this->saveProcessedData($data);
            }
            
            $this->apiLog("Retrying release [ID: {$releaseId}]: {$info['name']}");
            
            // Parse and search with new strategies
            $parsed = $this->parseReleaseName($info['name']);
            $results = $this->searchScene($parsed['studio'], $parsed['performer'], $parsed['date'], $parsed['raw'], $parsed['title'] ?? null);
            
            if (!empty($results)) {
                $bestMatch = $this->findBestMatch($results, $parsed);
                
                if ($bestMatch) {
                    $scene = $bestMatch['scene'];
                    $tpdbUrl = "https://theporndb.net/scenes/{$scene['id']}";
                    
                    if ($this->updateReleaseURL($releaseId, $tpdbUrl)) {
                        $this->markReleaseAsProcessed($releaseId, $info['name'], $tpdbUrl, $bestMatch['score'], 'SUCCESS');
                        $updated++;
                    } else {
                        $this->markReleaseAsProcessed($releaseId, $info['name'], null, null, 'UPDATE_FAILED');
                        $failed++;
                    }
                } else {
                    $this->markReleaseAsProcessed($releaseId, $info['name'], null, null, 'NO_MATCH');
                    $failed++;
                }
            } else {
                $this->markReleaseAsProcessed($releaseId, $info['name'], null, null, 'NO_RESULTS');
                $failed++;
            }
            
            $processed++;
            sleep($this->rateLimitDelay);
        }
        
        return ['processed' => $processed, 'updated' => $updated, 'failed' => $failed];
    }
    
    public function __destruct() {
        $modeStr = $this->dryRun ? "DRY RUN MODE" : "LIVE MODE";
        $this->apiLog("=== ThePornDB Updater Finished [{$modeStr}] ===");
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
    $updater = new ThePornDBUpdater(
        [],
        $verbose,
        $logDir,
        null,
        $dryRun  // Pass dry run flag
    );
    
    $runCount = 0;
    
    $modeEmoji = $dryRun ? "🔍" : "🔞";
    $modeText = $dryRun ? "DRY RUN" : "LIVE";
    
    error_log("{$modeEmoji} ThePornDB Updater Started [{$modeText} MODE]");
    if ($dryRun) {
        error_log("⚠️  DRY RUN MODE - No database changes will be made!");
        error_log("⚠️  Log files will be saved with '_dryrun' suffix");
    }
    echo "Press Ctrl+C to stop gracefully\n\n";
    
    while (true) {
        if ($shutdownRequested) {
            error_log("🛑 Graceful shutdown initiated.\n");
            break;
        }
        
        $runCount++;

        $stats = $updater->getStatistics();
        
        if ($stats && $stats['without_url'] > 0) {
            error_log("📊 Run #{$runCount} - " . date('H:i:s') . " - XXX releases without URL: {$stats['without_url']}");
            
            $result = $updater->processReleases($batchSize);
            
            error_log("Processed: {$result['processed']}, Updated: {$result['updated']}, Failed: {$result['failed']}, Remaining: {$result['remaining']}");
            
            if ($runCount % 10 === 0) {
                $updater->cleanupProcessedIds(50000);
            }
            
            if ($result['remaining'] > 0 && !$dryRun) {
                sleep(3);
            } else if ($dryRun) {
                error_log("🔍 DRY RUN COMPLETE - Exiting after one batch");
                break;
            }
        } else {
            error_log("😴 No releases needing updates. Idling for " . round($idleTime / 60, 1) . " minutes...");

            if ($dryRun) {
                error_log("🔍 DRY RUN MODE - Exiting instead of idling");
                break;
            }
            
            for ($i = 0; $i < $idleTime; $i += $checkInterval) {
                if ($shutdownRequested) {
                    break 2;
                }
                
                for ($j = 0; $j < $checkInterval; $j++) {
                    if ($shutdownRequested) {
                        break 3;
                    }
                    sleep(1);
                }
            }
            
            error_log("Idle period complete, checking for new releases...");
        }
        
        if (function_exists('gc_mem_caches')) {
            gc_mem_caches();
        }
        
        $memoryUsage = round(memory_get_usage(true) / 1024 / 1024, 2);
        if ($memoryUsage > 256) {
            error_log("Memory usage high ({$memoryUsage}MB), restarting script...");
            break;
        }
    }        
    
    $finalStats = $updater->getStatistics();
    if ($finalStats) {
        error_log("=== FINAL STATISTICS" . ($dryRun ? " [DRY RUN]" : "") . " ===");
        error_log("Total runs: {$runCount}");
        error_log("Total XXX releases: {$finalStats['total_xxx_releases']}");
        error_log("With URL: {$finalStats['with_url']} ({$finalStats['completion_percentage']}%)");
        error_log("Without URL: {$finalStats['without_url']}");
        error_log("Learned mappings: {$finalStats['learned_performers']} performers, {$finalStats['learned_studios']} studios");
        if ($dryRun) {
            error_log("⚠️  DRY RUN MODE - No actual database changes were made");
        }
    }

    error_log("👋 Script terminated gracefully at " . date('Y-m-d H:i:s'));
    
} catch (Exception $e) {
    error_log("ThePornDB Updater Fatal Error: " . $e->getMessage());
    exit(1);
}
?>

