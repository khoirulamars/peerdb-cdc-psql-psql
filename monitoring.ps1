# ===================================================================
# PeerDB Pipeline Real-Time Statistics Monitor (Enhanced with File Selection)
# PeerDB PostgreSQL to PostgreSQL Replication
# ===================================================================
# Purpose: Comprehensive monitoring for PeerDB CDC pipeline with enhanced analytics and file selection
# Author: PeerDB Team  
# Version: 3.0 (Enhanced with File Selection)
# Last Updated: 2025-08-25
# ===================================================================

# ===================================================================
# UTILITY FUNCTIONS
# ===================================================================

function Write-Header {
    param([string]$title)
    Write-Host "`n$title" -ForegroundColor Cyan
    Write-Host ("=" * 80) -ForegroundColor Gray
}

function Write-TableHeader {
    param($col1, $col2, $col3, $col4)
    Write-Host ("{0,-30} {1,-15} {2,-15} {3,-15}" -f $col1, $col2, $col3, $col4) -ForegroundColor Yellow
    Write-Host ("=" * 80) -ForegroundColor Gray
}

function Write-TableRow {
    param($col1, $col2, $col3, $col4, $color = "White")
    Write-Host ("{0,-30} {1,-15} {2,-15} {3,-15}" -f $col1, $col2, $col3, $col4) -ForegroundColor $color
}

function Convert-PostgreSizeToKiB {
    param([string]$pgSizeOutput)
    
    # Convert PostgreSQL pg_size_pretty output from "kB" to "KiB"
    if ($pgSizeOutput -match '(\d+(?:\.\d+)?)\s*kB') {
        return $pgSizeOutput -replace '\s*kB', ' KiB'
    } elseif ($pgSizeOutput -match '(\d+(?:\.\d+)?)\s*MB') {
        return $pgSizeOutput -replace '\s*MB', ' MiB'  
    } elseif ($pgSizeOutput -match '(\d+(?:\.\d+)?)\s*GB') {
        return $pgSizeOutput -replace '\s*GB', ' GiB'
    } elseif ($pgSizeOutput -match '(\d+(?:\.\d+)?)\s*TB') {
        return $pgSizeOutput -replace '\s*TB', ' TiB'
    } else {
        return $pgSizeOutput  # Return as-is for bytes
    }
}

function Get-AllLogFiles {
    $logPath = "testing-results"
    
    if (-not (Test-Path $logPath)) {
        Write-Host "Testing-results directory not found!" -ForegroundColor Red
        return @{
            StressTestFiles = @()
            ResourceUsageFiles = @()
        }
    }
    
    # Get all stress test files
    $stressTestFiles = Get-ChildItem -Path $logPath -Filter "peerdb-stress-test-*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending
    
    # Get all resource usage files (both with and without timestamp)
    $resourceUsageFiles = @()
    $resourceUsageFiles += Get-ChildItem -Path $logPath -Filter "peerdb-resource-usage-*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending
    $legacyResourceFile = Get-ChildItem -Path $logPath -Filter "peerdb-resource-usage.log" -ErrorAction SilentlyContinue
    if ($legacyResourceFile) {
        $resourceUsageFiles += $legacyResourceFile
    }
    
    return @{
        StressTestFiles = $stressTestFiles
        ResourceUsageFiles = $resourceUsageFiles
        LogPath = $logPath
    }
}

function Show-LogFileSelection {
    param($logFiles)
    
    Write-Host ""
    Write-Host "======================================" -ForegroundColor Magenta
    Write-Host "    FILE LOG SELECTION MENU" -ForegroundColor Magenta
    Write-Host "======================================" -ForegroundColor Magenta
    Write-Host ""
    
    # Show Stress Test Files
    Write-Host "[STRESS TEST LOG FILES]" -ForegroundColor Yellow
    if ($logFiles.StressTestFiles.Count -gt 0) {
        for ($i = 0; $i -lt $logFiles.StressTestFiles.Count; $i++) {
            $file = $logFiles.StressTestFiles[$i]
            $size = [math]::Round($file.Length / 1KB, 2)
            $timeInfo = $file.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
            Write-Host "  $($i + 1). $($file.Name) ($size KiB, $timeInfo)" -ForegroundColor White
        }
    } else {
        Write-Host "  No stress test files found" -ForegroundColor Red
    }
    
    Write-Host ""
    
    # Show Resource Usage Files
    Write-Host "[RESOURCE USAGE LOG FILES]" -ForegroundColor Yellow
    if ($logFiles.ResourceUsageFiles.Count -gt 0) {
        for ($i = 0; $i -lt $logFiles.ResourceUsageFiles.Count; $i++) {
            $file = $logFiles.ResourceUsageFiles[$i]
            $size = [math]::Round($file.Length / 1KB, 2)
            $timeInfo = $file.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
            Write-Host "  $($i + 1). $($file.Name) ($size KiB, $timeInfo)" -ForegroundColor White
        }
    } else {
        Write-Host "  No resource usage files found" -ForegroundColor Red
    }
    
    Write-Host ""
    Write-Host "Selection Options:" -ForegroundColor Cyan
    Write-Host "  [A] Auto-select latest files" -ForegroundColor Green
    Write-Host "  [M] Manual file selection" -ForegroundColor Yellow
    Write-Host "  [Q] Quit to main menu" -ForegroundColor Red
    Write-Host ""
}

function Select-LogFiles {
    param($logFiles)
    
    do {
        Show-LogFileSelection $logFiles
        $choice = Read-Host "Choose selection mode [A/M/Q]"
        
        switch ($choice.ToUpper()) {
            "A" {
                # Auto-select latest files
                $selectedStressTest = if ($logFiles.StressTestFiles.Count -gt 0) { $logFiles.StressTestFiles[0] } else { $null }
                $selectedResourceUsage = if ($logFiles.ResourceUsageFiles.Count -gt 0) { $logFiles.ResourceUsageFiles[0] } else { $null }
                
                Write-Host ""
                Write-Host "[AUTO SELECTION]" -ForegroundColor Green
                Write-Host "Selected Stress Test: $(if ($selectedStressTest) { $selectedStressTest.Name } else { 'None' })" -ForegroundColor White
                Write-Host "Selected Resource Usage: $(if ($selectedResourceUsage) { $selectedResourceUsage.Name } else { 'None' })" -ForegroundColor White
                
                return @{
                    StressTestFile = $selectedStressTest
                    ResourceUsageFile = $selectedResourceUsage
                    Valid = $true
                }
            }
            
            "M" {
                # Manual selection
                Write-Host ""
                Write-Host "[MANUAL SELECTION]" -ForegroundColor Yellow
                
                # Select Stress Test File
                $selectedStressTest = $null
                if ($logFiles.StressTestFiles.Count -gt 0) {
                    Write-Host "Select Stress Test File (1-$($logFiles.StressTestFiles.Count), or 0 for none): " -NoNewline
                    $stressChoice = Read-Host
                    
                    if ($stressChoice -match '^\d+$') {
                        $index = [int]$stressChoice - 1
                        if ($index -ge 0 -and $index -lt $logFiles.StressTestFiles.Count) {
                            $selectedStressTest = $logFiles.StressTestFiles[$index]
                            Write-Host "Selected: $($selectedStressTest.Name)" -ForegroundColor Green
                        } elseif ($stressChoice -eq "0") {
                            Write-Host "No stress test file selected" -ForegroundColor Yellow
                        } else {
                            Write-Host "Invalid selection" -ForegroundColor Red
                            continue
                        }
                    } else {
                        Write-Host "Invalid input" -ForegroundColor Red
                        continue
                    }
                }
                
                # Select Resource Usage File
                $selectedResourceUsage = $null
                if ($logFiles.ResourceUsageFiles.Count -gt 0) {
                    Write-Host "Select Resource Usage File (1-$($logFiles.ResourceUsageFiles.Count), or 0 for none): " -NoNewline
                    $resourceChoice = Read-Host
                    
                    if ($resourceChoice -match '^\d+$') {
                        $index = [int]$resourceChoice - 1
                        if ($index -ge 0 -and $index -lt $logFiles.ResourceUsageFiles.Count) {
                            $selectedResourceUsage = $logFiles.ResourceUsageFiles[$index]
                            Write-Host "Selected: $($selectedResourceUsage.Name)" -ForegroundColor Green
                        } elseif ($resourceChoice -eq "0") {
                            Write-Host "No resource usage file selected" -ForegroundColor Yellow
                        } else {
                            Write-Host "Invalid selection" -ForegroundColor Red
                            continue
                        }
                    } else {
                        Write-Host "Invalid input" -ForegroundColor Red
                        continue
                    }
                }
                
                return @{
                    StressTestFile = $selectedStressTest
                    ResourceUsageFile = $selectedResourceUsage
                    Valid = $true
                }
            }
            
            "Q" {
                return @{ Valid = $false }
            }
            
            default {
                Write-Host "Invalid choice. Please select A, M, or Q." -ForegroundColor Red
                Start-Sleep -Seconds 2
            }
        }
    } while ($true)
}

function Invoke-PostgresQuery {
    param([string]$Database, [string]$Query)
    
    try {
        $result = docker exec catalog psql -U postgres -d $Database -t -c $Query 2>$null
        if ($result -is [array]) {
            return ($result -join "").Trim()
        } else {
            return $result
        }
    } catch {
        return $null
    }
}

# ===================================================================
# 1. SYSTEM PERFORMANCE LOG (from selected log file)
# ===================================================================

function Show-SystemPerformancePhases {
    param($selectedFiles)
    
    Write-Header "1. System Resource Usage (from Selected Log)"
    
    if (-not $selectedFiles.ResourceUsageFile) {
        Write-Host "No resource usage log file selected. Please select a file first." -ForegroundColor Yellow
        return
    }
    
    $resourceLogPath = $selectedFiles.ResourceUsageFile.FullName
    Write-Host "Analyzing: $($selectedFiles.ResourceUsageFile.Name)" -ForegroundColor Gray
    Write-Host ""
    
    $lines = Get-Content $resourceLogPath
    $phases = @("BASELINE", "INSERT", "FINAL")
    
    # First, find all INSERT-BATCH phases for detailed display
    $insertBatches = @()
    for ($i = 0; $i -lt $lines.Length; $i++) {
        if ($lines[$i] -match "INSERT-BATCH-(\d+)") {
            $insertBatches += @{
                BatchNumber = [int]$matches[1]
                LineIndex = $i
            }
        }
    }
    
    Write-Host "Resource Log Analysis:" -ForegroundColor Yellow
    Write-Host "  File: $($selectedFiles.ResourceUsageFile.Name)" -ForegroundColor White
    Write-Host "  Size: $([math]::Round($selectedFiles.ResourceUsageFile.Length / 1KB, 2)) KiB" -ForegroundColor White
    Write-Host "  Modified: $($selectedFiles.ResourceUsageFile.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor White
    Write-Host "  INSERT Batches Found: $($insertBatches.Count)" -ForegroundColor White
    Write-Host ""
    
    foreach ($phase in $phases) {
        if ($phase -eq "INSERT" -and $insertBatches.Count -gt 0) {
            # Display multiple INSERT batches
            Write-Host "INSERT BATCHES ($($insertBatches.Count) found):" -ForegroundColor Yellow
            
            # Show first 5 and last 5 batches if more than 10
            $batchesToShow = if ($insertBatches.Count -le 10) { 
                $insertBatches 
            } else { 
                $insertBatches[0..4] + @{BatchNumber = "..."; LineIndex = -1} + $insertBatches[-5..-1]
            }
            
            foreach ($batch in $batchesToShow) {
                if ($batch.BatchNumber -eq "...") {
                    Write-Host "  ... (showing first 5 and last 5 batches)" -ForegroundColor Gray
                    continue
                }
                
                $phaseStartIndex = $batch.LineIndex
                $batchNumber = $batch.BatchNumber
                
                Write-Host ""
                Write-Host "Phase: INSERT-BATCH-$batchNumber" -ForegroundColor Yellow
                
                # Show timestamp and phase name
                $timestampLine = $lines[$phaseStartIndex]
                if ($timestampLine -match "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})") {
                    $timestamp = $matches[1]
                    Write-Host "$timestamp        INSERT-BATCH-$batchNumber" -ForegroundColor White
                }
                
                DisplayContainerStats $lines $phaseStartIndex
            }
        } else {
            # Display BASELINE and FINAL phases
            $searchPhase = $phase
            $phaseStartIndex = -1
            for ($i = 0; $i -lt $lines.Length; $i++) {
                if ($lines[$i] -match "\s$searchPhase$" -or ($lines[$i] -match "^\d{4}-\d{2}-\d{2}.*$searchPhase$")) {
                    $phaseStartIndex = $i
                    break
                }
            }
            
            if ($phaseStartIndex -ge 0) {
                Write-Host ""
                Write-Host "Phase: $phase" -ForegroundColor Yellow
                
                # Show timestamp and phase name
                $timestampLine = $lines[$phaseStartIndex]
                if ($timestampLine -match "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})") {
                    $timestamp = $matches[1]
                    Write-Host "$timestamp        $phase" -ForegroundColor White
                }
                
                DisplayContainerStats $lines $phaseStartIndex
            } else {
                Write-Host ""
                Write-Host "Phase: $phase" -ForegroundColor Yellow
                Write-Host "Data not found in log file" -ForegroundColor Red
            }
        }
    }
}

function DisplayContainerStats {
    param($lines, $phaseStartIndex)
    
    # Find and display DOCKER STATS section
    $statsStartIndex = -1
    for ($j = $phaseStartIndex; $j -lt ($phaseStartIndex + 5) -and $j -lt $lines.Length; $j++) {
        if ($lines[$j] -match "DOCKER STATS:") {
            $statsStartIndex = $j
            break
        }
    }
    
    if ($statsStartIndex -ge 0) {
        Write-Host "DOCKER STATS:" -ForegroundColor Cyan
        
        # Track processed containers to avoid duplicates
        $processedContainers = @()
        
        # Process each container section
        $currentIndex = $statsStartIndex + 1
        $endIndex = $phaseStartIndex + 100
        if ($endIndex -ge $lines.Length) { $endIndex = $lines.Length - 1 }
        
        while ($currentIndex -lt $endIndex) {
            $line = $lines[$currentIndex]
            
            # Check for container section headers
            if ($line -match "^(PEERDB CONTAINERS|CATALOG CONTAINER|TEMPORAL CONTAINERS?):\s*$") {
                Write-Host $line -ForegroundColor White
                $currentIndex++
                
                # Look for table header
                if ($currentIndex -lt $lines.Length -and $lines[$currentIndex] -match "NAME.*CPU.*MEM") {
                    Write-Host ("{0,-30} {1,-15} {2,-15}" -f "NAME", "CPU %", "MEM USAGE / LIMIT") -ForegroundColor Yellow
                    $currentIndex++
                    
                    # Parse container data lines
                    while ($currentIndex -lt $endIndex) {
                        $containerLine = $lines[$currentIndex]
                        
                        # Stop at empty line, separator, or next section
                        if ($containerLine.Trim() -eq "" -or $containerLine -match "^=+$" -or $containerLine -match "^[A-Z\s]+CONTAINER[S]?:\s*$") {
                            if ($containerLine -match "^[A-Z\s]+CONTAINER[S]?:\s*$") {
                                $currentIndex-- # Back up to process the section header
                            }
                            break
                        }
                        
                        # Process container data line (skip duplicates)
                        if ($containerLine -match "^[a-zA-Z0-9_-]") {
                            $parts = $containerLine -split '\s+' | Where-Object { $_ -ne "" }
                            if ($parts.Count -ge 3) {
                                $containerName = $parts[0]
                                
                                # Skip if already processed
                                if ($processedContainers -notcontains $containerName) {
                                    $processedContainers += $containerName
                                    $cpuPercent = $parts[1]
                                    
                                    # Memory format: "54.04MiB / 3.726GiB" or separated
                                    if ($parts[2] -match "(\d+\.?\d*\w+iB)\s*/\s*(\d+\.?\d*\w+iB)") {
                                        $memUsage = $matches[1] + " / " + $matches[2]
                                    } elseif ($parts.Count -ge 5 -and $parts[3] -eq "/" -and $parts[4]) {
                                        $memUsage = $parts[2] + " / " + $parts[4]
                                    } else {
                                        $memUsage = $parts[2] + " / " + ($parts[3] -replace "/", "")
                                    }
                                    
                                    Write-Host ("{0,-30} {1,-15} {2,-15}" -f $containerName, $cpuPercent, $memUsage) -ForegroundColor Gray
                                }
                            }
                        }
                        $currentIndex++
                    }
                }
            } elseif ($line -match "^=+$") {
                # End of phase data
                break
            } else {
                $currentIndex++
            }
        }
    }
}

# ===================================================================
# 2. PEERDB SERVER HEALTH (realtime)
# ===================================================================

function Get-PeerDBServerHealth {
    Write-Header "2. PeerDB PostgreSQL Server Health Check"
    Write-Host "Testing connection to PeerDB catalog and database servers..." -ForegroundColor Gray
    Write-Host ""
    
    Write-Host "PostgreSQL Connection Tests:" -ForegroundColor Yellow
    Write-TableHeader "Server" "Status" "Response Time" "Version"
    
    try {
        # Catalog database test
        $catalogStart = Get-Date
        $catalogVersion = Invoke-PostgresQuery "postgres" "SELECT version();"
        $catalogEnd = Get-Date
        $catalogTime = [math]::Round(($catalogEnd - $catalogStart).TotalMilliseconds, 2)
        
        if ($catalogVersion) {
            $version = "Available"
            if ($catalogVersion -match "PostgreSQL\s+([\d.]+)") {
                $version = $matches[1]
            }
            Write-TableRow "Catalog (postgres)" "OK" "$catalogTime ms" $version "Green"
        } else {
            Write-TableRow "Catalog (postgres)" "FAIL" "N/A" "N/A" "Red"
        }
        
        # Source database test
        $sourceStart = Get-Date
        $sourceVersion = Invoke-PostgresQuery "source" "SELECT version();"
        $sourceEnd = Get-Date
        $sourceTime = [math]::Round(($sourceEnd - $sourceStart).TotalMilliseconds, 2)
        
        if ($sourceVersion) {
            $version = "Available"
            if ($sourceVersion -match "PostgreSQL\s+([\d.]+)") {
                $version = $matches[1]
            }
            Write-TableRow "Source DB" "OK" "$sourceTime ms" $version "Green"
        } else {
            Write-TableRow "Source DB" "FAIL" "N/A" "N/A" "Red"
        }
        
        # Target database test
        $targetStart = Get-Date
        $targetVersion = Invoke-PostgresQuery "target" "SELECT version();"
        $targetEnd = Get-Date
        $targetTime = [math]::Round(($targetEnd - $targetStart).TotalMilliseconds, 2)
        
        if ($targetVersion) {
            $version = "Available"
            if ($targetVersion -match "PostgreSQL\s+([\d.]+)") {
                $version = $matches[1]
            }
            Write-TableRow "Target DB" "OK" "$targetTime ms" $version "Green"
        } else {
            Write-TableRow "Target DB" "FAIL" "N/A" "N/A" "Red"
        }
        
    } catch {
        Write-Host "Error checking PeerDB server health: $_" -ForegroundColor Red
    }
}

# ===================================================================
# 3. PEERDB TABLE STATS (all tables, with totals)
# ===================================================================

function Get-PeerDBTableStats {
    Write-Header "3. PeerDB Table Statistics (Source & Target)"
    Write-Host "Analyzing all tables in both source and target databases, row counts, sizes, and totals..." -ForegroundColor Gray
    Write-Host ""
    
    $databases = @(
        @{name='Source'; db='source'; color='Cyan'}, 
        @{name='Target'; db='target'; color='Green'}
    )
    
    foreach ($dbInfo in $databases) {
        Write-Host "$($dbInfo.name) Database ($($dbInfo.db)):" -ForegroundColor $dbInfo.color
        Write-TableHeader "Table" "Rows" "Size" "Last Modified"
        
        $tables = @("customers", "products", "orders")
        $totalRows = 0
        $totalSize = 0
        
        foreach ($table in $tables) {
            try {
                $query = @"
SELECT 
    COUNT(*) as total_rows,
    pg_size_pretty(pg_total_relation_size('$table')) as table_size,
    pg_total_relation_size('$table') as size_bytes
FROM $table;
"@
                
                $result = Invoke-PostgresQuery $dbInfo.db $query
                $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
                
                if ($result) {
                    $parts = $result.Trim() -split '\|' | ForEach-Object { $_.Trim() }
                    if ($parts.Length -ge 3) {
                        $rows = [int]$parts[0]
                        $sizeFormatted = Convert-PostgreSizeToKiB $parts[1]
                        $sizeBytes = [long]$parts[2]
                        
                        $totalRows += $rows
                        $totalSize += $sizeBytes
                        
                        Write-TableRow "$($dbInfo.db).$table" $rows $sizeFormatted $timestamp $dbInfo.color
                    }
                } else {
                    Write-TableRow "$($dbInfo.db).$table" "NOT_FOUND" "N/A" "N/A" "Yellow"
                }
            } catch {
                Write-TableRow "$($dbInfo.db).$table" "ERROR" "N/A" "N/A" "Red"
            }
        }
        
        # Show totals
        $totalSizeFormatted = if ($totalSize -gt 0) {
            if ($totalSize -gt 1GB) { [math]::Round($totalSize / 1GB, 2).ToString() + " GB" }
            elseif ($totalSize -gt 1MB) { [math]::Round($totalSize / 1MB, 2).ToString() + " MB" }
            elseif ($totalSize -gt 1KB) { [math]::Round($totalSize / 1KB, 2).ToString() + " KiB" }
            else { $totalSize.ToString() + " bytes" }
        } else { "0 bytes" }
        
        Write-Host ("=" * 80) -ForegroundColor Gray
        Write-TableRow "TOTAL ($($dbInfo.name))" $totalRows $totalSizeFormatted "Summary" "White"
        Write-Host ""
    }
}

# ===================================================================
# 4. ADDITIONAL ANALYSIS FUNCTIONS (from peerdb_monitor_final.ps1)
# ===================================================================

function Get-PeerDBFlowAnalysis {
    Write-Header "4. PeerDB Flow Status & CDC Analysis"
    Write-Host "Analyzing PeerDB flows and CDC replication status..." -ForegroundColor Gray
    Write-Host ""
    
    try {
        Write-Host "CDC Table Synchronization Analysis:" -ForegroundColor Yellow
        Write-TableHeader "Database" "Record Count" "Sync Status" "Lag"
        
        # Get counts for all tables
        $tables = @("customers", "products", "orders")
        $totalSourceCount = 0
        $totalTargetCount = 0
        $allTablesSync = $true
        
        foreach ($table in $tables) {
            # Get source count
            $sourceCountResult = Invoke-PostgresQuery "source" "SELECT COUNT(*) FROM $table;"
            $sourceCount = 0
            if ($sourceCountResult) {
                $sourceCount = [int]($sourceCountResult.Trim())
            }
            $totalSourceCount += $sourceCount
            
            # Get target count
            $targetCountResult = Invoke-PostgresQuery "target" "SELECT COUNT(*) FROM $table;"
            $targetCount = 0
            $syncStatus = "NO_TABLE"
            $lag = "N/A"
            $targetColor = "Red"
            
            if ($targetCountResult) {
                $targetCount = [int]($targetCountResult.Trim())
                $lag = $sourceCount - $targetCount
                
                if ($lag -eq 0) {
                    $syncStatus = "SYNCED"
                    $targetColor = "Green"
                } elseif ([math]::Abs($lag) -le 5) {
                    $syncStatus = "NEAR_SYNC"
                    $targetColor = "Yellow"
                } else {
                    $syncStatus = "LAG"
                    $targetColor = "Red"
                    $allTablesSync = $false
                }
            } else {
                $allTablesSync = $false
            }
            $totalTargetCount += $targetCount
            
            Write-TableRow "Source ($table)" $sourceCount.ToString() "Reference" "0" "Cyan"
            Write-TableRow "Target ($table)" $targetCount.ToString() $syncStatus $lag $targetColor
        }
        
        # Show flow efficiency
        Write-Host ""
        Write-Host "PeerDB Flow Performance Summary:" -ForegroundColor Yellow
        
        if ($totalSourceCount -gt 0) {
            $efficiency = if ($totalTargetCount -gt 0) { ($totalTargetCount / $totalSourceCount) * 100 } else { 0 }
            $totalLag = $totalSourceCount - $totalTargetCount
            
            Write-Host "  Total Source Records: $totalSourceCount" -ForegroundColor Cyan
            Write-Host "  Total Target Records: $totalTargetCount" -ForegroundColor Green
            Write-Host "  Replication Efficiency: $([math]::Round($efficiency, 1))%" -ForegroundColor $(if ($efficiency -ge 95) { "Green" } elseif ($efficiency -ge 80) { "Yellow" } else { "Red" })
            Write-Host "  Current Lag: $totalLag records" -ForegroundColor $(if ([math]::Abs($totalLag) -le 5) { "Green" } elseif ([math]::Abs($totalLag) -le 100) { "Yellow" } else { "Red" })
            
            if ($allTablesSync) {
                Write-Host "  Status: All tables fully synchronized" -ForegroundColor Green
            } elseif ($efficiency -ge 95) {
                Write-Host "  Status: Near real-time replication" -ForegroundColor Green
            } elseif ($efficiency -ge 80) {
                Write-Host "  Status: Acceptable replication lag" -ForegroundColor Yellow
            } else {
                Write-Host "  Status: Significant replication lag detected" -ForegroundColor Red
            }
        } else {
            Write-Host "  Status: No data found in source tables" -ForegroundColor Yellow
        }
        
    } catch {
        Write-Host "Error analyzing PeerDB flows: $_" -ForegroundColor Red
    }
}

function Get-CDCOperationsAnalysis {
    param($selectedFiles)
    
    Write-Header "5. CDC Operations Analysis (Insert/Update/Delete)"
    
    $logInsertCount = 0
    $updateCount = 0
    $deleteCount = 0
    $logFailedCount = 0
    
    if ($selectedFiles.StressTestFile) {
        Write-Host "Analyzing operations from log file: $($selectedFiles.StressTestFile.Name)" -ForegroundColor Gray
        $logContent = Get-Content $selectedFiles.StressTestFile.FullName
        
        # Parse log for operation counts
        $logContent | ForEach-Object {
            if ($_ -match "Successful Operations: (\d+)") {
                $logInsertCount = [int]$matches[1]
            }
            if ($_ -match "Failed Operations: (\d+)") {
                $logFailedCount = [int]$matches[1]
            }
        }
    } else {
        Write-Host "No stress test log file selected. Using real-time database checks only." -ForegroundColor Yellow
    }
    
    # Real-time DB check for operation counts
    $sourceInsert = Invoke-PostgresQuery "source" "SELECT COUNT(*) FROM orders;"
    $targetInsert = Invoke-PostgresQuery "target" "SELECT COUNT(*) FROM orders;"
    
    $sourceCount = if ($sourceInsert) { [int]($sourceInsert.Trim()) } else { 0 }
    $targetCount = if ($targetInsert) { [int]($targetInsert.Trim()) } else { 0 }
    
    Write-Host ""
    Write-Host "Operation Statistics:" -ForegroundColor Yellow
    Write-TableHeader "Operation" "Source" "Target" "Failed"
    Write-TableRow "INSERT" $sourceCount $targetCount $logFailedCount "Green"
    Write-TableRow "UPDATE" $updateCount $updateCount $logFailedCount "Green"
    Write-TableRow "DELETE" $deleteCount $deleteCount $logFailedCount "Green"
    
    # Show operation summary
    Write-Host ""
    Write-Host "Operation Summary:" -ForegroundColor Yellow
    $syncDiff = $sourceCount - $targetCount
    Write-Host "  Source Records: $sourceCount" -ForegroundColor Cyan
    Write-Host "  Target Records: $targetCount" -ForegroundColor Green
    Write-Host "  Sync Difference: $syncDiff" -ForegroundColor $(if ($syncDiff -eq 0) { "Green" } elseif ([math]::Abs($syncDiff) -le 10) { "Yellow" } else { "Red" })
    Write-Host "  Failed Operations (from log): $logFailedCount" -ForegroundColor $(if ($logFailedCount -eq 0) { "Green" } else { "Red" })
    Write-Host "  Log INSERT Count: $logInsertCount" -ForegroundColor Cyan
    
    Write-Host ""
    Write-Host "Additional Operation Details:" -ForegroundColor Yellow
    Write-Host "  Update Operations: $updateCount" -ForegroundColor Cyan
    Write-Host "  Delete Operations: $deleteCount" -ForegroundColor Cyan
}

function Get-ContainerHealthStatus {
    Write-Header "6. Container Health and Status"
    Write-Host "Checking Docker container health and connectivity..." -ForegroundColor Gray
    Write-Host ""
    
    try {
        Write-Host "PeerDB Pipeline Container Status:" -ForegroundColor Yellow
        Write-TableHeader "Container" "Status" "Uptime" "Health"
        
        # Check PeerDB related containers
        $peerdbContainers = @("catalog", "temporal", "temporal-admin-tools", "temporal-ui", "peerdb-server", "peerdb-ui", "flow_api", "flow-worker", "flow-snapshot-worker")
        $healthy = 0
        $total = 0
        
        foreach ($containerName in $peerdbContainers) {
            # Get container status and running duration
            $containerStatus = docker ps --filter "name=$containerName" --format "{{.Status}}" 2>$null
            $containerCreated = docker ps --filter "name=$containerName" --format "{{.CreatedAt}}" 2>$null
            $total++
            
            if ($containerStatus) {
                $status = "RUNNING"
                
                # Get container start time from CreatedAt field
                if ($containerCreated) {
                    try {
                        # Parse the created timestamp - Docker format is "2025-08-19 10:13:01 +0700 +07"
                        # Extract just the date and time part before the timezone
                        if ($containerCreated -match "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})") {
                            $dateTimeStr = $matches[1]
                            $createdDate = [DateTime]::ParseExact($dateTimeStr, "yyyy-MM-dd HH:mm:ss", $null)
                            $uptime = $createdDate.ToString("dd/MM/yyyy HH:mm:ss")
                        } else {
                            $uptime = "Running"
                        }
                    } catch {
                        # Fallback to relative time if parsing fails
                        $uptime = "Running"
                    }
                } else {
                    $uptime = "Running"
                }
                
                $health = "HEALTHY"
                $color = "Green"
                $healthy++
            } else {
                $status = "DOWN"
                $uptime = "N/A"
                $health = "UNHEALTHY"
                $color = "Red"
            }
            
            Write-TableRow $containerName $status $uptime $health $color
        }
        
        # Check additional PeerDB containers with pattern matching
        $additionalContainers = docker ps --filter name=peerdb --format "{{.Names}};{{.Status}};{{.CreatedAt}}" 2>$null
        if ($additionalContainers) {
            foreach ($container in $additionalContainers) {
                $parts = $container -split ";"
                if ($parts.Length -ge 2) {
                    $name = $parts[0]
                    $statusRaw = $parts[1].ToString().Trim()
                    $createdRaw = if ($parts.Length -ge 3) { $parts[2] } else { "" }
                    
                    # Skip if already processed
                    if ($peerdbContainers -notcontains $name) {
                        $total++
                        if ($statusRaw -match "Up") {
                            $healthy++
                            
                            # Get container start time from CreatedAt field
                            if ($createdRaw) {
                                try {
                                    # Parse the created timestamp - Docker format is "2025-08-19 10:13:01 +0700 +07"
                                    # Extract just the date and time part before the timezone
                                    if ($createdRaw -match "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})") {
                                        $dateTimeStr = $matches[1]
                                        $createdDate = [DateTime]::ParseExact($dateTimeStr, "yyyy-MM-dd HH:mm:ss", $null)
                                        $uptime = $createdDate.ToString("dd/MM/yyyy HH:mm:ss")
                                    } else {
                                        $uptime = "Running"
                                    }
                                } catch {
                                    # Fallback to relative time if parsing fails
                                    $uptime = "Running"
                                }
                            } else {
                                $uptime = "Running"
                            }
                            
                            Write-TableRow $name "RUNNING" $uptime "HEALTHY" "Green"
                        } else {
                            Write-TableRow $name "DOWN" "N/A" "UNHEALTHY" "Red"
                        }
                    }
                }
            }
        }
        
        Write-Host ("=" * 80) -ForegroundColor Gray
        Write-Host ""
        $percentage = if ($total -gt 0) { [math]::Round(($healthy / $total) * 100, 0) } else { 0 }
        $healthColor = if ($percentage -ge 80) { "Green" } elseif ($percentage -ge 50) { "Yellow" } else { "Red" }
        Write-Host "Overall Health: $healthy/$total containers healthy ($percentage%)" -ForegroundColor $healthColor
        
    } catch {
        Write-Host "Error checking container health: $_" -ForegroundColor Red
    }
}

function Get-PerformanceSummary {
    Write-Header "7. Performance Summary and Recommendations"
    Write-Host "Analyzing overall PeerDB CDC pipeline performance..." -ForegroundColor Gray
    Write-Host ""
    
    try {
        Write-Host "PeerDB Pipeline Health Summary:" -ForegroundColor Yellow
        
        # Test catalog connection
        $catalogTest = Invoke-PostgresQuery "postgres" "SELECT 1;"
        $sourceTest = Invoke-PostgresQuery "source" "SELECT 1;"
        $targetTest = Invoke-PostgresQuery "target" "SELECT 1;"
        
        Write-Host "  PeerDB Catalog : $(if ($catalogTest) { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($catalogTest) { 'Green' } else { 'Red' })
        Write-Host "  Source Database : $(if ($sourceTest) { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($sourceTest) { 'Green' } else { 'Red' })
        Write-Host "  Target Database : $(if ($targetTest) { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($targetTest) { 'Green' } else { 'Red' })
        
        # Check container status
        $catalogContainer = docker ps --filter name=catalog --format "{{.Status}}" 2>$null
        $temporalContainer = docker ps --filter name=temporal --format "{{.Status}}" 2>$null
        
        Write-Host "  Catalog Container : $(if ($catalogContainer -match 'Up') { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($catalogContainer -match 'Up') { 'Green' } else { 'Red' })
        Write-Host "  Temporal Container : $(if ($temporalContainer -match 'Up') { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($temporalContainer -match 'Up') { 'Green' } else { 'Red' })
        
        Write-Host ""
        Write-Host "Data Synchronization Status:" -ForegroundColor Yellow
        
        # Check synchronization status
        $sourceCount = Invoke-PostgresQuery "source" "SELECT COUNT(*) FROM orders;"
        $targetCount = Invoke-PostgresQuery "target" "SELECT COUNT(*) FROM orders;"
        
        $sourceRecords = if ($sourceCount) { [int]($sourceCount.Trim()) } else { 0 }
        $targetRecords = if ($targetCount) { [int]($targetCount.Trim()) } else { 0 }
        
        if ($sourceRecords -gt 0 -and $targetRecords -gt 0) {
            $syncPercentage = [math]::Round(($targetRecords / $sourceRecords) * 100, 1)
            Write-Host "  Source Records: $sourceRecords" -ForegroundColor Cyan
            Write-Host "  Target Records: $targetRecords" -ForegroundColor Green
            Write-Host "  Sync Percentage: $syncPercentage%" -ForegroundColor $(if ($syncPercentage -ge 95) { "Green" } elseif ($syncPercentage -ge 80) { "Yellow" } else { "Red" })
            
            if ($sourceRecords -eq $targetRecords) {
                Write-Host "  Sync Status: Fully synchronized" -ForegroundColor Green
            } else {
                Write-Host "  Sync Status: Replication in progress" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  No data synchronization detected" -ForegroundColor Yellow
        }
        
        Write-Host ""
        Write-Host "Recommendations:" -ForegroundColor Yellow
        Write-Host "  - Monitor replication lag regularly using this script" -ForegroundColor White
        Write-Host "  - Check PeerDB UI at http://localhost:3000 for visual monitoring" -ForegroundColor White
        Write-Host "  - Run INSERT tests to validate CDC pipeline performance" -ForegroundColor White
        Write-Host "  - Validate data integrity between source and target periodically" -ForegroundColor White
        Write-Host "  - Set up automated alerts for CDC pipeline failures" -ForegroundColor White
        
    } catch {
        Write-Host "Error generating performance summary: $_" -ForegroundColor Red
    }
}

function Get-DatabaseMetrics {
    Write-Header "8. Database Performance Metrics"
    Write-Host "Analyzing database performance and resource usage..." -ForegroundColor Gray
    Write-Host ""
    
    try {
        Write-Host "PostgreSQL Connection Statistics:" -ForegroundColor Yellow
        Write-TableHeader "Database" "Active Connections" "Max Connections" "Usage %"
        
        $connQuery = "SELECT count(*) as active, setting as max_conn FROM pg_stat_activity, pg_settings WHERE name='max_connections' GROUP BY setting;"
        
        # Source database connections
        $sourceConn = Invoke-PostgresQuery "source" $connQuery
        if ($sourceConn) {
            $sourceParts = $sourceConn.Trim() -split '\|' | ForEach-Object { $_.Trim() }
            if ($sourceParts.Length -ge 2) {
                $activeConns = $sourceParts[0]
                $maxConns = $sourceParts[1]
                $usage = [math]::Round(([int]$activeConns / [int]$maxConns) * 100, 1)
                Write-TableRow "Source" $activeConns $maxConns "$usage%" "Cyan"
            }
        } else {
            Write-TableRow "Source" "ERROR" "ERROR" "N/A" "Red"
        }
        
        # Target database connections
        $targetConn = Invoke-PostgresQuery "target" $connQuery
        if ($targetConn) {
            $targetParts = $targetConn.Trim() -split '\|' | ForEach-Object { $_.Trim() }
            if ($targetParts.Length -ge 2) {
                $activeConns = $targetParts[0]
                $maxConns = $targetParts[1]
                $usage = [math]::Round(([int]$activeConns / [int]$maxConns) * 100, 1)
                Write-TableRow "Target" $activeConns $maxConns "$usage%" "Green"
            }
        } else {
            Write-TableRow "Target" "ERROR" "ERROR" "N/A" "Red"
        }
        
        Write-Host ""
        Write-Host "Database Size Information:" -ForegroundColor Yellow
        Write-TableHeader "Database" "Total Size" "Orders Table" "Index Size"
        
        # Source database size
        $sourceSizeQuery = "SELECT pg_size_pretty(pg_database_size('source')) as db_size, pg_size_pretty(pg_total_relation_size('orders')) as table_size, pg_size_pretty(pg_indexes_size('orders')) as index_size;"
        $sourceSize = Invoke-PostgresQuery "source" $sourceSizeQuery
        if ($sourceSize) {
            $sizeParts = $sourceSize.Trim() -split '\|' | ForEach-Object { $_.Trim() }
            if ($sizeParts.Length -ge 3) {
                $dbSize = Convert-PostgreSizeToKiB $sizeParts[0]
                $tableSize = Convert-PostgreSizeToKiB $sizeParts[1]
                $indexSize = Convert-PostgreSizeToKiB $sizeParts[2]
                Write-TableRow "Source" $dbSize $tableSize $indexSize "Cyan"
            }
        } else {
            Write-TableRow "Source" "ERROR" "ERROR" "ERROR" "Red"
        }
        
        # Target database size
        $targetSizeQuery = "SELECT pg_size_pretty(pg_database_size('target')) as db_size, pg_size_pretty(pg_total_relation_size('orders')) as table_size, pg_size_pretty(pg_indexes_size('orders')) as index_size;"
        $targetSize = Invoke-PostgresQuery "target" $targetSizeQuery
        if ($targetSize) {
            $sizeParts = $targetSize.Trim() -split '\|' | ForEach-Object { $_.Trim() }
            if ($sizeParts.Length -ge 3) {
                $dbSize = Convert-PostgreSizeToKiB $sizeParts[0]
                $tableSize = Convert-PostgreSizeToKiB $sizeParts[1]
                $indexSize = Convert-PostgreSizeToKiB $sizeParts[2]
                Write-TableRow "Target" $dbSize $tableSize $indexSize "Green"
            }
        } else {
            Write-TableRow "Target" "ERROR" "ERROR" "ERROR" "Red"
        }
        
    } catch {
        Write-Host "Error getting database metrics: $_" -ForegroundColor Red
    }
}

function Get-PeerDBFlowHealth {
    Write-Header "9. PeerDB Flow Health & Configuration"
    Write-Host "Analyzing PeerDB flow configuration and health status..." -ForegroundColor Gray
    Write-Host ""
    
    try {
        Write-Host "Flow Configuration Status:" -ForegroundColor Yellow
        Write-TableHeader "Flow Component" "Status" "Details" "Health"
        
        # Check if PeerDB flows are configured
        $flowQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%flow%';"
        $flowTables = Invoke-PostgresQuery "postgres" $flowQuery
        
        if ($flowTables -and [int]($flowTables.Trim()) -gt 0) {
            Write-TableRow "Flow Tables" "OK" "Configuration tables found" "Healthy" "Green"
        } else {
            Write-TableRow "Flow Tables" "WARNING" "No flow tables found" "Check Config" "Yellow"
        }
        
        # Check replication slots
        $slotQuery = "SELECT COUNT(*) FROM pg_replication_slots;"
        $slots = Invoke-PostgresQuery "source" $slotQuery
        
        if ($slots) {
            $slotCount = [int]($slots.Trim())
            if ($slotCount -gt 0) {
                Write-TableRow "Replication Slots" "OK" "$slotCount slots active" "Healthy" "Green"
            } else {
                Write-TableRow "Replication Slots" "WARNING" "No active slots" "Check Setup" "Yellow"
            }
        } else {
            Write-TableRow "Replication Slots" "ERROR" "Cannot check slots" "Unhealthy" "Red"
        }
        
        # Check WAL level
        $walQuery = "SELECT setting FROM pg_settings WHERE name = 'wal_level';"
        $walLevel = Invoke-PostgresQuery "source" $walQuery
        
        if ($walLevel) {
            $level = $walLevel.Trim()
            if ($level -eq "logical") {
                Write-TableRow "WAL Level" "OK" "logical replication" "Optimal" "Green"
            } else {
                Write-TableRow "WAL Level" "WARNING" "$level (not logical)" "Suboptimal" "Yellow"
            }
        } else {
            Write-TableRow "WAL Level" "ERROR" "Cannot check WAL level" "Unknown" "Red"
        }
        
        # Check max_wal_senders
        $walSendersQuery = "SELECT setting FROM pg_settings WHERE name = 'max_wal_senders';"
        $walSenders = Invoke-PostgresQuery "source" $walSendersQuery
        
        if ($walSenders) {
            $senders = [int]($walSenders.Trim())
            if ($senders -ge 2) {
                Write-TableRow "WAL Senders" "OK" "$senders senders available" "Sufficient" "Green"
            } else {
                Write-TableRow "WAL Senders" "WARNING" "$senders senders (low)" "Increase" "Yellow"
            }
        } else {
            Write-TableRow "WAL Senders" "ERROR" "Cannot check senders" "Unknown" "Red"
        }
        
    } catch {
        Write-Host "Error checking PeerDB flow health: $_" -ForegroundColor Red
    }
}

function Show-AnalysisSummary {
    param($selectedFiles)
    
    Write-Header "10. Analysis Summary & Recommendations"
    
    $summary = @()
    
    if ($selectedFiles.StressTestFile) {
        Write-Host "Latest INSERT Test Summary:" -ForegroundColor Yellow
        $logContent = Get-Content $selectedFiles.StressTestFile.FullName
        
        $logContent | ForEach-Object {
            if ($_ -match "Test Duration: (.+)") {
                $summary += "  Test Duration: $($matches[1])"
            }
            if ($_ -match "Total Records Attempted: (\d+)") {
                $summary += "  Records Attempted: $($matches[1])"
            }
            if ($_ -match "Successful Operations: (\d+)") {
                $summary += "  Successful Operations: $($matches[1])"
            }
            if ($_ -match "Success Rate: (.+)%") {
                $summary += "  Success Rate: $($matches[1])%"
            }
            if ($_ -match "Throughput: (.+) operations/second") {
                $summary += "  Throughput: $($matches[1]) ops/sec"
            }
        }
        
        if ($summary.Count -gt 0) {
            foreach ($item in $summary) { 
                Write-Host $item -ForegroundColor White 
            }
        } else {
            Write-Host "  No performance data found in log" -ForegroundColor Yellow
        }
    } else {
        Write-Host "No INSERT test log selected." -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "Resource Usage Summary:" -ForegroundColor Yellow
    if ($selectedFiles.ResourceUsageFile) {
        $resourceContent = Get-Content $selectedFiles.ResourceUsageFile.FullName
        
        # Analyze INSERT batch performance
        $insertBatches = @()
        for ($i = 0; $i -lt $resourceContent.Length; $i++) {
            if ($resourceContent[$i] -match 'INSERT-BATCH-(\d+)') {
                $batchNumber = [int]$matches[1]
                $endIndex = $i + 30
                if ($endIndex -ge $resourceContent.Length) { $endIndex = $resourceContent.Length - 1 }
                
                $batchData = $resourceContent[$i..$endIndex]
                $batchCpu = 0
                $batchMemoryMB = 0
                $containerCount = 0
                
                for ($j = 0; $j -lt $batchData.Length; $j++) {
                    $line = $batchData[$j]
                    if ($line -match '^(\S+)\s+(\d+\.\d+)%\s+(\d+\.\d+)(\w+)iB') {
                        $containerName = $matches[1]
                        $cpuPercent = [double]$matches[2]
                        $memoryValue = [double]$matches[3]
                        $memoryUnit = $matches[4]
                        
                        # Focus on key containers for batch analysis
                        if ($containerName -like "*flow*" -or $containerName -like "*catalog*" -or $containerName -like "*temporal*") {
                            $memoryMB = switch ($memoryUnit) {
                                "M" { $memoryValue }
                                "G" { $memoryValue * 1024 }
                                "K" { $memoryValue / 1024 }
                                default { $memoryValue }
                            }
                            
                            $batchCpu += $cpuPercent
                            $batchMemoryMB += $memoryMB
                            $containerCount++
                        }
                    }
                }
                
                if ($containerCount -gt 0) {
                    $insertBatches += [PSCustomObject]@{
                        Batch = $batchNumber
                        AvgCPU = [math]::Round($batchCpu / $containerCount, 2)
                        TotalMemoryMB = [math]::Round($batchMemoryMB, 0)
                    }
                }
            }
        }
        
        # Display INSERT batch resource analysis
        if ($insertBatches.Count -gt 0) {
            Write-Host "  INSERT Batch Resource Analysis:" -ForegroundColor Cyan
            
            $maxCpuBatch = $insertBatches | Sort-Object AvgCPU -Descending | Select-Object -First 1
            $minCpuBatch = $insertBatches | Sort-Object AvgCPU | Select-Object -First 1
            $avgCpu = ($insertBatches | Measure-Object -Property AvgCPU -Average).Average
            $avgMemory = ($insertBatches | Measure-Object -Property TotalMemoryMB -Average).Average
            
            Write-Host "    Total INSERT Batches: $($insertBatches.Count)" -ForegroundColor White
            Write-Host "    Average CPU (per batch): $([math]::Round($avgCpu, 2))%" -ForegroundColor White
            Write-Host "    Average Memory (per batch): $([math]::Round($avgMemory / 1024, 2)) GB" -ForegroundColor White
            Write-Host "    Peak CPU Batch: #$($maxCpuBatch.Batch) ($($maxCpuBatch.AvgCPU)%)" -ForegroundColor Yellow
            Write-Host "    Lowest CPU Batch: #$($minCpuBatch.Batch) ($($minCpuBatch.AvgCPU)%)" -ForegroundColor Green
            
            # Performance trend analysis
            if ($insertBatches.Count -ge 3) {
                $firstThird = $insertBatches | Select-Object -First ([math]::Floor($insertBatches.Count / 3))
                $lastThird = $insertBatches | Select-Object -Last ([math]::Floor($insertBatches.Count / 3))
                
                $earlyAvgCpu = ($firstThird | Measure-Object -Property AvgCPU -Average).Average
                $lateAvgCpu = ($lastThird | Measure-Object -Property AvgCPU -Average).Average
                $cpuTrend = $lateAvgCpu - $earlyAvgCpu
                
                $trendColor = if ([math]::Abs($cpuTrend) -le 1) { "Green" } elseif ([math]::Abs($cpuTrend) -le 3) { "Yellow" } else { "Red" }
                Write-Host "    CPU Trend (early vs late): $(if ($cpuTrend -gt 0) { '+' })$([math]::Round($cpuTrend, 2))%" -ForegroundColor $trendColor
            }
        }
        
        # Show baseline vs final comparison
        $phases = @("BASELINE", "FINAL")
        foreach ($phase in $phases) {
            $phaseData = $resourceContent | Where-Object { $_ -match $phase }
            if ($phaseData.Count -gt 0) {
                Write-Host "  $phase phase logged with container stats" -ForegroundColor White
            }
        }
    } else {
        Write-Host "  No resource usage log selected" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "Recommendations:" -ForegroundColor Yellow
    Write-Host "  - Monitor resource usage and replication lag regularly" -ForegroundColor White
    Write-Host "  - Check PeerDB UI for visual flow monitoring and configuration" -ForegroundColor White
    Write-Host "  - Validate data integrity between source and target databases" -ForegroundColor White
    Write-Host "  - Set up automated alerts for flow failures and lag spikes" -ForegroundColor White
    Write-Host "  - Run INSERT tests periodically to validate pipeline performance" -ForegroundColor White
    Write-Host "  - Monitor container health and restart unhealthy containers" -ForegroundColor White
}

# ===================================================================
# MAIN EXECUTION - SIMPLIFIED WITH AUTO ANALYSIS
# ===================================================================

Clear-Host
Write-Host "======================================" -ForegroundColor Magenta
Write-Host "  PeerDB Pipeline Monitor" -ForegroundColor Magenta
Write-Host "  Analysis of Performance" -ForegroundColor Magenta
Write-Host "======================================" -ForegroundColor Magenta
Write-Host "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
Write-Host ""

try {
    # Step 1: Get all available log files
    $logFiles = Get-AllLogFiles
    
    # Step 2: File Selection
    Write-Host "STEP 1: Log File Selection" -ForegroundColor Cyan
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    $selectedFiles = Select-LogFiles $logFiles
    
    if (-not $selectedFiles.Valid) {
        Write-Host "No files selected. Exiting..." -ForegroundColor Red
        exit
    }
    
    # Step 3: Run Complete Analysis Automatically
    Write-Host ""
    Write-Host "STEP 2: Running Complete PeerDB Analysis..." -ForegroundColor Cyan
    Write-Host ("=" * 80) -ForegroundColor Gray
    Write-Host ""
    
    # Analysis 1: System Resource Usage (from selected log)
    Show-SystemPerformancePhases $selectedFiles
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 2: Real-time Server Health Check
    Get-PeerDBServerHealth
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 3: Database Table Statistics
    Get-PeerDBTableStats
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 4: PeerDB Flow Analysis
    Get-PeerDBFlowAnalysis
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 5: CDC Operations Analysis
    Get-CDCOperationsAnalysis $selectedFiles
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 6: Container Health Status
    Get-ContainerHealthStatus
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 7: Performance Summary
    Get-PerformanceSummary
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 8: Database Performance Metrics
    Get-DatabaseMetrics
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 9: PeerDB Flow Health & Configuration
    Get-PeerDBFlowHealth
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Analysis 10: Complete Analysis Summary & Recommendations
    Show-AnalysisSummary $selectedFiles
    Write-Host ("=" * 80) -ForegroundColor Gray
    
    # Final Summary
    Write-Host ""
    Write-Host "ANALYSIS COMPLETE!" -ForegroundColor Green
    Write-Host "======================================" -ForegroundColor Magenta
    Write-Host "Selected Files:" -ForegroundColor Yellow
    if ($selectedFiles.StressTestFile) {
        Write-Host "  Stress Test: $($selectedFiles.StressTestFile.Name)" -ForegroundColor White
    }
    if ($selectedFiles.ResourceUsageFile) {
        Write-Host "  Resource Usage: $($selectedFiles.ResourceUsageFile.Name)" -ForegroundColor White
    }
    Write-Host "Report Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
    Write-Host "======================================" -ForegroundColor Magenta
    
} catch {
    Write-Host "Error during monitoring: $($_.Exception.Message)" -ForegroundColor Red
}

# ===================================================================
# END OF SCRIPT
# ===================================================================
