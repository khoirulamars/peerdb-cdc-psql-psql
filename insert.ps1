# ===================================================================
# PeerDB INSERT Stress Test ENHANCED - Unique Resource Logging
# ===================================================================
# Purpose: Test PeerDB CDC pipeline dengan INSERT dan log resource docker tiap batch dengan timestamp unik
# Author: PeerDB Team  
# Version: 3.0 (Enhanced with unique resource logs)
# Last Updated: 2025-08-25
# ===================================================================

param(
    [int]$RecordCount = 5000,
    [int]$BatchSize = 500,
    [int]$DelayBetweenBatches = 1,
    [switch]$ShowProgress
)

# ===================================================================
# CONFIGURATION and VARIABLES
# ===================================================================

$script:METRICS = @{
    StartTime = $null
    EndTime = $null
    TotalRecords = 0
    SuccessfulOperations = 0
    FailedOperations = 0
    BatchTimes = @()
    Errors = @()
    CustomerIds = @()
    ProductIds = @()
    TestId = $null
}

$Colors = @{
    Info = "Cyan"
    Success = "Green" 
    Warning = "Yellow"
    Error = "Red"
    Progress = "Blue"
}

# Generate unique test ID dengan timestamp
$script:TestId = Get-Date -Format "yyyy-MM-dd-HH-mm-ss"
$LogPath = "testing-results/peerdb-stress-test-$($script:TestId).log"
$ResourceLogPath = "testing-results/peerdb-resource-usage-$($script:TestId).log"

# ===================================================================
# UTILITY FUNCTIONS
# ===================================================================

function Test-Prerequisites {
    Write-Log "Checking PeerDB prerequisites..." "Info"
    
    # Check PeerDB catalog container
    $catalogStatus = docker ps --filter "name=catalog" --format "{{.Status}}" 2>$null
    if (-not $catalogStatus -or $catalogStatus -notmatch "Up") {
        Write-Log "PeerDB catalog container is not running" "Error"
        return $false
    }
    
    # Test PeerDB catalog connection
    try {
        $testResult = docker exec catalog psql -U postgres -d postgres -c "SELECT 1;" 2>$null
        if ($testResult -match "1") {
            Write-Log "PeerDB Catalog: Connected" "Success"
        } else {
            Write-Log "PeerDB catalog connection failed" "Error"
            return $false
        }
    } catch {
        Write-Log "Cannot connect to PeerDB catalog: $($_.Exception.Message)" "Error"
        return $false
    }
    
    # Test source database connection
    try {
        $sourceTest = docker exec catalog psql -U postgres -d source -c "SELECT 1;" 2>$null
        if ($sourceTest -match "1") {
            Write-Log "Source Database: Connected" "Success"
        } else {
            Write-Log "Source database connection failed" "Error"
            return $false
        }
    } catch {
        Write-Log "Cannot connect to source database: $($_.Exception.Message)" "Error"
        return $false
    }
    
    Write-Log "All PeerDB prerequisites met!" "Success"
    return $true
}

function Get-ExistingCustomers {
    try {
        Write-Log "Fetching existing customers from source database..." "Info"
        $result = docker exec catalog psql -U postgres -d source -c "SELECT id FROM customers;" 2>$null
        $customerIds = @()
        $result -split "`n" | ForEach-Object {
            if ($_ -match '^\s*(\d+)\s*$') {
                $customerIds += [int]$matches[1]
            }
        }
        if ($customerIds.Count -eq 0) {
            Write-Log "No customers found. Using existing customer IDs from orders table..." "Warning"
            # Get existing customer IDs from orders table
            $result = docker exec catalog psql -U postgres -d source -c "SELECT DISTINCT purchaser FROM orders LIMIT 10;" 2>$null
            $result -split "`n" | ForEach-Object {
                if ($_ -match '^\s*(\d+)\s*$') {
                    $customerIds += [int]$matches[1]
                }
            }
        }
        Write-Log "Found $($customerIds.Count) customer IDs: $($customerIds -join ', ')" "Success"
        return $customerIds
    } catch {
        throw "Failed to fetch customers: $($_.Exception.Message)"
    }
}

function Get-ExistingProducts {
    try {
        Write-Log "Fetching existing products from source database..." "Info"
        $result = docker exec catalog psql -U postgres -d source -c "SELECT id FROM products;" 2>$null
        $productIds = @()
        $result -split "`n" | ForEach-Object {
            if ($_ -match '^\s*(\d+)\s*$') {
                $productIds += [int]$matches[1]
            }
        }
        if ($productIds.Count -eq 0) {
            Write-Log "No products found. Using existing product IDs from orders table..." "Warning"
            # Get existing product IDs from orders table
            $result = docker exec catalog psql -U postgres -d source -c "SELECT DISTINCT product_id FROM orders LIMIT 10;" 2>$null
            $result -split "`n" | ForEach-Object {
                if ($_ -match '^\s*(\d+)\s*$') {
                    $productIds += [int]$matches[1]
                }
            }
        }
        Write-Log "Found $($productIds.Count) product IDs: $($productIds -join ', ')" "Success"
        return $productIds
    } catch {
        throw "Failed to fetch products: $($_.Exception.Message)"
    }
}

function Get-NextOrderId {
    try {
        $result = docker exec catalog psql -U postgres -d source -c "SELECT MAX(id) FROM orders;" 2>$null
        $maxId = $result -split "`n" | Where-Object { $_ -match '^\s*(\d+)\s*$' } | ForEach-Object { [int]$matches[1] } | Select-Object -First 1
        if ($maxId -eq $null) {
            return 1
        }
        return $maxId + 1
    } catch {
        return 1
    }
}

function Get-CurrentOrderCount {
    param([string]$Database = "source")
    try {
        $result = docker exec catalog psql -U postgres -d $Database -c "SELECT COUNT(*) FROM orders;" 2>$null
        $count = $result -split "`n" | Where-Object { $_ -match '^\s*(\d+)\s*$' } | ForEach-Object { [int]$matches[1] } | Select-Object -First 1
        return $count
    } catch {
        return 0
    }
}

function Write-Log {
    param([string]$Message, [string]$Type = "Info")
    $timestamp = Get-Date -Format "HH:mm:ss"
    $color = $Colors[$Type]
    $fullMessage = "[$timestamp] $Message"
    Write-Host $fullMessage -ForegroundColor $color
    Add-Content -Path $LogPath -Value $fullMessage
}

function Write-ResourceUsage {
    param([string]$Phase)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logLine = "$timestamp`t$Phase"
    
    # Log header dengan informasi test
    if ($Phase -eq "BASELINE") {
        $testInfo = @"
========================================
PeerDB INSERT Stress Test Resource Log
Test ID: $($script:TestId)
Test Start: $timestamp
Record Count: $RecordCount
Batch Size: $BatchSize
========================================

"@
        Add-Content -Path $ResourceLogPath -Value $testInfo
    }
    
    try {
        # Get all running containers first to identify which ones exist
        $runningContainers = docker ps --format "{{.Names}}"
        
        if ($runningContainers) {
            # Filter for specific container types - updated to include all 10 containers
            $peerdbContainers = @($runningContainers | Where-Object { $_ -like "*peerdb*" -or $_ -like "*flow*" -or $_ -like "*minio*" })
            $catalogContainers = @($runningContainers | Where-Object { $_ -like "*catalog*" })
            $temporalContainers = @($runningContainers | Where-Object { $_ -like "*temporal*" })
            
            $peerdbStats = ""
            $catalogStats = ""
            $temporalStats = ""
            
            # Get PeerDB container stats
            if ($peerdbContainers.Count -gt 0) {
                $statsCmd = "docker stats --no-stream --format ""table {{.Name}}`t{{.CPUPerc}}`t{{.MemUsage}}`t{{.NetIO}}`t{{.BlockIO}}"" " + ($peerdbContainers -join " ")
                $peerdbStats = Invoke-Expression $statsCmd 2>$null | Out-String
            }
            
            # Get Catalog container stats
            if ($catalogContainers.Count -gt 0) {
                $statsCmd = "docker stats --no-stream --format ""table {{.Name}}`t{{.CPUPerc}}`t{{.MemUsage}}`t{{.NetIO}}`t{{.BlockIO}}"" " + ($catalogContainers -join " ")
                $catalogStats = Invoke-Expression $statsCmd 2>$null | Out-String
            }
            
            # Get Temporal container stats
            if ($temporalContainers.Count -gt 0) {
                $statsCmd = "docker stats --no-stream --format ""table {{.Name}}`t{{.CPUPerc}}`t{{.MemUsage}}`t{{.NetIO}}`t{{.BlockIO}}"" " + ($temporalContainers -join " ")
                $temporalStats = Invoke-Expression $statsCmd 2>$null | Out-String
            }
            
            $allStats = "PEERDB CONTAINERS:" + [Environment]::NewLine + $peerdbStats.Trim() +
                       [Environment]::NewLine + [Environment]::NewLine + "CATALOG CONTAINER:" + [Environment]::NewLine + $catalogStats.Trim() +
                       [Environment]::NewLine + [Environment]::NewLine + "TEMPORAL CONTAINERS:" + [Environment]::NewLine + $temporalStats.Trim()
        } else {
            $allStats = "(No containers found running)"
        }
    } catch { 
        $allStats = "(docker stats unavailable: $($_.Exception.Message))" 
    }
    
    Add-Content -Path $ResourceLogPath -Value "$logLine"
    Add-Content -Path $ResourceLogPath -Value ("DOCKER STATS:" + [Environment]::NewLine + $allStats)
    Add-Content -Path $ResourceLogPath -Value ("=" * 100)
    
    # Log to main log file juga untuk tracking
    Write-Log "Resource usage logged for phase: $Phase" "Info"
}

function Invoke-BulkInsertTest {
    param([int]$TotalRecords, [int]$BatchSize, [array]$CustomerIds, [array]$ProductIds)
    
    Write-Log "Starting PeerDB bulk INSERT test: $TotalRecords records in batches of $BatchSize" "Info"
    $successCount = 0
    $batchNumber = 0
    $totalBatches = [math]::Ceiling($TotalRecords / $BatchSize)
    $currentOrderId = Get-NextOrderId
    
    for ($startRecord = 0; $startRecord -lt $TotalRecords; $startRecord += $BatchSize) {
        $batchNumber++
        $endRecord = [math]::Min($startRecord + $BatchSize, $TotalRecords)
        $currentBatchSize = $endRecord - $startRecord
        
        if ($ShowProgress) {
            Write-Log "Processing batch $batchNumber/$totalBatches (records $($startRecord + 1)-$endRecord)" "Progress"
        }
        
        $batchStart = Get-Date
        $insertValues = @()
        
        for ($i = 0; $i -lt $currentBatchSize; $i++) {
            $customerId = $CustomerIds | Get-Random
            $productId = $ProductIds | Get-Random
            $quantity = Get-Random -Minimum 1 -Maximum 100
            $orderDate = (Get-Date).AddDays(-(Get-Random -Minimum 0 -Maximum 30)).ToString("yyyy-MM-dd")
            $insertValues += "($currentOrderId, '$orderDate', $customerId, $quantity, $productId)"
            $currentOrderId++
        }
        
        $insertQuery = @"
INSERT INTO orders (id, order_date, purchaser, quantity, product_id)
VALUES $($insertValues -join ', ');
"@
        
        try {
            $result = docker exec catalog psql -U postgres -d source -c $insertQuery 2>&1
            if ($LASTEXITCODE -eq 0 -and $result -match "INSERT 0 $currentBatchSize") {
                $successCount += $currentBatchSize
                $script:METRICS.SuccessfulOperations += $currentBatchSize
                if ($ShowProgress) {
                    Write-Log "Batch $batchNumber successful: $currentBatchSize records inserted" "Success"
                }
            } else {
                $script:METRICS.FailedOperations += $currentBatchSize
                $errorMsg = "Batch $batchNumber failed - Result: $result, Exit Code: $LASTEXITCODE"
                $script:METRICS.Errors += $errorMsg
                Write-Log $errorMsg "Error"
            }
        } catch {
            $script:METRICS.FailedOperations += $currentBatchSize
            $errorMsg = "Batch $batchNumber exception: $($_.Exception.Message)"
            $script:METRICS.Errors += $errorMsg
            Write-Log $errorMsg "Error"
        }
        
        $batchEnd = Get-Date
        $batchDuration = ($batchEnd - $batchStart).TotalMilliseconds
        $script:METRICS.BatchTimes += $batchDuration
        
        # Log resource usage per batch dengan timestamp unik
        Write-ResourceUsage -Phase "INSERT-BATCH-$batchNumber"
        
        # Log batch summary
        $failedOps = $script:METRICS.FailedOperations
        $batchLog = "Batch $batchNumber completed: $currentBatchSize records, $([math]::Round($batchDuration, 2)) ms, Total Success: $successCount, Total Failed: $failedOps"
        Add-Content -Path $LogPath -Value $batchLog
        
        # Progress update every 5 batches untuk lebih frequent updates
        if ($batchNumber % 5 -eq 0) {
            $progress = [math]::Round(($endRecord / $TotalRecords) * 100, 1)
            $avgBatchTime = if ($script:METRICS.BatchTimes.Count -gt 0) {
                [math]::Round(($script:METRICS.BatchTimes | Measure-Object -Average).Average, 2)
            } else { 0 }
            Write-Log "Progress: $progress% | Success: $successCount/$endRecord | Avg Batch: ${avgBatchTime}ms | Test ID: $($script:TestId)" "Progress"
        }
        
        if ($DelayBetweenBatches -gt 0 -and $batchNumber -lt $totalBatches) {
            Start-Sleep -Seconds $DelayBetweenBatches
        }
    }
    
    Write-Log "PeerDB bulk INSERT completed: $successCount/$TotalRecords records inserted successfully" "Success"
    return $successCount
}

function Show-Results {
    $script:METRICS.EndTime = Get-Date
    $totalDuration = $script:METRICS.EndTime - $script:METRICS.StartTime
    
    Write-Log "" "Info"
    Write-Log "PEERDB INSERT PERFORMANCE RESULTS (Test ID: $($script:TestId))" "Success"
    Write-Log "=======================================================" "Success"
    Write-Log "Test Start Time: $($script:METRICS.StartTime.ToString('yyyy-MM-dd HH:mm:ss'))" "Info"
    Write-Log "Test End Time: $($script:METRICS.EndTime.ToString('yyyy-MM-dd HH:mm:ss'))" "Info"
    Write-Log "Test Duration: $($totalDuration.ToString('hh\:mm\:ss'))" "Info"
    Write-Log "Total Records Attempted: $($script:METRICS.TotalRecords)" "Info"
    Write-Log "Successful Operations: $($script:METRICS.SuccessfulOperations)" "Success"
    Write-Log "Failed Operations: $($script:METRICS.FailedOperations)" "Error"
    
    $successRate = if ($script:METRICS.TotalRecords -gt 0) {
        [math]::Round(($script:METRICS.SuccessfulOperations / $script:METRICS.TotalRecords) * 100, 2)
    } else { 0 }
    Write-Log "Success Rate: $successRate%" "Info"
    
    $throughput = if ($totalDuration.TotalSeconds -gt 0) {
        [math]::Round($script:METRICS.SuccessfulOperations / $totalDuration.TotalSeconds, 2)
    } else { 0 }
    Write-Log "Throughput: $throughput operations/second" "Success"
    
    if ($script:METRICS.BatchTimes.Count -gt 0) {
        $avgBatchTime = [math]::Round(($script:METRICS.BatchTimes | Measure-Object -Average).Average, 2)
        $maxBatchTime = [math]::Round(($script:METRICS.BatchTimes | Measure-Object -Maximum).Maximum, 2)
        $minBatchTime = [math]::Round(($script:METRICS.BatchTimes | Measure-Object -Minimum).Minimum, 2)
        
        Write-Log "Average Batch Time: ${avgBatchTime}ms" "Info"
        Write-Log "Max Batch Time: ${maxBatchTime}ms" "Warning"
        Write-Log "Min Batch Time: ${minBatchTime}ms" "Success"
    }
    
    if ($script:METRICS.Errors.Count -gt 0) {
        Write-Log "Errors encountered: $($script:METRICS.Errors.Count)" "Error"
        $script:METRICS.Errors | Select-Object -First 3 | ForEach-Object {
            Write-Log "  $_" "Error"
        }
    }
    
    Write-Log "Log Files Generated:" "Info"
    Write-Log "  Stress Test Log: $LogPath" "Info"
    Write-Log "  Resource Usage Log: $ResourceLogPath" "Info"
    Write-Log "=======================================================" "Success"
}

function Main {
    try {
        $script:METRICS.StartTime = Get-Date
        $script:METRICS.TotalRecords = $RecordCount
        
        Write-Log "PeerDB CDC Pipeline INSERT Stress Test" "Info"
        Write-Log "===============================================" "Info"
        Write-Log "Test ID: $($script:TestId)" "Info"
        Write-Log "Target Records: $RecordCount" "Info"
        Write-Log "Batch Size: $BatchSize" "Info"
        Write-Log "Delay Between Batches: ${DelayBetweenBatches}s" "Info"
        Write-Log "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" "Info"
        Write-Log "Stress Test Log: $LogPath" "Info"
        Write-Log "Resource Usage Log: $ResourceLogPath" "Info"
        Write-Log "" "Info"
        
        if (-not (Test-Prerequisites)) {
            throw "Prerequisites check failed"
        }
        
        $customerIds = Get-ExistingCustomers
        $productIds = Get-ExistingProducts
        $script:METRICS.CustomerIds = $customerIds
        $script:METRICS.ProductIds = $productIds
        
        $initialOrderCount = Get-CurrentOrderCount "source"
        Write-Log "Initial source order count: $initialOrderCount" "Info"
        
        # Baseline resource usage dengan header informasi
        Write-ResourceUsage -Phase "BASELINE"
        
        Write-Log "Starting PeerDB INSERT stress test..." "Success"
        $insertSuccessCount = Invoke-BulkInsertTest -TotalRecords $RecordCount -BatchSize $BatchSize -CustomerIds $customerIds -ProductIds $productIds
        
        Write-Host ""
        Write-Host "Test Execution Summary (Test ID: $($script:TestId)):" -ForegroundColor Yellow
        Write-Host "  Total Test Records: $RecordCount" -ForegroundColor Cyan
        Write-Host "  Successful Operations: $insertSuccessCount" -ForegroundColor Green
        Write-Host "  Failed Operations: $($RecordCount - $insertSuccessCount)" -ForegroundColor $(if (($RecordCount - $insertSuccessCount) -eq 0) { "Green" } else { "Red" })
        
        # Wait for potential CDC processing
        Start-Sleep -Seconds 5
        
        $finalSourceCount = Get-CurrentOrderCount "source"
        $finalTargetCount = Get-CurrentOrderCount "target"
        $actualInserted = $finalSourceCount - $initialOrderCount
        
        Write-Log "Final source order count: $finalSourceCount (inserted: $actualInserted)" "Success"
        Write-Log "Final target order count: $finalTargetCount" "Success"
        Write-Log "INSERT operation success count: $insertSuccessCount" "Success"
        
        Show-Results
        
        # Final resource usage dengan summary
        Write-ResourceUsage -Phase "FINAL"
        
        # Add test completion summary to resource log
        $completionSummary = @"

========================================
Test Completion Summary
Test ID: $($script:TestId)
Test End: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
Total Duration: $((Get-Date) - $script:METRICS.StartTime)
Records Processed: $insertSuccessCount/$RecordCount
Success Rate: $(if ($RecordCount -gt 0) { [math]::Round(($insertSuccessCount / $RecordCount) * 100, 2) } else { 0 })%
========================================
"@
        Add-Content -Path $ResourceLogPath -Value $completionSummary
        
        Write-Log "" "Info"
        Write-Log "PeerDB stress test completed successfully!" "Success"
        Write-Log "Unique log files generated with Test ID: $($script:TestId)" "Success"
        Write-Log "Stress Test Log: $LogPath" "Info"
        Write-Log "Resource Usage Log: $ResourceLogPath" "Info"
        
    } catch {
        Write-Log "PeerDB stress test failed: $($_.Exception.Message)" "Error"
        if ($script:METRICS.StartTime) {
            Show-Results
        }
        throw
    }
}

# ===================================================================
# MAIN EXECUTION
# ===================================================================

Clear-Host
Write-Host "PeerDB CDC Pipeline INSERT Stress Test" -ForegroundColor Magenta
Write-Host "Unique Resource Logging per Test Run" -ForegroundColor Magenta
Write-Host "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
Write-Host "Test ID: $script:TestId" -ForegroundColor Cyan

# Create results directory if it doesn't exist
if (-not (Test-Path "testing-results")) {
    New-Item -ItemType Directory -Path "testing-results" -Force | Out-Null
    Write-Host "Created testing-results directory" -ForegroundColor Green
}

try {
    Write-Host ""
    Write-Host "Log Files to be Generated:" -ForegroundColor Yellow
    Write-Host "  Stress Test Log: $LogPath" -ForegroundColor White
    Write-Host "  Resource Usage Log: $ResourceLogPath" -ForegroundColor White
    Write-Host ""
    
    Main
    exit 0
} catch {
    Write-Host "Script execution failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# ===================================================================
# END OF SCRIPT
# ===================================================================
