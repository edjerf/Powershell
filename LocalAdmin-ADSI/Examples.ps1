using namespace System.Collections.Concurrent
Import-Module ./LocalAdmin-ADSI.psm1

# Get some list of computer objects from AD to work on - this could also be adapted to ingest a file or CSV
$testComputers = Get-ADGroupMember "Server Test Group" | Where-Object { $_.objectClass -eq 'computer' }

#
# Poll computers for their local Administrator membership and store in a hashtable - serial
#
$serialTimer =  [system.diagnostics.stopwatch]::StartNew()
$htResults = @{}
foreach ($testComputer in $testComputers) {
    $adminGroups = Get-LocalAdminsADSI -ComputerName $testComputer.name
    $htResults.Add($testComputer.name, $adminGroups)
}
$serialTimer.Stop()
Write-Output "Serial execution complete - $($serialTimer.Elapsed.TotalSeconds)"
# $htResults.GetEnumerator()

#
# Parallel version of the above compatible with powershell 7+
#
$parallelTimer =  [system.diagnostics.stopwatch]::StartNew()
$cdResults = [ConcurrentDictionary[string, object]]::new()
$testComputers | ForEach-Object -Parallel {
    Import-Module ./LocalAdmin-ADSI.psm1
    $adminGroups = Get-LocalAdminsADSI -ComputerName $_.name
    $localResult = $using:cdResults
    $cdAddResult = $localResult.TryAdd($_.name, $adminGroups)

    if (-not $cdAddResult) {
        Write-Warning "Unable to add $($_.name) to results dict."
    }
}
$parallelTimer.Stop()
$parallelTimer.Elapsed
Write-Output "Parallel execution complete - $($parallelTimer.Elapsed.TotalSeconds)"
# $cdResults.GetEnumerator() # Output Results

# Search results to identify computers with a particular group
$computerNamesWithoutGroup = @()
foreach ($item in $cdResults.GetEnumerator()) {
    if ($item.Value -notContains "WinNT://TEXAS/Admin Datacenter") {
        $computerNamesWithoutGroup += $item.Key
    }
}
$computerNamesWithoutGroup.count


#
# Add group to list of servers
#
$computerNames = "testserver1","testserver2","testserver3"
$serialTimer =  [system.diagnostics.stopwatch]::StartNew()
foreach ($computerName in $computerNames) {
    Add-MemberToLocalAdminADSI -ComputerName $computerName -MemberName "Test AD Group"
}
$serialTimer.Stop()
Write-Output "Serial execution complete - $($serialTimer.Elapsed.TotalSeconds)"
#
# Remove group from list of servers
#
$computerNames = "testserver1","testserver2","testserver3"
$serialTimer =  [system.diagnostics.stopwatch]::StartNew()
foreach ($computerName in $computerNames) {
    Remove-MemberFromLocalAdminADSI -ComputerName $computerName -MemberName "Test AD Group"
}
$serialTimer.Stop()
Write-Output "Serial execution complete - $($serialTimer.Elapsed.TotalSeconds)"


#
# Parallel versions of the above compatible with Powershell 7+
#
#Add
$computerNames = "testserver1","testserver2","testserver3"
$parallelTimer =  [system.diagnostics.stopwatch]::StartNew()
$computerNames | ForEach-Object -Parallel {
    Import-Module ./LocalAdmin-ADSI.psm1
    Add-MemberToLocalAdminADSI -ComputerName $_ -MemberName "Test AD Group"
}
$parallelTimer.Stop()
$parallelTimer.Elapsed
Write-Output "Parallel execution complete - $($parallelTimer.Elapsed.TotalSeconds)"
#Remove
$computerNames = "testserver1","testserver2","testserver3"
$parallelTimer =  [system.diagnostics.stopwatch]::StartNew()
$computerNames | ForEach-Object -Parallel {
    Import-Module ./LocalAdmin-ADSI.psm1
    Remove-MemberFromLocalAdminADSI -ComputerName $_ -MemberName "Test AD Group"
}
$parallelTimer.Stop()
$parallelTimer.Elapsed
Write-Output "Parallel execution complete - $($parallelTimer.Elapsed.TotalSeconds)"