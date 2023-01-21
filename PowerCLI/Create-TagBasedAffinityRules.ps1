<#
.Synopsis
   For each cluster in the specified vCenter, create (or update) and apply tag-based DRS affinity rules
.DESCRIPTION
   Script will iterate through all clusters in the specified vCenter and attempt to perform the following actions:

   For each cluster, find all hosts and VMs with the specified host/VM affinity vCenter tags, then create appropriate Host/VM DRS groups which contain
   all of the specified objects with those tags.

   If both groups were successfully created, attempt to create a DRS rule binding the two groups together with the specified ruletype and enablement properties.
.EXAMPLE
    Create a "MustRunOn" ruleset between specified vCenter tags in target vCenter

    Create-TagBasedAffinityRules.ps1 -vCenter "TestvCenter" -VMGroupCategory "SQLStatus" -VMGroupTag SQLInstalled -HostGroupCategory "DRSAffinity" -HostGroupTag "SQLLicensed" -RuleType MustRunOn
.EXAMPLE
    Create a default "ShouldRunOn" ruleset, but start disabled

    Create-TagBasedAffinityRules.ps1 -vCenter "TestvCenter" -VMGroupCategory "SQLStatus" -VMGroupTag SQLInstalled -HostGroupCategory "DRSAffinity" -HostGroupTag "SQLLicensed" -RuleEnabled:$false
.NOTES
   v1.0
   Eric Djerf 8/16/2020
#>

Param
(
    # vCenter Name
    [Parameter(Mandatory = $true)]
    [ValidateNotNull()]
    [ValidateNotNullOrEmpty()]
    [string]
    $vCenter,

    # VMGroup Tag Category
    [Parameter(Mandatory = $true)]
    [ValidateNotNull()]
    [ValidateNotNullOrEmpty()]
    [string]
    $VMGroupCategory,

    # VMGroup Tag Name
    [Parameter(Mandatory = $true)]
    [ValidateNotNull()]
    [ValidateNotNullOrEmpty()]
    [string]
    $VMGroupTag,

    # HostGroup Tag Category
    [Parameter(Mandatory = $true)]
    [ValidateNotNull()]
    [ValidateNotNullOrEmpty()]
    [string]
    $HostGroupCategory,

    # HostGroup Tag Name
    [Parameter(Mandatory = $true)]
    [ValidateNotNull()]
    [ValidateNotNullOrEmpty()]
    [string]
    $HostGroupTag,

    # Specifies the type of affinity rule to create. This parameter accepts MustRunOn, ShouldRunOn, MustNotRunOn, and ShouldNotRunOn values. Defaults to ShouldRunOn
    [Parameter(Mandatory = $false)]
    [ValidateNotNull()]
    [ValidateNotNullOrEmpty()]
    [ValidateSet("MustRunOn", "ShouldRunOn", "MustNotRunOn", "ShouldNotRunOn")]
    [string]
    $RuleType = "ShouldRunOn",

    # Specifies if the created rule will be enabled or disabled. Defaults to Enabled
    [Parameter(Mandatory = $false)]
    [bool]
    $RuleEnabled = $true,

    # Optional Credential for vCenter connection
    [Parameter(Mandatory = $false)]
    [System.Management.Automation.PSCredential]
    $Credential = $null,

    # Enable Transcript Logging. Logs are sent to "Logs" directory in same path as script.
    [Parameter(Mandatory = $false)]
    [switch]
    $Log
)

Function Start-Log {
    Param (
        [int]$KeepLog = 15
    )

    $Script:VerbosePreference = "Continue"
    $LogPath = Join-Path -Path (Split-Path $Script:MyInvocation.MyCommand.Path) -ChildPath "Logs"
    If (-not (Test-Path $LogPath) ) {
        Try {
            New-Item -Path $LogPath -ItemType Directory -ErrorAction Stop | Out-Null
        } Catch {
            Write-Error "Unable to create log folder because ""$($Error[0])"", no logging will be available for this script"
            Return
        }
    }
    $LogPathName = Join-Path -Path $LogPath -ChildPath "$($Script:MyInvocation.MyCommand.Name)-$(Get-Date -Format 'MM-dd-yyyy').log"
    Try {
        Start-Transcript $LogPathName -Append -ErrorAction Stop
        Write-Verbose "$(Get-Date): Logging for $($Script:MyInvocation.MyCommand.Name) started"
    } Catch {
        Write-Error "Unable to create transcript log because ""$($Error[0])"""
        Return
    }
    If (@(Get-ChildItem "$LogPath\*.log" | Where-Object LastWriteTime -LT (Get-Date).AddDays(-$KeepLog)).Count -gt 0) {
        Write-Verbose "$(Get-Date): Removing old log files:"
        Get-ChildItem "$LogPath\*.log" | Where-Object LastWriteTime -LT (Get-Date).AddDays(-$KeepLog) | Remove-Item -Confirm:$false
    }
}
Function Stop-Log {
    Write-Verbose "$(Get-Date): Logging for $($Script:MyInvocation.MyCommand.Name) completed"
    $Script:VerbosePreference = "SilentlyContinue"
    Stop-Transcript
}

if ($Log) {
    Start-Log
}

Write-Output "Connecting to vCenter $vCenter"

try {
    if ($Credential) {
        $targetVC = Connect-VIServer $vCenter -ErrorAction Stop -Credential $Credential
    } else {
        $targetVC = Connect-VIServer $vCenter -ErrorAction Stop
    }
} catch {
    Write-Output "Unable to connect to vCenter $($vCenter):"
    Write-Output $_
    if ($Log) {
        Stop-Log
    }
    exit
}

$targetClusters = Get-Cluster
Write-Output $targetClusters

foreach ($cluster in $targetClusters) {
    
    ###
    #region Host Group Configuration
    ###
    # Find all Hosts in cluster with the HostGroup tag, if there are some, then we have to either create a new host group and add them OR add them to an existing host group

    $vmHosts = $cluster | Get-VMHost
    if ($vmHosts) {
        # Handle empty cluster corner case - get-tagassignment will possibly return values even if the specified entity is null
        $hostTagAssignments = Get-TagAssignment -Category $HostGroupCategory -Entity $vmHosts | Where-Object { $_.Tag.Name -eq $HostGroupTag }
    } else {
        $hostTagAssignments = $null
    }
    $hostGroupName = $HostGroupCategory + "_" + $HostGroupTag + "_h"

    if ($hostTagAssignments) {
        Write-Output "$($hostTagAssignments.count) host tag assignments for '$($HostGroupCategory)/$($HostGroupTag)' found in cluster $cluster"

        $taggedHostObjects = $hostTagAssignments | Select-Object -ExpandProperty Entity
        $untaggedHostObjects = $vmHosts | Where-Object { $_ -notin $taggedHostObjects }

        # Check if a DRS Host group for this tag definition exists in the current cluster, if not, create it and assign the tagged VMs to it.
        $drsHostGroup = $cluster | Get-DrsClusterGroup -Type VMHostGroup -Name $hostGroupName -ErrorAction SilentlyContinue
        if (-not $drsHostGroup) {
            Write-Output "Creating new DRS Host group '$hostGroupName' in cluster $cluster"
            $drsHostGroup = New-DrsClusterGroup -Name $hostGroupName -Cluster $cluster -VMHost $taggedHostObjects
        } else {
            # If the DRS Host group does exist, then two things need to be done: Tagged hosts need to be added if not already there, and untagged hosts need to be removed.
            $drsHostGroup | Set-DrsClusterGroup -Add -VMHost $taggedHostObjects
            $drsHostGroup | Set-DrsClusterGroup -Remove -VMHost $untaggedHostObjects
        }
    } else {
        Write-Output "No host tag assignments for '$($hostGroupCategory)/$($hostGroupTag)' found in cluster $cluster, skipped host group creation."
        $drsHostGroup = $null
    }
    #endregion

    ###
    #region VM Group Configuration
    ###
    # Find all VMs in cluster with the VMGroup tag, if there are some, then we have to either create a new VM group and add them OR add them to an existing VM group
    $VMs = $cluster | Get-VM
    if ($VMs) {
        # Get-Tagassignment may return tagassignments even if the passed entities happen to be null (will return all tag assignments which match criteria) - we only want
        # tag assignments if there are objects in the cluster to retrieve from - handles empty cluster corner case
        $VMTagAssignments = Get-TagAssignment -Category $VMGroupCategory -Entity $VMs | Where-Object { $_.Tag.Name -eq $VMGroupTag }
    } else {
        $VMTagAssignments = $null
    }
    $VMGroupName = $VMGroupCategory + "_" + $VMGroupTag + "_v"

    if ($VMTagAssignments) {
        Write-Output "$($VMTagAssignments.count) VM tag assignments for '$($VMGroupCategory)/$($VMGroupTag)' found in cluster $cluster"

        $taggedVMObjects = $VMTagAssignments | Select-Object -ExpandProperty Entity
        $untaggedVMObjects = $VMs | Where-Object { $_ -notin $taggedVMObjects }

        # Check if a DRS VM group for this tag definition exists in the current cluster, if not, create it and assign the tagged VMs to it.
        $drsVMGroup = $cluster | Get-DrsClusterGroup -Type VMGroup -Name $VMGroupName -ErrorAction SilentlyContinue
        if (-not $drsVMGroup) {
            Write-Output "Creating new DRS VM group '$VMGroupName' in cluster $cluster"
            $drsVMGroup = New-DrsClusterGroup -Name $VMGroupName -Cluster $cluster -VM $taggedVMObjects
        } else {
            # If the DRS VM group does exist, then two things need to be done: Tagged VMs need to be added if not already there, and untagged VMs need to be removed.
            $drsVMGroup | Set-DrsClusterGroup -Add -VM $taggedVMObjects
            $drsVMGroup | Set-DrsClusterGroup -Remove -VM $untaggedVMObjects
        }
    } else {
        Write-Output "No VM tag assignments for '$($VMGroupCategory)/$($VMGroupTag)' found in cluster $cluster, skipped VM group creation."
        $drsVMGroup = $null
    }
    #endregion

    ###
    #region DRS Rule Creation
    ###
    # Only attempt to create the rule if valid host groups and valid VM groups have both been created and/or discovered
    if ($drsHostGroup -and $drsVMGroup) {
        $drsAffinityRule = Get-DrsVMHostRule -Cluster $cluster -VMGroup $drsVMGroup -VMHostGroup $drsHostGroup
        $drsAffinityRuleName = $VMGroupName + "_$($RuleType)_" + $hostGroupName
        # If rule doesn't exist, create new, if it does exist, update it to match the current spec.
        if (-not $drsAffinityRule) {
            Write-Output "Creating new DRS affinity rule $drsAffinityRuleName - Enabled:$RuleEnabled"
            New-DrsVMHostRule -Name $drsAffinityRuleName -Cluster $cluster -VMGroup $drsVMGroup -VMHostGroup $drsHostGroup -Type $RuleType -Enabled:$RuleEnabled
        } else {
            #Update
            Write-Output "Updating rule $($drsAffinityRule.Name) with rulespec:"
            Write-Output "$drsAffinityRuleName - Enabled:$RuleEnabled"
            $drsAffinityRule | Set-DrsVMHostRule -Name $drsAffinityRuleName -VMGroup $drsVMGroup -VMHostGroup $drsHostGroup -Type $RuleType -Enabled:$RuleEnabled
        }
    } else {
        Write-Output "$($cluster.Name) missing both hosts and VMs matching the specified affinity tags, Skipped rule creation. HostTag '$HostGroupCategory/$HostGroupTag' VMTag '$VMGroupCategory/$VMGroupTag'"
    }
    #endregion
}
Disconnect-VIServer -Server $targetVC -Confirm:$false

if ($Log) {
    Stop-Log
}