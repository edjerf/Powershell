<# 
.SYNOPSIS
    Create DSCluster-local copies of specified Templates to all known datastore clusters, and place in the specified folder. Each new template is
    appended with the name of target DSC
.DESCRIPTION
    This script is intended to be used in conjunction with vRA builds (or really any provisioning framework which uses the clone-from-template
    mechanic) to allow clone operations to target copies of the template which are local to the destination datastore cluster instead of the blueprint
    default template, which is typically located in the management clusters or in the content library.

    Doing so allows the clone operation to be performed by a host local to the destination storage cluster, and thus offloaded to the underlying storage via
    VAAI, which signifigantly speeds up provisioning times while also eliminating the need to ship the data over the provisioning network and storage fabrics.

    This assumes compute clusters also have segmented storage boundries - in designs where the same underlying storage is available to all compute
    clusters or the management cluster has visibility to all storage, a solution like this may not be needed.

    This script includes LucD's fantastic Get-VIEventPlus function to greatly speed up the polling of events
.PARAMETER vCenter
    vCenter(s) to connect to, specified by name or IP. If multiple are specified, they will be processed sequentially.
.PARAMETER SourceFolder
    VMFolder, specified by name, where script should look for the source templates to be copied. Must exist and be unique within its vCenter.
.PARAMETER DestinationFolder
    VMFolder, specified by name, which the copied templates will be placed into. Must exist and be unique with its vCenter.
.PARAMETER ExcludeClusters
    Datatore cluster(s) to be excluded as targets for copying templates, specified by name. Useful for excluding special use-case clusters as copy
    targets. (Replication clusters, failover clusters, etc).
.PARAMETER ExcludeVSAN
    Specifies whether or not to exclude vSAN datastores as a target "datastore cluster" - default false.
.PARAMETER Recursion
    Specifies the search depth for SourceFolder - either search recursively or not - default false.
.EXAMPLE
    Seed-ClusterLocalTemplates.ps1 -vCenter "vc1.corp" -SourceFolder "Production" -DestinationFolder "Prod_DSLocal"
.EXAMPLE
    Seed-ClusterLocalTemplates.ps1 -vCenter "vc1.corp","vc2.corp" -SourceFolder "Production" -DestinationFolder "Prod_DSLocal"
    -ExcludeClusters "failover1","failover2"
.NOTES 
    Name : Seed-ClusterLocalTemplates
    Author : Eric Djerf
#>
param
(
    [Parameter(
        Mandatory = $true,
        ValueFromPipeline = $false)]
    $vCenter, # vCenter(s) to run against, specified by name or IP
    [Parameter(
        Mandatory = $true)]
    $SourceFolder, # VM folder by name where source templates can be found. Must exist and be unique for the vCenter(s)
    [Parameter(
        Mandatory = $true)]
    $DestinationFolder, # VM folder destination for DSCluster-local copies of templates. Must exist and be unique for the vCenter
    [Parameter(
        Mandatory = $false)]
    $ExcludeClusters, # List of Datastore clusters to exclude as targets, by name.
    [Parameter(
        Mandatory = $false)]
    [switch]$ExcludeVSAN = $false, # Specifies whether to include vsan datastores as a "target cluster" to replicate to
    [Parameter(
        Mandatory = $false)]
    [switch]$Recursion = $false # Specifies search depth for $SourceFolder, either recurse or not
)

#region Get-VIEventPlus
<#
 .SYNOPSIS  Function GET-VIEventPlus Returns vSphere events    
 .DESCRIPTION The function will return vSphere events. With
 	the available parameters, the execution time can be
 	improved, compered to the original Get-VIEvent cmdlet. 
 .NOTES  Author:  Luc Dekens   
 .PARAMETER Entity
 	When specified the function returns events for the
 	specific vSphere entity. By default events for all
 	vSphere entities are returned. 
 .PARAMETER EventType
 	This parameter limits the returned events to those
 	specified on this parameter. 
 .PARAMETER Start
 	The start date of the events to retrieve 
 .PARAMETER Finish
 	The end date of the events to retrieve. 
 .PARAMETER Recurse
 	A switch indicating if the events for the children of
 	the Entity will also be returned 
 .PARAMETER User
 	The list of usernames for which events will be returned 
 .PARAMETER System
 	A switch that allows the selection of all system events. 
 .PARAMETER ScheduledTask
 	The name of a scheduled task for which the events
 	will be returned 
 .PARAMETER FullMessage
 	A switch indicating if the full message shall be compiled.
 	This switch can improve the execution speed if the full
 	message is not needed.   
 .EXAMPLE
 	PS> Get-VIEventPlus -Entity $vm
 .EXAMPLE
 	PS> Get-VIEventPlus -Entity $cluster -Recurse:$true
 #>
function Get-VIEventPlus {
 	 
    param(
        [VMware.VimAutomation.ViCore.Impl.V1.Inventory.InventoryItemImpl[]]$Entity,
        [string[]]$EventType,
        [DateTime]$Start,
        [DateTime]$Finish = (Get-Date),
        [switch]$Recurse,
        [string[]]$User,
        [Switch]$System,
        [string]$ScheduledTask,
        [switch]$FullMessage = $false
    )

    process {
        $eventnumber = 100
        $events = @()
        $eventMgr = Get-View EventManager
        $eventFilter = New-Object VMware.Vim.EventFilterSpec
        $eventFilter.disableFullMessage = ! $FullMessage
        $eventFilter.entity = New-Object VMware.Vim.EventFilterSpecByEntity
        $eventFilter.entity.recursion = & { if ($Recurse) { "all" }else { "self" } }
        $eventFilter.eventTypeId = $EventType
        if ($Start -or $Finish) {
            $eventFilter.time = New-Object VMware.Vim.EventFilterSpecByTime
            if ($Start) {
                $eventFilter.time.beginTime = $Start
            }
            if ($Finish) {
                $eventFilter.time.endTime = $Finish
            }
        }
        if ($User -or $System) {
            $eventFilter.UserName = New-Object VMware.Vim.EventFilterSpecByUsername
            if ($User) {
                $eventFilter.UserName.userList = $User
            }
            if ($System) {
                $eventFilter.UserName.systemUser = $System
            }
        }
        if ($ScheduledTask) {
            $si = Get-View ServiceInstance
            $schTskMgr = Get-View $si.Content.ScheduledTaskManager
            $eventFilter.ScheduledTask = Get-View $schTskMgr.ScheduledTask |
            Where-Object { $_.Info.Name -match $ScheduledTask } |
            Select-Object -First 1 |
            Select-Object -ExpandProperty MoRef
        }
        if (!$Entity) {
            $Entity = @(Get-Folder -Name Datacenters)
        }
        $entity | ForEach-Object {
            $eventFilter.entity.entity = $_.ExtensionData.MoRef
            $eventCollector = Get-View ($eventMgr.CreateCollectorForEvents($eventFilter))
            $eventsBuffer = $eventCollector.ReadNextEvents($eventnumber)
            while ($eventsBuffer) {
                $events += $eventsBuffer
                $eventsBuffer = $eventCollector.ReadNextEvents($eventnumber)
            }
            $eventCollector.DestroyCollector()
        }
        $events
    }
}
#endregion Get-VIEventPlus

#region Copy-TemplateToClusterLocal
<#
Clones the given VM template(s) to a target datastore cluster local copy. Checks if destination template already exists, if it does then only
overwrite if the source template has been modified since destination was last copied. Uses the last "convertedToTemplate" event as the last
modified date for the source and target templates - event type "com.vmware.vc.vm.VmConvertedToTemplateEvent" and returns either the cloned Template objects,
or the vCenter task objects if run asynchronously.
#>
function Copy-TemplateToClusterLocal {

    param (
        # Template object(s) to use as source
        [Parameter(ValueFromPipeline = $true)]
        $Template,
        $DatastoreCluster,
        $DestinationFolder,
        $Server = $global:DefaultVIServers,
        $lookBackDays = 31, # Number of days to look back for events, default 31 assumes templates are updated roughly once a month
        [switch]$RunAsync = $false
    )
  
    begin {
        $targetTemplateFolder = Get-Folder $DestinationFolder -Server $Server
    }

    process {
        foreach ($sourceTemplate in $Template) {
            $targetTemplateName = "$($sourceTemplate.Name)_$($DatastoreCluster.Name)" # templatename_clustername
            $sourceCluster = Get-DatastoreCluster -Template $sourceTemplate -Server $Server
            if (-not $sourceCluster) {
                # No source cluster found, might be a source datastore (vsan)
                $sourceCluster = Get-Datastore | Where-Object { $_.Type -eq 'vsan' }
            }
            if ($sourceCluster -eq $DatastoreCluster) {
                # Bail if the source and target clusters are the same - no need to make a copy to the same storage
                Write-Verbose "$($sourceTemplate.Name) cluster $($sourceCluster.Name) is the same as $($DatastoreCluster.Name), skipping."
                continue
            }
            $targetTemplate = Get-Template -Name $targetTemplateName -Location $targetTemplateFolder -Server $Server -ErrorAction SilentlyContinue

            # If the target template already exists, check if the "frozen" date (i.e. converted to template) of the source is newer than the current
            # target. If it is, then delete it first before proceeding with the copy
            if ( $targetTemplate ) {
                # VI events are stored in (and returned) in UTC in the vCenter database, get most recent "converted to template" events
                $eventParams = @{
                    EventType = "com.vmware.vc.vm.VmConvertedToTemplateEvent"
                    Start     = (Get-Date).AddDays(($lookBackDays * -1)).ToUniversalTime()
                    Finish    = (Get-Date).ToUniversalTime()
                }
                $targetModifiedDate = Get-VIEventPlus @eventParams -Entity $targetTemplate | Sort-Object -Property CreatedTime | Select-Object -ExpandProperty CreatedTime -Last 1
                $sourceModifiedDate = Get-VIEventPlus @eventParams -Entity $sourceTemplate | Sort-Object -Property CreatedTime | Select-Object -ExpandProperty CreatedTime -Last 1

                if ($sourceModifiedDate -gt $targetModifiedDate ) {
                    # The target template already exists, but the source is newer than desination so we still need to copy, delete target first.
                    Write-Verbose "Found existing template $($targetTemplate.Name) with date $($targetModifiedDate.ToString()) older than source $($sourceTemplate.Name) - $($sourceModifiedDate.ToString()), removing."
                    $targetTemplate | Remove-Template -DeletePermanently -Confirm:$false
                } else {
                    # The target template exists and the destination is newer than the source, so no copy needs to be done, continue out.
                    Write-Verbose "Existing template $($targetTemplate.Name) is newer than source, skipping copy"
                    continue
                }
            }

            # Perform copy. Since we're copying cross-cluster a host connected to the DSCluster needs to be specified
            Write-Verbose "Copying template $($sourceCluster.Name):$($sourceTemplate.Name) to $($DatastoreCluster.Name):$($targetTemplateName)"
            $targetHost = $DatastoreCluster | Get-VMHost | Get-Random
            $returnObject = New-Template -Name $targetTemplateName -Template $sourceTemplate -Datastore $targetCluster -Location $targetTemplateFolder -VMHost $targetHost -Confirm:$false -RunAsync:$RunAsync
            $returnObject
        }
    }

    end {
        # Process return objects, will either be
    }
}
#endregion Copy-TemplateToClusterLocal

$executionTimer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Verbose "Started execution timer."

try {
    $connectedvCenters = Connect-VIServer $vCenter -ErrorAction Stop
} catch {
    throw "Error connecting to vCenters: $_"
}

# Iterate through connected vCenters - retrieve list of all datastore clusters excluding clusters specifically omitted via $ExcludeClusters.
# For each template, initiate a copy to each datastore cluster and append the datastore cluster name, skipping the cluster the source template resides on.
# Each copy will run async, so collect task UIDs

$cloneTasks = New-Object -TypeName System.Collections.ArrayList
$exitStatus = 0

foreach ($vCenter in $connectedvCenters) {

    $targetClusters = Get-DatastoreCluster -Server $vCenter | Where-Object { $ExcludeClusters -notcontains $_.Name } -ErrorAction SilentlyContinue
    if (-not $ExcludeVSAN) {
        $targetClusters += Get-Datastore -Server $vCenter | Where-Object { ($ExcludeClusters -notcontains $_.Name) -and $_.Type -eq 'vsan' }
    }
    $sourceTemplates = Get-Template -Location (Get-Folder -Name $SourceFolder -Server $vCenter) -NoRecursion:(-not $Recursion) -ErrorAction SilentlyContinue

    # If there are both target clusters and source templates then there is work to do, otherwise skip this vCenter
    if ( $targetClusters -and $sourceTemplates ) {
        # For every cluster, copy the source templates to that cluster
        foreach ($targetCluster in $targetClusters) {
            $cloneTask = Copy-TemplateToClusterLocal -Template $sourceTemplates -DatastoreCluster $targetCluster -DestinationFolder $DestinationFolder -Server $vCenter -RunAsync
            if ($cloneTask) {
                $cloneTasks.Add($cloneTask) | Out-Null
            }
        }
    } else {
        Write-Verbose "Unable to find clusters/templates in vCenter $($vCenter.Name), skipping."
    }
}

# Loop to wait for clone tasks to finish if necessary, set exit code dependant on result
if ($cloneTasks) {
    do {
        Start-Sleep -Seconds 30 # Poll tasks every 30 seconds

        # Get all tasks from initial list that haven't already successfully completed
        $taskArr = Get-Task | Where-Object { $cloneTasks -contains $_ -and $_.State -ne "Success" }
        $taskArr | Format-Table | Out-String | Write-Verbose
        # If one or more tasks errors, log and set exit status appropriately.
        $errorTasks = $taskArr | Where-Object { $_.State -eq "Error" }
        foreach ($errorTask in $errorTasks) {
            Write-Verbose "$($errorTask.Description) $($errorTask.State)"
            $cloneTasks.Remove($errorTask) | Out-Null
            $exitStatus = 1
        }
    } while ($taskArr)
}

Disconnect-VIServer $vCenter -Force -Confirm:$false

$executionTimer.Stop()
Write-Verbose "Execution timer stopped. Elapsed time: $($executionTimer.Elapsed.ToString())"
if ($exitStatus) {
    Write-Warning "One or more clone tasks completed with errors - please check log for details."
}

exit $exitStatus