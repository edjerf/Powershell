<#
    .SYNOPSIS
    Migrate a list of VMs from their source storage location to a target storage location. This method supports live migration between different vCenters, DVSwitches, Datastores, etc.
    .DESCRIPTION
    Migrates a list of VMs to a new target location (Defined by vCenter, Cluster, DVSwitch, Portgroup, and Datastore/DSCluster), taking into account a preference for a remaining free space buffer. This script will also perform basic capacity load balancing across both datastore & host clusters when making placement decisions.
	
    Input CSV must formatted to contain the following Columns: VMName, SourceVC, TargetVC, TargetFolder(optional), TargetCluster, TargetDatastore, TargetSwitch, TargetPortgroup, SwitchType
    .PARAMETER CsvPath
    Specifies a path to location of the input CSV file. Required.
    .PARAMETER FreeBuffer
    Free space buffer, expressed as a percentage -  i.e. a value of 30 will result in migrations being performed until the target Datastore is at 70% used capacity. This value applies globally to all Datastores/Clusters included in the input file.
    .EXAMPLE
    C:\PS> .\Bulk-XVMotion.ps1 -CsvPath "\\thrnas01\servers\ServerScriptingandAutomation\Vmware\StorageVmotion\svmotiontable.csv" -FreeBuffer 20
    Will attempt to perform all migrations defined in the supplied CSV, omitting any movement that would place a datastore target below 20% free space.
    .NOTES
    Author: Eric Djerf
    Date:   5/17/2017
	
    xVmotion function template provided by William Lam (@lamw) https://github.com/lamw
    http://www.virtuallyghetto.com/2016/05/automating-cross-vcenter-vmotion-xvc-vmotion-between-the-same-different-sso-domain.html

    VERSION HISTORY:
    0.1.0 12/29/2016 - Initial
    0.2.0 5/17/2017 - Refactor

#>
param(
  [Parameter(
      Position=0,
      Mandatory=$true,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [String]$CsvPath, #Specifies a path to location of the input CSV file. Required.
  [Parameter(
      Position=1,
      Mandatory=$false,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [Int]$FreeBuffer=20, #Free space buffer, expressed as a percentage
  [Parameter(
      Position=2,
      Mandatory=$false,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [Int]$MaxConcurrent=2, #Maximum number of concurrent move tasks, default 2
    [Parameter(
      Position=3,
      Mandatory=$false,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [switch]$sendMail=$false, #Boolean to send migration status to a designated email address
    [Parameter(
      Position=4,
      Mandatory=$false,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [String]$mailFrom, #From e-mail address, only valid if $sendMail is enabled
      [Parameter(
      Position=5,
      Mandatory=$false,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [String[]]$mailTo, #To e-mail address, only valid if $sendMail is enabled
        [Parameter(
      Position=6,
      Mandatory=$false,
      ValueFromPipeline=$true,
    ValueFromPipelineByPropertyName=$true)]
  [String]$mailServer #SMTP server, only valid if $sendMail is enabled
)

if ( !(Get-Module -Name VMware.VimAutomation.Core -ErrorAction SilentlyContinue) ) {
  Import-Module VMware.PowerCLI
}

#region Functions
Function xMove-VM {
    param(
    [Parameter(
        Position=0,
        Mandatory=$true,
        ValueFromPipeline=$true,
        ValueFromPipelineByPropertyName=$true)
    ]
    [VMware.VimAutomation.Types.VIServer]$sourcevc,
    [VMware.VimAutomation.Types.VIServer]$destvc,
    [VMware.VimAutomation.Types.VirtualMachine]$vm,
    [String]$switchtype,
    [String]$vSwitch,
    [VMware.VimAutomation.Types.Cluster]$cluster,
    [VMware.VimAutomation.ViCore.Types.V1.DatastoreManagement.Datastore]$datastore,
    [VMware.VimAutomation.Types.VMHost]$vmhost,
    [String]$vmnetworks,
    [Management.Automation.PSCredential]$viCredential
    )

    # Retrieve Source VC SSL Thumbprint
    $vcurl = "https://" + $destVC
  add-type @"
        using System.Net;
        using System.Security.Cryptography.X509Certificates;
        
            public class IDontCarePolicy : ICertificatePolicy {
            public IDontCarePolicy() {}
            public bool CheckValidationResult(
                ServicePoint sPoint, X509Certificate cert,
                WebRequest wRequest, int certProb) {
                return true;
            }
        }
"@
    [System.Net.ServicePointManager]::CertificatePolicy = new-object IDontCarePolicy
    # Need to do simple GET connection for this method to work
    Invoke-RestMethod -Uri $vcurl -Method Get | Out-Null

    $endpoint_request = [System.Net.Webrequest]::Create("$vcurl")
    # Get Thumbprint + add colons for a valid Thumbprint
    $destVCThumbprint = ($endpoint_request.ServicePoint.Certificate.GetCertHashString()) -replace '(..(?!$))','$1:'

    # Source VM to migrate
    $vm_view = Get-View (Get-VM -Server $sourcevc -Name $vm) -Property Config.Hardware.Device

    # Find all Etherenet Devices for given VM which
    # we will need to change its network at the destination
    $vmNetworkAdapters = @()
    $devices = $vm_view.Config.Hardware.Device
    foreach ($device in $devices) {
        if($device -is [VMware.Vim.VirtualEthernetCard]) {
            $vmNetworkAdapters += $device
        }
    }

    # Relocate Spec for Migration
    $spec = New-Object VMware.Vim.VirtualMachineRelocateSpec

    $spec.datastore = $datastore.Id
    $spec.host = $vmhost.Id
    $spec.pool = $cluster.ExtensionData.ResourcePool

    # Service Locator for the destination vCenter Server
    # regardless if its within same SSO Domain or not
    $service = New-Object VMware.Vim.ServiceLocator
    $credential = New-Object VMware.Vim.ServiceLocatorNamePassword
    $credential.username = $viCredential.GetNetworkCredential().UserName
    $credential.password = $viCredential.GetNetworkCredential().Password
    $service.credential = $credential
    $service.instanceUuid = $destvc.InstanceUuid.ToUpper()
    $service.sslThumbprint = $destVCThumbprint
    $service.url = "https://$destVC"
    $spec.service = $service

    # Create VM spec depending if destination networking
    # is using Distributed Virtual Switch (VDS) or
    # is using Virtual Standard Switch (VSS)
    $count = 0
    if($switchtype -eq "vds") {
        foreach ($vmNetworkAdapter in $vmNetworkAdapters) {
            # New VM Network to assign vNIC
            $vmnetworkname = ($vmnetworks -split ",")[$count]

            # Extract Distributed Portgroup required info
            $dvpg = Get-VDPortgroup -Server $destvc -Name $vmnetworkname -VDSwitch $vSwitch
            $vds_uuid = (Get-View $dvpg.ExtensionData.Config.DistributedVirtualSwitch).Uuid
            $dvpg_key = $dvpg.ExtensionData.Config.key

            # Device Change spec for VSS portgroup
            $dev = New-Object VMware.Vim.VirtualDeviceConfigSpec
            $dev.Operation = "edit"
            $dev.Device = $vmNetworkAdapter
            $dev.device.Backing = New-Object VMware.Vim.VirtualEthernetCardDistributedVirtualPortBackingInfo
            $dev.device.backing.port = New-Object VMware.Vim.DistributedVirtualSwitchPortConnection
            $dev.device.backing.port.switchUuid = $vds_uuid
            $dev.device.backing.port.portgroupKey = $dvpg_key
            $spec.DeviceChange += $dev
            $count++
        }
    } else {
        foreach ($vmNetworkAdapter in $vmNetworkAdapters) {
            # New VM Network to assign vNIC
            $vmnetworkname = ($vmnetworks -split ",")[$count]

            # Device Change spec for VSS portgroup
            $dev = New-Object VMware.Vim.VirtualDeviceConfigSpec
            $dev.Operation = "edit"
            $dev.Device = $vmNetworkAdapter
            $dev.device.backing = New-Object VMware.Vim.VirtualEthernetCardNetworkBackingInfo
            $dev.device.backing.deviceName = $vmnetworkname
            $spec.DeviceChange += $dev
            $count++
        }
    }

    Write-Debug "`nMigrating $vmname from $sourceVC to $destVC ...`n"

    # Issue Cross VC-vMotion
    $task = $vm_view.RelocateVM_Task($spec,"defaultPriority")
    $retTask = Get-Task -Id ("Task-$($task.value)")
    return $retTask
    #$task1 | Wait-Task -Verbose
}


Function Get-MostFreeDSFromCluster
{
  Param
  (
    [Parameter(ValueFromPipeline=$true)]
    [VMware.VimAutomation.ViCore.Impl.V1.DatastoreManagement.StorageResourceImpl]$DatastoreCluster
  )
  Process
  {
    if (-not $DatastoreCluster) 
    {
      Write-Debug "No Datastore defined as input"
      return $null
    }
	
    $arrDatastoreList = Get-View ($DatastoreCluster | Get-View).ChildEntity|Select-Object Name
    $mostFreeDS = $arrDatastoreList | ForEach-Object{Get-Datastore $_.Name} | Sort-Object FreeSpaceGB -Descending | Select-Object -First 1
    Write-Debug "Selected `"$($mostFreeDS.Name)`" from datastore cluster `"$($DatastoreCluster.Name)`""
    return $mostFreeDS
  }
}

Function Get-LeastUtilizedHostFromCluster
{
  Param
  (
    [Parameter(ValueFromPipeline=$true)]
    [VMware.VimAutomation.Types.Cluster]$HostCluster
  )
  Process
  {
    # Return host with most free memory to run VMs
    if (-not $HostCluster) 
    {
      Write-Debug "No Host Cluster defined as input"
      return $null
    }
    $arrVMHosts = Get-VMHost -Location $HostCluster
    $mostFreeHost = $arrVMHosts | Where-Object {$_.ConnectionState -eq "Connected"} | Select-Object Name,@{Name="PctFree";Expression={$_.MemoryUsageGB/$_.MemoryTotalGB * 100 }} | Sort-Object PctFree | Select-Object -First 1
    Write-Debug "Selected `"$($mostFreeHost.Name)`" from host cluster `"$($HostCluster.Name)`""
    return $arrVMHosts | Where-Object {$_.Name -eq $mostFreeHost.Name}
  }
}

Function Wait-MoveTask
{
  # Loop and wait for one of the move tasks in $taskArr to complete, update appropriate $workList task with status
  Param
  (
    [Parameter(ValueFromPipeline=$false)]
    [System.Collections.ArrayList]$taskArr,
    [Parameter(ValueFromPipeline=$false)]
    [System.Collections.ArrayList]$workList
  )
  Start-Sleep -Seconds 5

  # Get updated status on tasks referenced by $taskArr
  $taskList = Get-Task -Server $sourceVCConn | Where-Object {$taskArr.Contains($_.Id)}
  # Check all tasks for finished status, if finished then remove and update $workList item with status
  foreach ($task in $taskList)
  {
    # Find $workList item associated with $task
    $workItem = $workList | Where-Object {$_.TaskId -eq $task.Id}

    if ($task.State -eq "Success")
    {
      $workItem.EndTime = Get-Date
      $workItem.DurationMinutes = [math]::Round(($workItem.EndTime - $workItem.StartTime).TotalMinutes)
      $workItem.Status = "Success"
      $taskArr.Remove($task.Id)
      # Job complete, check if a specified folder exists and attempt to move VM to folder
      if ( $workItem.TargetFolder -ne "")
      {
        $targetFolder = Get-Folder -Name $workItem.TargetFolder -Server $targetVCConn -ErrorAction SilentlyContinue
      }
      else {
        $targetFolder = $null
      }

      if ($targetFolder)
      {
        Get-VM -Name $workItem.VMName | Move-VM -Destination $targetFolder
      }
      if ($sendMail)
      {
        Send-StatusMail -workItem $workItem
      }
      # Power on if necessary
      sleep 1
      $movedVM = get-vm -Name $workItem.VMName
      if ($movedVM.PowerState -eq "PoweredOff")
      {
          Start-VM $movedVM -Confirm:$false
      }
    }
    elseif ($task.State -eq "Error")
    {
      $workItem.Notes = $task.ExtensionData.Info.Error.LocalizedMessage
      $workItem.EndTime = Get-Date
      $workItem.DurationMinutes = [math]::Round(($workItem.EndTime - $workItem.StartTime).TotalMinutes)
      $workItem.Status = "Error"
      $taskArr.Remove($task.Id)
      if ($sendMail)
      {
        Send-StatusMail -workItem $workItem
      }
    }
  }
}

Function Send-StatusMail
{
    # Send a mail message using the specified e-mail settings on the start or completion of an item. Start or completion is determined by the
    # status of the workItem
    Param
    (
    [Parameter(ValueFromPipeline=$false)]
    $workItem
    )
    
    # A workItem with some sort of Status indicates completion of the task, blank or null status indicates the item is being initiated
    if ($workItem.Status)
    {
        $mailSubject = "$($workItem.VMName) - $($workItem.Application) - migration complete - $($workItem.EndTime)"
        $mailBody = "Server: $($workItem.VMName)`rApplication: $($workItem.Application)`rStatus: $($workItem.Status)`rTarget Cluster: $($workItem.TargetCluster)`rTarget Network: $($workItem.TargetPortGroup)`rStart Time: $($workItem.StartTime)`rEnd Time: $($workItem.EndTime)`rDuration: $($workItem.DurationMinutes) Minute(s)`rSize: $([Math]::Round($workItem.UsedSpaceGB, 2)) GB`rTransfer Rate: $($workItem.UsedSpaceGB / $workItem.DurationMinutes) GB/Min`r`rNotes: $($workItem.Notes)`r`r`r`r"
        Send-MailMessage -From $mailFrom -To $mailTo -SmtpServer $mailServer -Subject $mailSubject -Body $mailBody
    }
    else
    {
        $mailSubject = "$($workItem.VMName) - $($workItem.Application) - migration start - $($workItem.StartTime)"
        $mailBody = "Server: $($workItem.VMName)`rApplication: $($workItem.Application)`rStatus: Starting`rTarget Cluster: $($workItem.TargetCluster)`rTarget Network: $($workItem.TargetPortGroup)`rStart Time: $($workItem.StartTime)`rSize: $([Math]::Round($workItem.UsedSpaceGB, 2)) GB`r`r`r`r"
        Send-MailMessage -From $mailFrom -To $mailTo -SmtpServer $mailServer -Subject $mailSubject -Body $mailBody
    }
    
}
#endregion

#Quick sanity checks in CSV input
$migrationList = Import-Csv $CsvPath

if ($migrationList -eq $null)
{
  Write-Output "Unable to read CSV file `"$CsvPath`""
  return
}

#Verify existence and presence of at most two vCenter servers
$sourceVC = $migrationList.SourceVC | Select-Object -Unique
$targetVC = $migrationList.TargetVC | Select-Object -Unique
if ( $sourceVC.Count -ne 1 -or $targetVC.Count -ne 1 )
{
  Write-Output "Unable to validate CSV (Check source and destination vCenters)"
  return
}

#Verify valid switch type configs, ensure values only contain "vds" or "standard"
foreach ($migration in $migrationList)
{
  if (($migration.SwitchType -ne "vds") -and ($migration.SwitchType -ne "standard"))
    {
      Write-Output "Unable to validate CSV (Check SwitchType)"
      return
    }
}

# Disconnect existing vCenter connections
if ( $DefaultViServer )
{
  Disconnect-VIServer * -Confirm:$false -Force -ErrorAction SilentlyContinue
}

$viCred = Get-Credential -Message "Credential for vCenter(s):"

# Connect to Source/Destination vCenter Server, verify if two connections are needed first.
$sourceVCConn = Connect-VIServer -Server $sourceVC -Credential $viCred
if ( $sourceVC -eq $targetVC )
{
  $targetVCConn = $sourceVCConn
}
else
{
  $targetVCConn = Connect-VIServer -Server $targetVC -Credential $viCred
}

# Verify connection
if ( -not ($sourceVCConn -and $targetVCConn) )
{
  Write-Output "Could not connect to one or both vCenters, exiting."
  return
}

# Build Main worklist from CSV to manage and pull jobs from. This ArrayList will also be updated as the script runs to use for reporting.
$workList = [System.Collections.ArrayList]@()
foreach ($migration in $migrationList)
{

  $workItem = "" | Select-Object VMName,Application,TaskID,Status,SourceVC,TargetVC,TargetFolder,TargetCluster,TargetDatastore,TargetSwitch,TargetPortGroup,SwitchType,Notes,StartTime,EndTime,DurationMinutes,UsedSpaceGB

  $migration.PSObject.Properties | ForEach-Object {$workItem.($_.Name) = $_.Value }
  $workList.Add($workItem)

}

$taskArr = [System.Collections.ArrayList]@()	# Maintain list of currently running tasks in hash table, with key being the String ID and value being the actual task object


###
# Main work loop
###
$running = $true
$i = 0
while ($running)
{
  # Check if task queue is full
  if ($taskArr.Count -lt $MaxConcurrent)
  {
    # Check if work remains
    if ($i -lt $workList.Count)
    {
      $workItem = $workList[$i]
      # Add Work
      $workItem.StartTime = Get-Date
      $sourceVM = Get-VM -Name $workItem.VMName -Server $sourceVCConn -ErrorAction SilentlyContinue
      $targetDS = Get-Datastore -Name $workItem.TargetDatastore -Server $destVCConn -ErrorAction SilentlyContinue
      if (-not $targetDS)
      {
        # May be a datastore cluster, check
        $targetDSCluster = Get-DatastoreCluster $workItem.TargetDatastore
        if ($targetDSCluster)
          { $targetDS = $targetDSCluster | Get-MostFreeDSFromCluster }
      }
      $targetCluster = Get-Cluster -Name $workItem.TargetCluster -Server $destVCConn -ErrorAction SilentlyContinue
      $targetHost = Get-LeastUtilizedHostFromCluster $targetCluster

      if ($workItem.SwitchType -eq "vds")
        {
          # Handle multiple comma-delimited port groups. These will be matched up to the
          # VMs network adapters in the order in which they appear on the VM
          $vmNetworkName = ($workItem.TargetPortGroup -split ",")
          $targetSwitch = Get-VDSwitch -Name $workItem.TargetSwitch -Location ($targetCluster | Get-Datacenter)
          $targetPortGroup = Get-VDPortgroup -Name $vmNetworkName -VDSwitch $targetSwitch
        }
      elseif ($workItem.SwitchType -eq "standard")
        {
          $vmNetworkName = ($workItem.TargetPortGroup -split ",")
          $targetSwitch = Get-VirtualSwitch -Name $workItem.TargetSwitch -VMHost $targetHost -ErrorAction SilentlyContinue
          $targetPortGroup = Get-VirtualPortGroup -VMHost $targetHost -Name $vmNetworkName
         }

      # Check for VM existence.
      if ($sourceVM -eq $null)
      {
        #Unable to locate VM, skip
        Write-Output "Unable to locate VM $($workItem.VMName); skipping"
        $workItem.Notes = "VM Not Found"
        $workItem.EndTime = Get-Date
        $workItem.Status = "Error"
      }
      # Check for Datastore/Cluster existence
      elseif ($targetDS -eq $null)
      {
        Write-Output "Unable to locate Datastore or Datastore cluster `"$($workItem.TargetDatastore)`"; skipping"
        $workItem.Notes = "DS/Cluster not found"
        $workItem.EndTime = Get-Date
        $workItem.Status = "Error"
      }
      # Check Datastore Space
      elseif (($targetDS.FreeSpaceGB - ($targetDS.CapacityGB * $FreeBuffer / 100)) -lt $sourceVM.UsedSpaceGB)
      {
        $dsBufferedFreeSpace = [Math]::Round(($targetDS.FreeSpaceGB - ($targetDS.CapacityGB * $FreeBuffer / 100)), 2)
        Write-Output "Not enough space on $($targetDS.Name) `($($dsBufferedFreeSpace)`) for $($sourceVM.Name) `($([Math]::Round($sourceVM.UsedSpaceGB,2))`), skipping."
        $workItem.Notes = "Not enough space: $($targetDS.Name) - $($dsBufferedFreeSpace), $($sourceVM.Name) - $($sourceVM.UsedSpaceGB)"
        $workItem.EndTime = Get-Date
        $workItem.Status = "Error"
      }
      # Check Host Cluster existence
      elseif ($targetCluster -eq $null)
      {
        Write-Output "Unable to locate Host Cluster `"$($workItem.TargetCluster)`"; skipping"
        $workItem.Notes = "Host cluster not found"
        $workItem.EndTime = Get-Date
        $workItem.Status = "Error"
      }
      # Check Portgroup existence, implicitly checks vswitch as well
      elseif ($targetPortGroup -eq $null)
      {
        Write-Output "Unable to locate portgroup `"$($workItem.TargetPortgroup)`"; skipping"
        $workItem.Notes = "PortGroup not found"
        $workItem.EndTime = Get-Date
        $workItem.Status = "Error"
      }
      # No failures, attempt move and add task
      else
      {
        Write-Output "$($sourceVM.Name) `($([Math]::Round($sourceVM.UsedSpaceGB, 2))`) moving to $($targetDS.Name)"
        Write-Output "$($i + 1)/$($migrationList.Count)"
        $vmNetworkAdapters = Get-NetworkAdapter -VM $sourceVM
        $workItem.UsedSpaceGB = $sourceVM.UsedSpaceGB
        # Future case - Use native PowerCLI6.5 Move-VM commandlet - for now this commandlet returns a task type incompatible with Get-Task when run asynchronously
        #$moveTask = Move-VM -VM $sourceVM -Destination $targetHost -Datastore $targetDS -NetworkAdapter $vmNetworkAdapters -PortGroup $targetPortGroup -Server $sourceVCConn -RunAsync
        $moveTask = xMove-VM -sourcevc $sourceVCConn -destvc $targetVCConn -vm $sourceVM -switchtype $workItem.SwitchType -vSwitch $targetSwitch.Name -cluster $targetCluster -datastore $targetDS -vmhost $targetHost -vmnetworks $workItem.TargetPortgroup -viCredential $viCred
        $taskArr.Add($moveTask.Id)
        $workItem.TaskID = $moveTask.Id
        
        if ($sendMail)
        {
            Send-StatusMail -workItem $workItem
        }
      }
      # Regardless of success or failure, increment worklist counter
      $i++
    }
    else
    # No work remains, bail and wait for tasks to complete
    {
      Write-Output "No work remains in worklist, setting running state to false."
      $running = $false
    }
  }
  else
  # Task Queue full, loop and check for completed tasks
  {
    # Loop for task status change while the queue is full, once queue is no longer full
    # bail out and return to top of main loop to get another task
    Write-Output "Waiting for currently executing tasks to finish..."
    do {
      Wait-MoveTask $taskArr $workList
    } while ( $taskArr.Count -ge $MaxConcurrent)
    Write-Output "Complete"
  }
}

# $running has been set to false and main loop exited - perform Final task cleanup and write out report
do {
  Wait-MoveTask $taskArr $workList
} while ( $taskArr.Count -gt 0)

$reportName = [IO.Path]::GetFileNameWithoutExtension($CsvPath) + "_report.csv"
$workList | Export-Csv -NoTypeInformation -Path (Join-Path (Split-Path $CsvPath -Parent) $reportName )

# Disconnect from Source/Destination VC
Disconnect-VIServer -Server $sourceVCConn -Confirm:$false
if ( $sourceVC -ne $targetVC )
{
  Disconnect-VIServer -Server $targetVCConn -Confirm:$false
}