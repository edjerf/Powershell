<#
.SYNOPSIS
    Repair-VRAReservations
    Script which reconciles vRA managed machine reservations with where the machines actually are in vCenter
.DESCRIPTION
    This script leverages PowerVRA and a CloudClient instance to repair VRA Reservation relationships that have drifted over time. Sometimes machines built in
    vRA are moved to another cluster and the underlying reservation is not updated in vRA - this script identifies those machines and sets the reservation
    correctly

    Requires:
        PowerCLI 11
        vRA 7.6
        CloudClient set up with persistent saved login
        PowerVRA <= 3.7 Installed - Install-Module -Name PowervRA -RequiredVersion 3.7.0
.EXAMPLE
    PS C:\> Repair-VRAReservations-vRAServer "vcenter.local" -vRATenant "mytenant" -vRACreds (Get-Credential) -vCenter "vcenter1","vcenter2" -ReportOnly
.PARAMETER vRAServer
    IP or FQDN of vRA server to connect to
.PARAMETER vRATenant
    Optional vRA tenant to attach PowerVRA session to
.PARAMETER vRACred
    Credential to use when connection to vRA Session
.PARAMETER vCenter
    vCenter Server(s) to connect to
.PARAMETER vCenterCred
    Optional Credentials to use when connecting to vCenter
.PARAMETER GroupSize
    Optional "batch" size of machines to send per CloudClient call
.PARAMETER ReportOnly
    Switch 
#>
[CmdletBinding()]
param(
    $vRAServer,
    $vRATenant,
    $vRACreds,
    $vCenter,
    $vCenterCred,
        
    [ValidateScript(
        {
            (Test-Path -Path $_ -Type Container) -and (Test-Path -Path (Join-Path -Path $_ -ChildPath "\bin\cloudclient.bat"))
        }#,ErrorMessage = "{0} does not appear to be a valid CloudClient install directory." PS 6 and up
    )]
    $CloudClientPath = "D:\vRealize CloudClient\VMware_vRealize_CloudClient-4.7.0-13182508",
    [int] $GroupSize = 50,
    [switch] $ReportOnly = $false
)
#######
# Set-vRAReservationsCC Helper function - this is the function which actually batches the calls out to CloudClient
function Set-vRAReservationsCC
{
    <#
    .SYNOPSIS
        Function to set vRA Reservations/Storage path on a list of VMs using a pre-configured CloudClient instance.
    .DESCRIPTION
        Use pre-configured cloudclient installation with saved credentials, make a call to the CloudClient to register a list of VMs with a specified
        reservation and storage location
    .PARAMETER NewStorage
        The new storage location for all specified machines
    .PARAMETER NewReservation
        The new reservation for all machines in the list
    .PARAMETER TargetVMs
        An array of VM names to target for reservation changes
    .PARAMETER GroupSize
        Size of batches to send with each CloudClient invocation, default 100
    #>
    [CmdletBinding()]
    param(
        [System.String] $NewReservation,
        [System.String] $NewStorage,
                
        [ValidateScript( {
                Test-Path -Path $_ -Type Container
            })]
        [System.String] $CloudClientPath = "D:\vRealize CloudClient\VMware_vRealize_CloudClient-4.7.0-13182508",
        [System.String[]] $TargetVMs,
        [int] $GroupSize = 100
    )
    
    $CloudClientBat = Join-Path -Path $CloudClientPath -ChildPath "\bin\cloudclient.bat"
    
    # Split number of remediations into into batches of 100 or fewer.
    $counter = [pscustomobject] @{ Value = 0 }
    $targetGroups = $TargetVMs | Group-Object -Property { [math]::Floor($counter.Value++ / $GroupSize) }
    Write-Verbose "Got $($TargetVMs.count) total items, splitting into batches of size $($GroupSize)"
    # Loop through and send all batches
    foreach ($targetGroup in $targetGroups)
    {
        Write-Verbose "Sending batch $($targetGroup.Name) - Size $($targetGroup.Count)"
        # CloudClient accepts names/ids as a comma delimited list
        $MachineIDs = $targetGroup.Group -join ","
        Write-Verbose "Batch contents: $($MachineIDs)"
            
        # Call the cloudclient executable, passing in the MachineIDs and New Reservation
        Write-Verbose "CloudClient invocation: $CloudClientBat vra machines change reservation --ids $MachineIDs --reservationName $NewReservation --storagePath $NewStorage"
        & $CloudClientBat vra machines change reservation --ids $MachineIDs --reservationName $NewReservation --storagePath $NewStorage
    }
}

try
{
    if ($vRATenant)
    {
        $vraSession = Connect-vRAServer -server $vRAServer -tenant $vRATenant -Credential $vRACreds -ErrorAction Stop
    }
    else
    {
        $vraSession = Connect-vRAServer -server $vRAServer -Credential $vRACreds -ErrorAction Stop
    }

    if ($vCenterCred)
    {
        $vcSession = Connect-VIServer -Server $vCenter -Credential $vCenterCred -ErrorAction Stop
    }
    else
    {
        $vcSession = Connect-VIServer -Server $vCenter -ErrorAction Stop
    }
}
catch
{
    Write-Error " $_"
    Exit 1
}

# Get a list of all machines in vRA in "good" standing - i.e. not missing, orphaned, or disposing
$machineResources = Get-vRAResource -Type Machine -WithExtendedData | Where-Object { "Missing", "Disposing" -notcontains $_.Status }
$vraMachines = @{}
#Build Hashtable mapping VM name to resource data
foreach ($machineResource in $machineResources)
{
    $vraMachines[$machineResource.Name] = $machineResource
}
# Obtain correlary list of all of those VMs in vCenter
$vCenterVMS = Get-VM $machineResources.Name
# Get a list of all vRA Reservations and compute resources - create hashmap associating vCenter compute clusters to  vRA reservations
$reservations = Get-VRAReservation
$vraReservations = @{}
foreach ($reservation in $reservations)
{
    $computeResourceString = $reservation.ExtensionData.entries | Where-Object { $_.key -eq "computeResource" } | Select-Object -ExpandProperty value
    $computeCluster = ($computeResourceString.label -split ' ')[0]
    $vraReservations[$computeCluster] = $reservation.Name
}

# Check list of vCenter VMs against list of vRA VMs, identify vRA VMs that have the "wrong" reservation
$VMstoRemediate = @()
foreach ($VM in $vCenterVMS)
{
    $vmCluster = $VM.VMHost.Parent | Select-Object -ExpandProperty Name # Cluster VM is actually running under in vCenter
    $vmReservation = $vraMachines[$VM.Name].data.MachineReservationName # vRA reservation machine resides under according to VRA

    # If the reservation in VRA does not match the cluster the VM actually resides in, add it to the list of VMs to fix
    if ($vraReservations[$vmCluster] -ne $vmReservation)
    {
        $lineItem = "" | Select-Object VMName, CurrentCluster, CurrentReservation, TargetReservation, TargetStorage
        $lineItem.VMName = $VM.Name
        $lineItem.CurrentCluster = $vmCluster
        $lineItem.CurrentReservation = $vmReservation
        $lineItem.TargetStorage = $VM | Get-DatastoreCluster | Select-Object -ExpandProperty Name
        if ($null -eq $lineItem.TargetStorage)
        {
            # If we don't get a datastore cluster back its likely a single datastore target or vSAN
            $lineItem.TargetStorage = $VM | Get-Datastore | Select-Object -ExpandProperty Name
        }
        $lineItem.TargetReservation = $vraReservations[$vmCluster]
        $VMstoRemediate += $lineItem
    }
    else
    {
        Write-Verbose "VM $($VM.Name) appears to be in the correct VRA reservation, skipping."
    }
    
}



if ($ReportOnly)
{
    $VMstoRemediate | Format-Table | Write-Output
}
else
{
    $VMstoRemediate | Format-Table | Out-String | Write-Verbose
    # If not running in report only mode then actually perform the remediation

    # Use group-object cmdlet to group the list of remediations by TargetReservation, and TargetStorage i.e. all remediation objects which
    # have the *same* target res, and target storage will be placed into groups together. This is important because the call to the
    # CloudClient executable expects a single compute and storage reservation and array of target VMs as arguments - by grouping these remediation targets
    # together we can create efficient groups to "batch" to the CC executable and reduce the number of API calls and execution time.
    $groupedRemediations = $VMstoRemediate | group-object  TargetReservation,TargetStorage | Sort-Object Count

    $prevLoc = Get-Location
    Set-Location $CloudClientPath
    foreach ($remediation in $groupedRemediations)
    {
        $vmNames = $remediation.Group.VMName
        $targetReservation = ($remediation.Name -Split ", ")[0]
        $targetStorage = ($remediation.Name -Split ", ")[1]
        Write-Verbose "Attempting reservation change for reservation target $($remediation.Name) - total items: $($remediation.Count)"
        Write-Verbose " $($vmNames)"

        Set-vRAReservationsCC -TargetVMs $vmNames -NewReservation $targetReservation -NewStorage $targetStorage -GroupSize $GroupSize
    }
    Set-Location $prevLoc
}

Disconnect-vRAServer -Confirm:$false
Disconnect-VIServer $vcSession -Confirm:$false