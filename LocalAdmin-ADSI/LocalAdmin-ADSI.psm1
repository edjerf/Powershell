function Add-MemberToLocalAdminADSI {
    param (
        [Parameter(Mandatory=$true)]
        [string]$MemberName,
        [string]$ComputerName = $env:COMPUTERNAME,
        [string]$Domain = (Get-WmiObject -Namespace root\cimv2 -Class Win32_ComputerSystem | Select-Object -ExpandProperty Domain)
    )

    try {
        $localAdminGroup = [ADSI]"WinNT://$ComputerName/Administrators"
        $adGroup =[ADSI]"WinNT://$Domain/$MemberName"
        $localAdminGroup.Add($adGroup.Path) 
    }
    catch {
        Write-Warning $Error[0].ToString()
        return $Error[0].ToString()
    }
    return $null
}

function Remove-MemberFromLocalAdminADSI {
    # Attempt to remove an AD group member from a machine's local Administrators group. MemberName should be the
    # unique SAMaccount name for the object.
    param (
        [Parameter(Mandatory=$true)]
        [string]$MemberName,
        [string]$ComputerName = $env:COMPUTERNAME,
        [string]$Domain = (Get-WmiObject -Namespace root\cimv2 -Class Win32_ComputerSystem | Select-Object -ExpandProperty Domain)
    )

    try {
        $localAdminGroup = [ADSI]"WinNT://$ComputerName/Administrators"
        $adGroup =[ADSI]"WinNT://$Domain/$MemberName"
        Write-Output "Removing group $($adGroup.Path) from $($localAdminGroup.Path)"
        $localAdminGroup.Remove($adGroup.Path)
    }
    catch {
        Write-Warning $Error[0].ToString()
        return $Error[0].ToString()
    }
    return $null
}


function Get-LocalAdminsADSI {
    # Return list of local administrators from a computer using the 2008/2012 compatible ADSI construct
    # Return value is an array of zero or more strings in ADSI format on success, null on failure
    param (
        [string]$ComputerName=$env:COMPUTERNAME
    )
    $computerADSI = [ADSI]("WinNT://$ComputerName,computer")
    try {
        $adminGroup = $computerADSI.psbase.children.find("Administrators")
        $adminMembers = @($adminGroup.psbase.invoke("Members") | ForEach-Object {$_.GetType().InvokeMember("Adspath", 'GetProperty', $null, $_, $null)})
        $adminMembers
    }
    catch {
        Write-Warning "Encountered error on $ComputerName - $($Error[0].ToString())"
        return $null
    }
}

Export-ModuleMember -Function Add-MemberToLocalAdminADSI
Export-ModuleMember -Function Remove-MemberFromLocalAdminADSI
Export-ModuleMember -Function Get-LocalAdminsADSI