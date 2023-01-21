<# .SYNOPSIS - .DESCRIPTION - .PARAMETER - .EXAMPLE - .NOTES Name : Verb-Noun Author : Author #>
function Verb-Noun
{
    [CmdletBinding()]
    PARAM
    (
        [Parameter(
            Position=0,
            Mandatory=$true,
            ValueFromPipeline=$true,
          ValueFromPipelineByPropertyName=$true)]
        [String]$stringVariable, # This is a required string variable
        [Parameter(
            Position=1,
            Mandatory=$false,
            ValueFromPipeline=$true,
          ValueFromPipelineByPropertyName=$true)]
        [Int]$FreeBuffer=20 # This is an optional integer variable
    )
    BEGIN
    {
        Write-Verbose "$((Get-Date).ToShortDateString()) : Started running $($MyInvocation.MyCommand)"
        #Initialise Variables
    }
    PROCESS
    {
        #Get data from object depending on type
        If ($SourceObject -is [String])
        {
        }
        #Code to check for passed parameter name
        if (!($PSBoundParameters.ContainsKey('PSTExportFolder')))
        {
        }
    }
    END
    {
    }
}