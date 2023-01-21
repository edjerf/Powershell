
<#
    .SYNOPSIS
    Identify and repair duplicate tag objects across linked-mode vCenters
    .DESCRIPTION
    This function aims to resolve a corner-case that can sometimes crop up with linked-mode vCenters and automated
    tag management resulting in unintended duplication of tags - in this case,'duplicate' tag objects are defined as
    tags with the same Name and Category, but with differing Uids.

    A tag duplication condition can occur when two clients attempt to create the same tag at the same time on
    different linked-mode vCenters - if the tag creation events both take place before the vCenters have had a chance
    to replicate their tag data to each other, two tags will be created with the same name but different unique tag
    IDs, and those tags will then subsequently be replicated to their partner vCenters.

    This function takes as argument (or pipeline) one or more strings representing TagCategories, identifies
    duplicate tags in that category, consolodates any tag-assignments for the duplicates to a single master tag, 
    and removes the remaining duplicate tag entries from the DB
    .PARAMETER TagCategory
    String(s) with the name of the target Tag category to repair.
    .EXAMPLE
    C:\PS> Repair-DuplicateTags -TagCategory "Application"
    Identify and repair any duplicate tags for the "Application" tag category.
    .EXAMPLE
    Get-TagCategory | Select-Object -ExpandProperty Name -Unique | Repair-DuplicateTags
    Iterate through every tag category on the connected vCenters and attempt to repair them all.
    .EXAMPLE
    Repair-DuplicateTags -TagCategory "TestCategory" -WhatIf -Verbose
    This function supports Cmdlet binding - enable verbose logging and perform a dry run.
    .NOTES
    Author - Eric Djerf
#>

function Repair-DuplicateTags {
    [CmdletBinding(SupportsShouldProcess)]
    param (
        [Parameter(
            Position = 0,
            Mandatory = $true,
            ValueFromPipeline = $true)]
        [string[]]$TagCategory
    )
    
    begin {
        if ($global:DefaultVIServers.Count -lt 2) {
            Write-Error "Function requires connectivity to at least two vCenter servers."
            break
        }
    }


    process {
            # Encapsulate in for loop to handle both pipeline and paramater input
        foreach ($tagC in $TagCategory) {

            # Find all tags with duplicates - Get all tags and group them by Name. We should recieve one per connected
            # vCenter server, per name. If there are more than that, it's a duplicate.
            $tagGroups = Get-Tag -Category $tagc | Group-Object -Property Name
            $duplicateGroups = $tagGroups | Where-Object { $_.Count -gt $global:DefaultVIServers.Count }
            
            if (-not $duplicateGroups) {
                Write-Verbose "No duplicate tags found for category $tagc"
                return
            }

            # Load all tag assignments for category into memory for quick lookup
            $tagAssignmentList = Get-TagAssignment -Category $tagc

            foreach ($duplicateGroup in $duplicateGroups) {
                $tagObjects = $duplicateGroup.Group
                # Do unique select on Tag Id property to identify the unique duplicate tag IDs, since in linked-mode
                # or multiple vCenter scenarios we'll recieve a tag object for each connected vCenter
                $duplicateTagIDs = $tagObjects | Select-Object -ExpandProperty Id -Unique

                # Iterate through each duplicate tag ID and find the ID with the most tag assignments across all
                # vCenters, this ID will now become the 'master' and all other tag assignments with the same name
                # but differing IDs will be removed and re-applied using the tag with the 'master' ID. This results
                # in the fewest reassignment actions.
                $mostAssignments = 0
                foreach ($tagID in $duplicateTagIDs) {
                    $tagAssignments = $tagAssignmentList | Where-Object { $_.Tag.Id -eq $tagID }
                    Write-Verbose "$tagID assignment count: $($tagAssignments.Count)"
                    if ($tagAssignments.Count -gt $mostAssignments) {  
                        Write-Verbose "$($tagAssignments.count) higher than previous largest count $mostAssignments, setting $tagID as highest assignment ID"
                        $mostAssignmentsID = $tagID
                        $mostAssignments = $tagAssignments.Count
                    }
                }

                # With 'master' tag ID identified, iterate through each connected vCenter and identify tagAssignments
                # within the tag 'group' (the group being the collection of tags with the same Name but differing IDs)
                # and iteratively re-assign them.
                foreach ($vCenter in $global:DefaultVIServers) {
                    $masterTag = Get-Tag -Id $mostAssignmentsID -Server $vCenter
                    # Identify tagAssignment objects which belong to this vCenter but do not match our 'master' ID
                    $removeAssignments = $tagAssignmentList | Where-Object { $_.Tag.Uid.Contains($vCenter.Uid) -and $_.Tag.Id -ne $mostAssignmentsID -and $_.Tag.Name -eq $masterTag.Name }
                    if ($removeAssignments) {
                        foreach ($removal in $removeAssignments) {
                            # Record existing entity, remove the old assignment, and add the new assignment
                            $originalEntity = $removal.Entity
                            if ( $PSCmdlet.ShouldProcess($originalEntity,"Reassign tag $($removal.Tag.Id)") ) {
                                Remove-TagAssignment -TagAssignment $removal -Confirm:$false
                                $newAssignment = New-TagAssignment -Tag $masterTag -Entity $originalEntity -Server $vCenter
                                Write-Verbose "Moved entity $originalEntity from $($removal.Tag.Id) to $($newAssignment.Tag.Id)"
                            }
                        }
                    }
                }
                # Once all tagassignments have been fixed across all connected vCenters, any duplicate tag that is not 
                # the 'master' may be safely deleted. Identify the tags to be deleted, verify there are no assignments,
                # and if there aren't then delete them.
                # Target only one vCenter for the actual tag Objects, the deletions will be replicated to the other vCenters
                $tagsToDelete = Get-Tag -Category $tagc -Name $masterTag.Name -Server $global:DefaultVIServer | Where-Object { $_.Id -ne $mostAssignmentsID }
            
                $checkAssignments = Get-TagAssignment -Category $tagc
                foreach ($tag in $tagsToDelete) {
                    if ($PSCmdlet.ShouldProcess($tag.Id, 'Verify and delete') ) {
                        if ($checkAssignments.Tag.Id -notcontains $tag.Id) {
                            # Targeted tag has no assignments in any vCenter, delete.
                            Write-Verbose "Tag $($tag.Uid) has no assignments, removing"
                            Remove-Tag -Tag $tag -Confirm:$false
                        } else {
                            Write-Warning "Tag $($tag.Uid) still has assignments, skipping"
                        }
                    }
                }
            }
        }
    }
    
    end {
        
    }
}

#Get-TagCategory | Select-Object -ExpandProperty Name -Unique | Repair-DuplicateTags -Verbose
#Repair-DuplicateTags -TagCategory "Application"
#Repair-DuplicateTags -TagCategory "TestCategory" -WhatIf -Verbose