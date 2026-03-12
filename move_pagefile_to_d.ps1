$ErrorActionPreference = "Stop"

$initialMb = 32768
$maximumMb = 40960
$target = "D:\pagefile.sys"

Write-Host "Disabling automatic pagefile management..."
$cs = Get-WmiObject -Class Win32_ComputerSystem -EnableAllPrivileges
$cs.AutomaticManagedPagefile = $false
$null = $cs.Put()

Write-Host "Removing existing pagefile settings..."
Get-WmiObject -Class Win32_PageFileSetting -ErrorAction SilentlyContinue | ForEach-Object {
    $_.Delete() | Out-Null
}

Write-Host "Creating pagefile on $target ($initialMb MB -> $maximumMb MB)..."
Set-WmiInstance -Class Win32_PageFileSetting -Arguments @{
    Name = $target
    InitialSize = $initialMb
    MaximumSize = $maximumMb
} | Out-Null

Write-Host ""
Write-Host "Done. New pagefile setting:"
Get-WmiObject -Class Win32_PageFileSetting |
    Select-Object Name, InitialSize, MaximumSize |
    Format-Table -AutoSize

Write-Host ""
Write-Host "Reboot Windows to apply the new pagefile location."
