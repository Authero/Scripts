<#
.SYNOPSIS
    Pings a specified IP address or hostname.
.DESCRIPTION
    This script takes an IP address or hostname as input and pings it.
.PARAMETER Target
    The IP address or hostname to ping.
.EXAMPLE
    .\pingtest.ps1 -Target "google.com"
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$Target
)

# Ping the target
Test-Connection -ComputerName $Target -Count 4