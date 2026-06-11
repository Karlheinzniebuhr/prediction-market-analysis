function Invoke-PmaStep {
    param(
        [string]$Label,
        [string]$LogPath,
        [scriptblock]$Command
    )
    Write-Host $Label
    $prevEap = $ErrorActionPreference
    $prevNative = $null
    if ($PSVersionTable.PSVersion.Major -ge 7) {
        $prevNative = $PSNativeCommandUseErrorActionPreference
        $PSNativeCommandUseErrorActionPreference = $false
    }
    $ErrorActionPreference = "SilentlyContinue"
    try {
        & $Command 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) {
                [string]$_.ToString()
            } else {
                [string]$_
            }
        } | Tee-Object -FilePath $LogPath
    } finally {
        $ErrorActionPreference = $prevEap
        if ($null -ne $prevNative) {
            $PSNativeCommandUseErrorActionPreference = $prevNative
        }
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Command failed with exit code $LASTEXITCODE (log: $LogPath)"
    }
}
