<#
.SYNOPSIS
    Installs the latest DewDB release for Windows (x86_64) into the current user's profile.

.DESCRIPTION
    Downloads the latest DewDB release ZIP and its SHA256SUMS.txt from GitHub, verifies
    the archive checksum, extracts it, and installs dewdb.exe to
    %LOCALAPPDATA%\DewDB\bin, adding that directory to the user PATH. The LICENSE and
    NOTICE texts shipped in the archive are installed to %LOCALAPPDATA%\DewDB.

    This is a user-level install: it never requires Administrator rights, never writes to
    Program Files, never installs a Windows service, and never touches the machine-level
    PATH. Other dewdb.exe copies elsewhere on the system are left untouched.

    This script is intended to eventually be served from https://windows.dewdb.com so that
    a documented one-liner installs DewDB. That endpoint is NOT live yet, so for now the
    script is run directly from the repository. It is written to work both when run as a
    file and when piped from the web into PowerShell, in which case no parameters are
    passed and the defaults apply.

.PARAMETER LoadFunctionsOnly
    Testing seam. When the script is dot-sourced with this switch it defines its functions
    but performs no install, so the verification logic can be exercised directly by tests.
    It can only prevent an install, never weaken one.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install-windows.ps1
#>
[CmdletBinding()]
param(
    [switch]$LoadFunctionsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

$script:DewDbRepo      = 'dewdb/dewdb'
$script:DewDbApiUrl    = "https://api.github.com/repos/$script:DewDbRepo/releases/latest"
$script:DewDbUserAgent = 'dewdb-install-windows'
$script:DewDbSumsName  = 'SHA256SUMS.txt'
$script:DewDbExeName   = 'dewdb.exe'
$script:DewDbLegalFiles = @('LICENSE', 'NOTICE')

function Get-DewDbInstallRoot {
    if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        throw 'LOCALAPPDATA is not set; cannot determine the per-user install location.'
    }
    return (Join-Path $env:LOCALAPPDATA 'DewDB')
}

function Get-DewDbInstallDir {
    return (Join-Path (Get-DewDbInstallRoot) 'bin')
}

# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

function Initialize-DewDbTls {
    # Windows PowerShell 5.1 defaults can exclude TLS 1.2, which GitHub requires.
    try {
        [Net.ServicePointManager]::SecurityProtocol =
            [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    } catch {
        Write-Verbose "Could not enable TLS 1.2 explicitly: $($_.Exception.Message)"
    }
    if ([enum]::GetNames([Net.SecurityProtocolType]) -contains 'Tls13') {
        try {
            [Net.ServicePointManager]::SecurityProtocol =
                [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls13
        } catch {
            Write-Verbose 'TLS 1.3 not usable on this platform; continuing with TLS 1.2.'
        }
    }
}

function Assert-DewDbHttps {
    param([Parameter(Mandatory)][string]$Uri)

    $parsed = $null
    if (-not [Uri]::TryCreate($Uri, [UriKind]::Absolute, [ref]$parsed)) {
        throw "Refusing to fetch a malformed URL: $Uri"
    }
    if ($parsed.Scheme -ne 'https') {
        throw "Refusing to fetch over '$($parsed.Scheme)'. DewDB is only downloaded over HTTPS. URL: $Uri"
    }
    return $parsed.AbsoluteUri
}

function Invoke-DewDbDownload {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][string]$OutFile
    )

    $safeUri = Assert-DewDbHttps -Uri $Uri
    $previousProgress = $ProgressPreference
    $ProgressPreference = 'SilentlyContinue'   # Invoke-WebRequest is very slow on 5.1 with progress on.
    try {
        Invoke-WebRequest -Uri $safeUri -OutFile $OutFile -UseBasicParsing `
            -Headers @{ 'User-Agent' = $script:DewDbUserAgent }
    } finally {
        $ProgressPreference = $previousProgress
    }

    if (-not (Test-Path -LiteralPath $OutFile -PathType Leaf)) {
        throw "Download did not produce a file: $OutFile"
    }
}

# ---------------------------------------------------------------------------
# Platform
# ---------------------------------------------------------------------------

function Get-DewDbOSArchitecture {
    try {
        return [string][System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    } catch {
        Write-Verbose 'RuntimeInformation unavailable; falling back to environment variables.'
    }

    $arch = $env:PROCESSOR_ARCHITEW6432
    if ([string]::IsNullOrWhiteSpace($arch)) { $arch = $env:PROCESSOR_ARCHITECTURE }
    return [string]$arch
}

function Assert-DewDbArchitecture {
    $arch = Get-DewDbOSArchitecture
    if ($arch -imatch '^(x64|amd64)$') { return $arch }

    throw @"
Unsupported architecture: '$arch'.
This installer currently provides Windows x86_64 (AMD64) builds only.
No other architecture (including ARM64 and 32-bit x86) is published yet.
"@
}

# ---------------------------------------------------------------------------
# Release metadata
# ---------------------------------------------------------------------------

function Get-DewDbLatestRelease {
    $uri = Assert-DewDbHttps -Uri $script:DewDbApiUrl
    try {
        return Invoke-RestMethod -Uri $uri -UseBasicParsing -Headers @{
            'User-Agent'           = $script:DewDbUserAgent
            'Accept'               = 'application/vnd.github+json'
            'X-GitHub-Api-Version' = '2022-11-28'
        }
    } catch {
        throw "Could not query the latest DewDB release from GitHub ($uri): $($_.Exception.Message)"
    }
}

function Get-DewDbVersionFromTag {
    param([Parameter(Mandatory)][AllowEmptyString()][string]$Tag)

    if ([string]::IsNullOrWhiteSpace($Tag)) {
        throw 'The GitHub release did not include a tag name; cannot determine the latest version.'
    }
    $trimmed = $Tag.Trim()
    if ($trimmed -match '^[vV](?<version>.+)$') { return $matches['version'] }
    return $trimmed
}

function Get-DewDbReleaseAsset {
    param(
        [Parameter(Mandatory)]$Release,
        [Parameter(Mandatory)][string]$Name
    )

    $assets = @()
    if (($Release.PSObject.Properties.Name -contains 'assets') -and ($null -ne $Release.assets)) {
        $assets = @($Release.assets)
    }

    # Match the exact published asset name; never trust a guessed or near-miss filename.
    $match = @($assets | Where-Object { $_.name -ceq $Name })
    if ($match.Count -eq 0) {
        $available = if ($assets.Count -gt 0) { ($assets | ForEach-Object { $_.name }) -join ', ' } else { '(none)' }
        throw @"
Required release asset '$Name' was not found in the latest DewDB release.
Assets present: $available
Installation aborted.
"@
    }

    $asset = $match[0]
    if (($asset.PSObject.Properties.Name -notcontains 'browser_download_url') -or
        [string]::IsNullOrWhiteSpace($asset.browser_download_url)) {
        throw "Release asset '$Name' has no download URL."
    }
    return $asset
}

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

function Get-DewDbExpectedHash {
    param(
        [Parameter(Mandatory)][string]$SumsFile,
        [Parameter(Mandatory)][string]$FileName
    )

    $lines = @(Get-Content -LiteralPath $SumsFile)
    foreach ($line in $lines) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        # Standard sha256sum layout: "<64 hex>  <name>" (or "<64 hex> *<name>" in binary mode).
        if ($line.Trim() -match '^(?<hash>[0-9a-fA-F]{64})\s+\*?(?<name>.+)$') {
            $entry = $matches['name'].Trim()
            if ($entry.StartsWith('./')) { $entry = $entry.Substring(2) }
            if ($entry -ceq $FileName) { return $matches['hash'] }
        }
    }

    throw @"
No SHA256 entry for '$FileName' was found in $script:DewDbSumsName.
Cannot verify the download. Installation aborted.
"@
}

function Confirm-DewDbChecksum {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$ExpectedHash,
        [string]$DisplayName
    )

    if ([string]::IsNullOrWhiteSpace($DisplayName)) { $DisplayName = Split-Path -Leaf $Path }

    $expected = $ExpectedHash.Trim()
    $actual   = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.Trim()

    if (-not [string]::Equals($expected, $actual, [StringComparison]::OrdinalIgnoreCase)) {
        throw @"
Checksum verification FAILED for $DisplayName
  Expected: $expected
  Actual:   $actual
The download does not match the checksum published with the release.
Installation aborted - nothing was extracted and nothing was installed.
"@
    }

    Write-Verbose "Checksum verified for ${DisplayName}: $actual"
}

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

function Expand-DewDbArchive {
    param(
        [Parameter(Mandatory)][string]$ZipPath,
        [Parameter(Mandatory)][string]$Destination
    )

    if (-not (Test-Path -LiteralPath $Destination)) {
        New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    }

    try {
        Add-Type -AssemblyName System.IO.Compression.FileSystem -ErrorAction Stop
        [System.IO.Compression.ZipFile]::ExtractToDirectory($ZipPath, $Destination)
    } catch {
        Write-Verbose "ZipFile extraction unavailable ($($_.Exception.Message)); using Expand-Archive."
        Expand-Archive -LiteralPath $ZipPath -DestinationPath $Destination -Force
    }
}

function Find-DewDbExecutable {
    param([Parameter(Mandatory)][string]$Root)

    $direct = Join-Path $Root $script:DewDbExeName
    if (Test-Path -LiteralPath $direct -PathType Leaf) { return $direct }

    $found = @(Get-ChildItem -LiteralPath $Root -Filter $script:DewDbExeName -Recurse -File -ErrorAction SilentlyContinue)
    if ($found.Count -eq 0) {
        throw "$script:DewDbExeName was not found in the extracted release archive. Installation aborted."
    }
    return $found[0].FullName
}

function Install-DewDbBinary {
    param(
        [Parameter(Mandatory)][string]$SourceExe,
        [Parameter(Mandatory)][string]$InstallDir
    )

    if (-not (Test-Path -LiteralPath $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }

    $destination = Join-Path $InstallDir $script:DewDbExeName

    try {
        # Replaces any older DewDB previously installed at this exact location.
        Copy-Item -LiteralPath $SourceExe -Destination $destination -Force
    } catch {
        # A running dewdb.exe cannot be overwritten, but it can be renamed out of the way.
        $parked = "$destination.old-$(Get-Date -Format 'yyyyMMddHHmmss')"
        Move-Item -LiteralPath $destination -Destination $parked -Force
        Copy-Item -LiteralPath $SourceExe -Destination $destination -Force
        Write-Warning "The previous dewdb.exe was in use; it was moved aside to $parked."
    }

    # Best-effort tidy-up of binaries parked by earlier runs.
    Get-ChildItem -LiteralPath $InstallDir -Filter "$script:DewDbExeName.old-*" -File -ErrorAction SilentlyContinue |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }

    return $destination
}

function Install-DewDbLegalFile {
    <#
      Installs the license texts shipped in the release archive to the install root,
      alongside bin rather than inside it. Both files are part of the release contract,
      so both are confirmed present before either one is copied.
    #>
    param(
        [Parameter(Mandatory)][string]$PayloadRoot,
        [Parameter(Mandatory)][string]$InstallRoot
    )

    $sources = @{}
    foreach ($name in $script:DewDbLegalFiles) {
        $candidate = Join-Path $PayloadRoot $name
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            throw "$name was not found in the extracted release archive. Installation aborted."
        }
        $sources[$name] = $candidate
    }

    if (-not (Test-Path -LiteralPath $InstallRoot)) {
        New-Item -ItemType Directory -Path $InstallRoot -Force | Out-Null
    }

    $installed = @()
    foreach ($name in $script:DewDbLegalFiles) {
        $destination = Join-Path $InstallRoot $name
        Copy-Item -LiteralPath $sources[$name] -Destination $destination -Force
        $installed += $destination
    }
    return $installed
}

# ---------------------------------------------------------------------------
# PATH
# ---------------------------------------------------------------------------

function Send-DewDbEnvironmentChange {
    # Tell already-running shells and Explorer that the user environment changed.
    try {
        if (-not ('DewDb.NativeMethods' -as [type])) {
            $signature = @'
[DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
public static extern IntPtr SendMessageTimeout(IntPtr hWnd, uint Msg, UIntPtr wParam,
    string lParam, uint fuFlags, uint uTimeout, out UIntPtr lpdwResult);
'@
            Add-Type -MemberDefinition $signature -Name 'NativeMethods' -Namespace 'DewDb' -ErrorAction Stop | Out-Null
        }
        $result = [UIntPtr]::Zero
        [void][DewDb.NativeMethods]::SendMessageTimeout([IntPtr]0xffff, 0x1A, [UIntPtr]::Zero, 'Environment', 0x2, 5000, [ref]$result)
    } catch {
        Write-Verbose "Could not broadcast the environment change: $($_.Exception.Message)"
    }
}

function Test-DewDbPathEntry {
    # True when a raw PATH entry refers to $Target, our install directory.
    # An empty entry never matches, so empty entries are always treated as unrelated.
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string]$Entry,
        [Parameter(Mandatory)][string]$Target
    )

    $trimmed = $Entry.Trim()
    if ([string]::IsNullOrEmpty($trimmed)) { return $false }
    return ([Environment]::ExpandEnvironmentVariables($trimmed).TrimEnd('\') -ieq $Target)
}

function Update-DewDbUserPath {
    <#
      Puts $Directory first in the *user* PATH and guarantees it appears exactly once.
      Reads and writes the raw registry value so that entries stored as REG_EXPAND_SZ
      (for example %USERPROFILE%\bin) are preserved unexpanded and the value kind is not
      silently downgraded. Every entry that is not our own install directory is carried
      over byte for byte, including empty entries; nothing unrelated is normalised,
      reordered or dropped. The machine PATH is never touched.
      Returns $true when the stored PATH was modified.
    #>
    param([Parameter(Mandatory)][string]$Directory)

    $target = $Directory.TrimEnd('\')
    $key = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment', $true)
    if ($null -eq $key) { throw 'Could not open HKCU\Environment to update the user PATH.' }

    $changed = $false
    try {
        $names = @($key.GetValueNames() | Where-Object { $_ -ieq 'Path' })
        $valueName = if ($names.Count -gt 0) { $names[0] } else { 'Path' }

        $raw  = ''
        $kind = [Microsoft.Win32.RegistryValueKind]::ExpandString
        $existing = $key.GetValue($valueName, $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
        if ($null -ne $existing) {
            $raw  = [string]$existing
            $kind = $key.GetValueKind($valueName)
        }
        if ($kind -ne [Microsoft.Win32.RegistryValueKind]::String -and
            $kind -ne [Microsoft.Win32.RegistryValueKind]::ExpandString) {
            $kind = [Microsoft.Win32.RegistryValueKind]::ExpandString
        }

        $kept = New-Object System.Collections.Generic.List[string]
        foreach ($entry in ($raw -split ';')) {
            # Drop only our own directory; keep every other entry exactly as written.
            if (Test-DewDbPathEntry -Entry $entry -Target $target) { continue }
            $kept.Add($entry)
        }

        $updated = if ([string]::IsNullOrEmpty($raw)) { $target }
                   else { (@($target) + $kept.ToArray()) -join ';' }
        if ($updated -cne $raw) {
            $key.SetValue($valueName, $updated, $kind)
            $changed = $true
        }
    } finally {
        $key.Dispose()
    }

    if ($changed) { Send-DewDbEnvironmentChange }
    return $changed
}

function Update-DewDbSessionPath {
    # Same contract as Update-DewDbUserPath, applied to the current process only.
    param([Parameter(Mandatory)][string]$Directory)

    $target = $Directory.TrimEnd('\')
    $current = [string]$env:PATH
    if ([string]::IsNullOrEmpty($current)) {
        $env:PATH = $target
        return
    }

    $kept = New-Object System.Collections.Generic.List[string]
    foreach ($entry in ($current -split ';')) {
        if (Test-DewDbPathEntry -Entry $entry -Target $target) { continue }
        $kept.Add($entry)
    }
    $env:PATH = (@($target) + $kept.ToArray()) -join ';'
}

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

function Invoke-DewDbInstall {
    [CmdletBinding()]
    param()

    Initialize-DewDbTls
    $arch = Assert-DewDbArchitecture
    Write-Host "Detected architecture: $arch"

    $installRoot = Get-DewDbInstallRoot
    $installDir  = Get-DewDbInstallDir

    Write-Host 'Looking up the latest DewDB release...'
    $release = Get-DewDbLatestRelease
    $tag = if ($release.PSObject.Properties.Name -contains 'tag_name') { [string]$release.tag_name } else { '' }
    $version = Get-DewDbVersionFromTag -Tag $tag
    Write-Host "Latest release: $tag"

    $zipName   = "dewdb-v$version-windows-x86_64.zip"
    $zipAsset  = Get-DewDbReleaseAsset -Release $release -Name $zipName
    $sumsAsset = Get-DewDbReleaseAsset -Release $release -Name $script:DewDbSumsName

    $workDir = $null
    try {
        $workDir = Join-Path ([System.IO.Path]::GetTempPath()) ('dewdb-install-' + [Guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Path $workDir -Force | Out-Null

        $zipPath  = Join-Path $workDir $zipName
        $sumsPath = Join-Path $workDir $script:DewDbSumsName

        Write-Host "Downloading $zipName..."
        Invoke-DewDbDownload -Uri $zipAsset.browser_download_url -OutFile $zipPath
        Write-Host "Downloading $script:DewDbSumsName..."
        Invoke-DewDbDownload -Uri $sumsAsset.browser_download_url -OutFile $sumsPath

        Write-Host 'Verifying SHA256 checksum...'
        $expected = Get-DewDbExpectedHash -SumsFile $sumsPath -FileName $zipName
        # Nothing below this line runs unless the archive matches the published checksum.
        Confirm-DewDbChecksum -Path $zipPath -ExpectedHash $expected -DisplayName $zipName
        Write-Host 'Checksum OK.'

        $extractDir = Join-Path $workDir 'extracted'
        Expand-DewDbArchive -ZipPath $zipPath -Destination $extractDir
        $sourceExe = Find-DewDbExecutable -Root $extractDir

        $destination = Install-DewDbBinary -SourceExe $sourceExe -InstallDir $installDir
        $null = Install-DewDbLegalFile -PayloadRoot (Split-Path -Parent $sourceExe) -InstallRoot $installRoot

        $pathChanged = Update-DewDbUserPath -Directory $installDir
        Update-DewDbSessionPath -Directory $installDir

        $versionOutput = & $destination --version
        if ($LASTEXITCODE -ne 0) {
            throw "The installed binary at $destination exited with code $LASTEXITCODE when run with --version."
        }
        $versionText = @($versionOutput) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -First 1
        if ($null -eq $versionText) { $versionText = "dewdb $version" }

        Write-Host ''
        Write-Host 'DewDB installed successfully.'
        Write-Host ''
        Write-Host "Version:  $(([string]$versionText).Trim())"
        Write-Host "Location: $destination"
        Write-Host ''
        Write-Host 'Get started:'
        Write-Host '  dewdb init'
        Write-Host '  dewdb'
        if ($pathChanged) {
            Write-Host ''
            Write-Host "Added $installDir to your user PATH. Open a new terminal to pick it up."
        }
    } finally {
        if ($workDir -and (Test-Path -LiteralPath $workDir)) {
            Remove-Item -LiteralPath $workDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

if (-not $LoadFunctionsOnly) {
    Invoke-DewDbInstall
}
