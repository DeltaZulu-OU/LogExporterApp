# Verifies that a release zip built on another OS has the same content as the published one.
# The published zip is never modified; this only fails the release when the builds diverge.
#
# Compared per entry:
#   - the set of file names must be identical;
#   - text files are compared with line endings normalized, because a Windows checkout uses CRLF;
#   - the app's own assembly and PDB embed OS-specific build paths, so the assembly is compared
#     by identity (name, version, public key token) and the PDB by presence only;
#   - every other file, i.e. NuGet-supplied assemblies, must be byte-identical.
param(
    [Parameter(Mandatory)] [string] $AppName,
    [Parameter(Mandatory)] [string] $ExpectedZip,
    [Parameter(Mandatory)] [string] $ActualZip
)

$ErrorActionPreference = 'Stop'

$textExtensions = @('.json', '.config', '.md')
$root = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid())
$expectedPath = Join-Path $root 'expected'
$actualPath = Join-Path $root 'actual'

Expand-Archive -Path $ExpectedZip -DestinationPath $expectedPath
Expand-Archive -Path $ActualZip -DestinationPath $actualPath

function Get-Entries([string] $path) {
    Get-ChildItem -Path $path -File -Recurse |
        ForEach-Object { [System.IO.Path]::GetRelativePath($path, $_.FullName).Replace('\', '/') } |
        Sort-Object
}

function Get-TextHash([string] $file) {
    $text = [System.IO.File]::ReadAllText($file).Replace("`r`n", "`n")
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
    [System.Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($bytes))
}

$expectedEntries = @(Get-Entries $expectedPath)
$actualEntries = @(Get-Entries $actualPath)
$failures = [System.Collections.Generic.List[string]]::new()

$setDiff = Compare-Object $expectedEntries $actualEntries
foreach ($d in $setDiff) {
    $side = if ($d.SideIndicator -eq '<=') { 'only in expected' } else { 'only in actual' }
    $failures.Add("$($d.InputObject): $side")
}

foreach ($entry in $expectedEntries | Where-Object { $actualEntries -contains $_ }) {
    $expectedFile = Join-Path $expectedPath $entry
    $actualFile = Join-Path $actualPath $entry
    $extension = [System.IO.Path]::GetExtension($entry).ToLowerInvariant()

    if ($entry -eq "$AppName.pdb") {
        continue
    }

    if ($entry -eq "$AppName.dll") {
        $expectedName = [System.Reflection.AssemblyName]::GetAssemblyName($expectedFile).FullName
        $actualName = [System.Reflection.AssemblyName]::GetAssemblyName($actualFile).FullName
        if ($expectedName -ne $actualName) {
            $failures.Add("${entry}: assembly identity differs ('$expectedName' vs '$actualName')")
        }
        continue
    }

    if ($textExtensions -contains $extension) {
        if ((Get-TextHash $expectedFile) -ne (Get-TextHash $actualFile)) {
            $failures.Add("${entry}: content differs")
        }
        continue
    }

    if ((Get-FileHash $expectedFile -Algorithm SHA256).Hash -ne (Get-FileHash $actualFile -Algorithm SHA256).Hash) {
        $failures.Add("${entry}: binary content differs")
    }
}

Remove-Item -Path $root -Recurse -Force

if ($failures.Count -gt 0) {
    $failures | ForEach-Object { Write-Host "::error::Release package mismatch: $_" }
    throw "Release packages differ in $($failures.Count) entr$(if ($failures.Count -eq 1) { 'y' } else { 'ies' })."
}

Write-Host "Release packages match ($($expectedEntries.Count) entries)."
