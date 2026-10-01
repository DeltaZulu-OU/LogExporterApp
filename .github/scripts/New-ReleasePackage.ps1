# Stages the build output and creates the release zip.
# Shared by the Windows and Linux release jobs so both package the same file set.
param(
    [Parameter(Mandatory)] [string] $AppName,
    [Parameter(Mandatory)] [string] $Version
)

$ErrorActionPreference = 'Stop'

$outputPath = 'src/bin/Release'
$configPath = "$outputPath/dnsApp.config"
$readmePath = 'README.md'
$pdbPath    = "$outputPath/$AppName.pdb"
$depsPath   = "$outputPath/$AppName.deps.json"

if (-not (Test-Path $outputPath)) {
    throw "Build output folder not found at '$outputPath'."
}

$dlls = Get-ChildItem -Path $outputPath -Filter '*.dll' -File

if (-not $dlls) {
    throw "No DLL files found in '$outputPath'."
}

if (-not (Test-Path $configPath)) {
    throw "DNS app config file not found at '$configPath'."
}

if (-not (Test-Path $readmePath)) {
    throw "README file not found at '$readmePath'."
}

$stagingPath = "release/$AppName"
$zipPath = "release/$AppName-$Version.zip"

New-Item -ItemType Directory -Path $stagingPath -Force | Out-Null
foreach ($dll in $dlls) { Copy-Item $dll.FullName "$stagingPath/$($dll.Name)" }
Copy-Item $pdbPath "$stagingPath/$AppName.pdb"
Copy-Item $depsPath "$stagingPath/$AppName.deps.json"
Copy-Item $configPath "$stagingPath/dnsApp.config"
Copy-Item $readmePath "$stagingPath/README.md"

Compress-Archive -Path "$stagingPath/*" -DestinationPath $zipPath -Force

return $zipPath
