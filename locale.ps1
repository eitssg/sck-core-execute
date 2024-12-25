
# set the current folder to core_execute/locale/en/LC_MESSAGES
# Powershell

# Check if the folder exists and fail if it does not
if (-not (Test-Path core_execute/locale/en/LC_MESSAGES)) {
    Write-Error "Folder core_execute/locale/en/LC_MESSAGES does not exist.  You need to be in the parent folder."
    exit 1
}

$currentPath = Get-Location

Set-Location -Path core_execute/locale/en/LC_MESSAGES
# Define the path to the msgfmt executable

$msgfmtPath = "C:\Program Files\gettext-iconv\bin\msgfmt.exe"

# Define the input .po file and output .mo file
$poFile = "messages.po"
$moFile = "messages.mo"

# Run the msgfmt command to compile the .po file into a .mo file
& $msgfmtPath -o $moFile $poFile

Set-Location -Path $currentPath
