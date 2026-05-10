param(
    [Parameter(Mandatory = $true)]
    [string]$Name,

    [Parameter(Mandatory = $true)]
    [string]$Email
)

& 'C:\Program Files\Git\cmd\git.exe' config --local user.name $Name
& 'C:\Program Files\Git\cmd\git.exe' config --local user.email $Email

Write-Host "Configured local Git identity:"
& 'C:\Program Files\Git\cmd\git.exe' config --local user.name
& 'C:\Program Files\Git\cmd\git.exe' config --local user.email

