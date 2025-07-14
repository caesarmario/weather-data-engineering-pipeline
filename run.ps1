####
## PowerShell Script to Control Docker Compose
## Mario Caesar // caesarmario87@gmail.com
## Usage:
##   .\run.ps1 up     → Starts all services with build
##   .\run.ps1 down   → Stops and removes all services and volumes
####

param (
    [string]$Action
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$envFile = Join-Path $projectRoot "docker\.env"

switch ($Action) {
    "up" {
        docker-compose --env-file "$envFile" -f "$composeFile" up --build
    }
    "down" {
        docker-compose --env-file "$envFile" -f "$composeFile" down -v --remove-orphans
        docker volume prune -f
    }
    default {
        Write-Host "Usage: .\run.ps1 [up|down]"
    }
}
