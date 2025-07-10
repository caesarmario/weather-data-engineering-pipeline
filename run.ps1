# run.ps1

param (
    [string]$Action
)

$composeFile = "docker/docker-compose.yml"

switch ($Action) {
    "up" {
        docker-compose -f $composeFile up --build
    }
    "down" {
        docker-compose -f $composeFile down -v --remove-orphans
        docker volume prune -f
    }
    default {
        Write-Host "Usage: .\run.ps1 [up|down]"
    }
}
