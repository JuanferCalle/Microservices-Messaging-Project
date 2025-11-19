# Build all service images using Docker (for Docker Desktop Kubernetes)
# Run from repository root: `.build_docker_images.ps1`

$services = @(
  'admission',
  'triage',
  'vitals',
  'doctor-assignment',
  'lab',
  'diagnosis',
  'treatment',
  'pharmacy',
  'billing',
  'discharge',
  'monitor',
  'frontend'
)

$cwd = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $cwd

foreach ($s in $services) {
  $tag = "hospital-$($s):latest"
  Write-Host "Building $s -> $tag"
  docker build -t $tag .\$s
}

Write-Host "All images built. You can now run: kubectl apply -f k8s/manifests.yaml -n hospital"