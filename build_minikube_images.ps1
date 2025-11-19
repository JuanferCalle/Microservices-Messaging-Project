# PowerShell script to build and load service images into Minikube
# Run this after `minikube start --driver=docker`

$cwd = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $cwd

$services = @(
  'triage',
  'vitals',
  'doctor-assignment',
  'lab',
  'diagnosis',
  'treatment',
  'pharmacy',
  'billing',
  'discharge',
  'monitor'
)

foreach ($s in $services) {
  $tag = "hospital-$($s -replace 'doctor-assignment','doctor-assignment')`:latest"
  Write-Host "Building image for $s -> $tag"
  minikube image build -t $tag .\$s
}

Write-Host "All images built and loaded into Minikube."
