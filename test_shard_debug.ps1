# Automatic token extraction and API testing script

$baseUrl = "http://localhost:5000"
$username = "aarav"
$password = "Aarav@123"

Write-Host "[LOCK] Logging in as $username..." -ForegroundColor Cyan

# Step 1: Login and extract token
try {
    $loginResponse = Invoke-WebRequest -Uri "$baseUrl/api/auth/login" `
        -Method POST `
        -ContentType "application/json" `
        -UseBasicParsing `
        -Body (ConvertTo-Json @{
            username = $username
            password = $password
            portal_role = "member"
        }) -ErrorAction Stop

    $loginData = $loginResponse.Content | ConvertFrom-Json
    $sessionToken = $loginData.session_token

    Write-Host "[OK] Login successful!" -ForegroundColor Green
    Write-Host "[TOKEN] Session Token: $sessionToken" -ForegroundColor Yellow
    Write-Host ""
}
catch {
    Write-Host "[ERROR] Login failed: $_" -ForegroundColor Red
    exit 1
}

# Step 2: Test the API with shard debug enabled
Write-Host "[SEARCH] Testing customer lookup with shard debug..." -ForegroundColor Cyan

$apiUrl = "$baseUrl/api/project/customers/1?session_token=$sessionToken" + "&include_shard_debug=1"

try {
    $response = Invoke-WebRequest -Uri $apiUrl `
        -UseBasicParsing `
        -ErrorAction Stop
    $data = $response.Content | ConvertFrom-Json
    
    Write-Host "[OK] API Response received!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Customer Data:" -ForegroundColor Cyan
    Write-Host ($data | ConvertTo-Json -Depth 10)
}
catch {
    Write-Host "[ERROR] API call failed: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[SUCCESS] Test completed successfully!" -ForegroundColor Green
