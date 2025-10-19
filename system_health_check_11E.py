#!/usr/bin/env python3
# system_health_check_11E.py — Phase 11-E: Multi-Service Health & Deployment Validation
# Usage: python system_health_check_11E.py [--ci]
# Requires: requests, json, time

import requests
import json
import time
import sys
from datetime import datetime
import uuid

# Phase 11-E: Multi-service configuration
SERVICES = {
    "app": "https://sara-ai-core-app.onrender.com",
    "streaming": "https://sara-ai-core-streaming.onrender.com", 
    "worker": "https://sara-ai-core-worker.onrender.com"
}

ENDPOINTS = ["/healthz", "/health", "/metrics"]

SESSION_ID = str(uuid.uuid4())
HEADERS = {
    "Content-Type": "application/json",
    "X-Trace-Source": "system-health-check",
    "X-Test-Phase": "11-E"
}

CI_MODE = "--ci" in sys.argv

def exponential_backoff_retry(func, max_retries=3, base_delay=1):
    """Phase 11-E: Retry helper with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(base_delay * (2 ** attempt))
    return None

def safe_get_json(url):
    """Enhanced with retry logic"""
    def _request():
        r = requests.get(url, timeout=10, headers=HEADERS)
        r.raise_for_status()
        return r.json()
    
    try:
        return exponential_backoff_retry(_request)
    except Exception as e:
        return {"error": str(e), "status": "failed"}

def safe_get_text(url):
    """Get raw text response with retry logic"""
    def _request():
        r = requests.get(url, timeout=10, headers=HEADERS)
        r.raise_for_status()
        return r.text
    
    try:
        return exponential_backoff_retry(_request)
    except Exception as e:
        return f"Error: {str(e)}"

def parse_prometheus_metrics(text):
    """Parse Prometheus metrics with enhanced error handling"""
    metrics = {}
    if text.startswith("Error:"):
        return metrics
        
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        try:
            parts = line.strip().split(" ")
            if len(parts) >= 2:
                k, v = parts[0], parts[1]
                metrics[k] = float(v)
        except ValueError:
            continue
    return metrics

def validate_r2_object(audio_url):
    """Phase 11-E: Validate R2 object accessibility"""
    if not audio_url or not audio_url.startswith("http"):
        return {"status": "invalid_url", "accessible": False}
    
    try:
        response = requests.head(audio_url, timeout=10, allow_redirects=True)
        return {
            "status": "success" if response.status_code == 200 else "failed",
            "accessible": response.status_code == 200,
            "http_status": response.status_code
        }
    except Exception as e:
        return {"status": "error", "accessible": False, "error": str(e)}

def validate_redis_connectivity(service_url):
    """Phase 11-E: Validate Redis connectivity via health endpoint"""
    try:
        health_data = safe_get_json(f"{service_url}/health")
        if health_data and isinstance(health_data, dict):
            redis_status = health_data.get("redis_status", "unknown")
            return {
                "status": "success" if redis_status == "ok" else "degraded",
                "redis_status": redis_status,
                "accessible": redis_status == "ok"
            }
    except Exception:
        pass
    
    return {"status": "failed", "accessible": False}

def run_tts_test(service_url):
    """Enhanced TTS test with R2 validation"""
    payload = {
        "session_id": SESSION_ID, 
        "text": "Phase 11-E system health validation message"
    }
    
    try:
        response = requests.post(
            f"{service_url}/tts_test", 
            json=payload, 
            headers=HEADERS, 
            timeout=20
        )
        
        result = {
            "status": response.status_code,
            "response": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text
        }
        
        # Phase 11-E: Validate R2 object if audio URL is returned
        if (result["status"] == 200 and 
            isinstance(result["response"], dict) and 
            "audio_url" in result["response"]):
            
            result["r2_validation"] = validate_r2_object(result["response"]["audio_url"])
        
        return result
        
    except Exception as e:
        return {"error": str(e), "status": "failed"}

def check_service_health(service_name, service_url):
    """Comprehensive service health check"""
    service_result = {
        "service": service_name,
        "url": service_url,
        "endpoints": {},
        "overall_status": "healthy"
    }
    
    # Check all endpoints
    for endpoint in ENDPOINTS:
        full_url = f"{service_url}{endpoint}"
        start_time = time.time()
        
        if endpoint == "/metrics":
            response_text = safe_get_text(full_url)
            latency_ms = (time.time() - start_time) * 1000
            metrics_data = parse_prometheus_metrics(response_text)
            
            service_result["endpoints"][endpoint] = {
                "status": "healthy" if response_text and not response_text.startswith("Error:") else "failed",
                "latency_ms": round(latency_ms, 2),
                "metrics_count": len(metrics_data),
                "sample_metrics": dict(list(metrics_data.items())[:3])  # First 3 metrics as sample
            }
        else:
            response_data = safe_get_json(full_url)
            latency_ms = (time.time() - start_time) * 1000
            
            service_result["endpoints"][endpoint] = {
                "status": "healthy" if "error" not in response_data else "failed",
                "latency_ms": round(latency_ms, 2),
                "response": response_data
            }
            
            # Update overall status if any endpoint fails
            if service_result["endpoints"][endpoint]["status"] == "failed":
                service_result["overall_status"] = "degraded"
    
    # Phase 11-E: Redis connectivity check
    service_result["redis_validation"] = validate_redis_connectivity(service_url)
    if service_result["redis_validation"]["status"] != "success":
        service_result["overall_status"] = "degraded"
    
    return service_result

def main():
    print("\n🚀 Starting Phase 11-E Multi-Service Health Validation")
    print(f"→ Services: {', '.join(SERVICES.keys())}")
    print(f"→ Session ID: {SESSION_ID}")
    print(f"→ CI Mode: {CI_MODE}\n")
    
    result = {
        "phase": "11-E",
        "schema_version": "phase_11e_v1",
        "start_time": datetime.utcnow().isoformat() + "Z",
        "services": {},
        "overall_status": "healthy"
    }
    
    # Step 1 — Baseline metrics from app service
    print("📊 Fetching baseline metrics snapshot...")
    baseline_metrics_text = safe_get_text(f"{SERVICES['app']}/metrics")
    baseline_metrics = parse_prometheus_metrics(baseline_metrics_text)
    result["metrics_before"] = {
        "total_metrics": len(baseline_metrics),
        "sample_metrics": dict(list(baseline_metrics.items())[:5])
    }
    
    # Step 2 — Comprehensive service health checks
    print("🔍 Performing multi-service health checks...")
    for service_name, service_url in SERVICES.items():
        print(f"  → Checking {service_name} service...")
        service_result = check_service_health(service_name, service_url)
        result["services"][service_name] = service_result
        
        # Update overall status
        if service_result["overall_status"] != "healthy":
            result["overall_status"] = "degraded"
    
    # Step 3 — TTS functionality test
    print("🗣️  Running TTS functionality test...")
    tts_result = run_tts_test(SERVICES['app'])
    result["tts_test"] = tts_result
    
    # Step 4 — Post-test metrics comparison
    print("📈 Capturing post-test metrics...")
    time.sleep(3)  # Allow metric propagation
    after_metrics_text = safe_get_text(f"{SERVICES['app']}/metrics") 
    after_metrics = parse_prometheus_metrics(after_metrics_text)
    result["metrics_after"] = {
        "total_metrics": len(after_metrics),
        "sample_metrics": dict(list(after_metrics.items())[:5])
    }
    
    # Step 5 — Calculate key metric deltas
    result["prometheus_deltas"] = {
        "health_checks_delta": after_metrics.get("sara_health_checks_total", 0) - baseline_metrics.get("sara_health_checks_total", 0),
        "tts_requests_delta": after_metrics.get("sara_tts_requests_total", 0) - baseline_metrics.get("sara_tts_requests_total", 0),
        "tts_failures_delta": after_metrics.get("sara_tts_failures_total", 0) - baseline_metrics.get("sara_tts_failures_total", 0)
    }
    
    # Step 6 — Final assessment
    result["end_time"] = datetime.utcnow().isoformat() + "Z"
    result["duration_seconds"] = round(
        (datetime.fromisoformat(result["end_time"].replace('Z', '+00:00')) - 
         datetime.fromisoformat(result["start_time"].replace('Z', '+00:00'))).total_seconds(), 2
    )
    
    # Output results
    if CI_MODE:
        # Compact output for CI/CD pipelines
        print(f"\n✅ HEALTH_CHECK_SUMMARY: {result['overall_status']}")
        print(f"📊 SERVICES: {len([s for s in result['services'].values() if s['overall_status'] == 'healthy'])}/{len(result['services'])} healthy")
        print(f"🔢 METRICS_DELTA: health_checks+{result['prometheus_deltas']['health_checks_delta']}")
        if "r2_validation" in result.get("tts_test", {}):
            r2_status = result["tts_test"]["r2_validation"]["status"]
            print(f"☁️  R2_ACCESS: {r2_status}")
    else:
        # Detailed output for manual runs
        print("\n✅ Phase 11-E health validation complete.\n")
        print(json.dumps(result, indent=2))
    
    # Always write detailed results to file
    timestamp = int(time.time())
    filename = f"system_health_summary_11E_{timestamp}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
        if not CI_MODE:
            print(f"\n📁 Detailed results written to {filename}")
    
    # Exit code for CI/CD (0 = success, 1 = degraded/failed)
    exit_code = 0 if result["overall_status"] == "healthy" else 1
    if CI_MODE:
        sys.exit(exit_code)
    else:
        print(f"\nExit code: {exit_code}")

if __name__ == "__main__":
    main()