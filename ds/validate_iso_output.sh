#!/bin/bash
# Validasi ISO output tanpa rg (menggunakan grep/awk/jq)
# Usage: ./validate_iso_output.sh [timestamp]

set -euo pipefail

DIR="output/iso_compliant"
TIMESTAMP=${1:-}

if [ -z "$TIMESTAMP" ]; then
    LATEST_AUDIT=$(ls -1t "$DIR"/*_audit.json 2>/dev/null | head -n 1 || true)
    if [ -n "$LATEST_AUDIT" ]; then
        PREFIX="${LATEST_AUDIT%_audit.json}"
        TIMESTAMP=$(basename "$PREFIX" | cut -d_ -f1)
    else
        TIMESTAMP=$(date +%Y%m%dT%H%M%SZ)
        PREFIX="${DIR}/${TIMESTAMP}"
    fi
else
    PREFIX="${DIR}/${TIMESTAMP}"
fi

MISSING=0

echo "🔍 Validating ISO 8000 Output Files..."
echo "   Directory: $DIR"
echo "   Timestamp: $TIMESTAMP"

# Cek direktori
if [ ! -d "$DIR" ]; then
    echo "❌ FAIL: Directory $DIR not found"
    exit 1
fi

# Cek 5 file wajib
FILES=(
    "${PREFIX}_audit.json"
    "${PREFIX}_provenance.json"
    "${PREFIX}_summary.json"
    "${PREFIX}_dashboard.html"
    "${PREFIX}_metrics.csv"
)

for f in "${FILES[@]}"; do
    if [ -f "$f" ]; then
        SIZE=$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f" 2>/dev/null)
        echo "✅ $(basename "$f"): ${SIZE} bytes"
    else
        echo "❌ MISSING: $(basename "$f")"
        MISSING=$((MISSING + 1))
    fi
done

if [ $MISSING -gt 0 ]; then
    echo "❌ FAIL: $MISSING files missing"
    exit 1
fi

# Validasi JSON structure dengan grep (tanpa rg)
echo ""
echo "📋 JSON Structure Validation (using grep):"

# 1. Cek key wajib di audit.json
AUDIT="${PREFIX}_audit.json"
REQUIRED_KEYS=(
    "metadata"
    "quality_dimensions"
    "semantic_quality"
    "final_assessment"
    "certification_ready"
    "compliance_level"
)

echo "   Checking $AUDIT:"
for key in "${REQUIRED_KEYS[@]}"; do
    if grep -q "\"$key\"" "$AUDIT" 2>/dev/null; then
        echo "   ✅ Key '$key' found"
    else
        echo "   ❌ Key '$key' MISSING"
        MISSING=$((MISSING + 1))
    fi
done

# 2. Cek quality_dimensions punya score
if grep -q '"score":' "$AUDIT"; then
    SCORE_COUNT=$(grep -o '"score":' "$AUDIT" | wc -l | awk '{print $1}')
    echo "   ✅ Found $SCORE_COUNT quality scores"
else
    echo "   ❌ No quality scores found"
    MISSING=$((MISSING + 1))
fi

# 3. Cek summary.json (compact validation)
SUMMARY="${PREFIX}_summary.json"
if [ -f "$SUMMARY" ]; then
    if grep -q '"certified":' "$SUMMARY"; then
        CERT=$(grep -o '"certified": *[^,}]*' "$SUMMARY" | head -n 1 | cut -d: -f2 | tr -d ' ')
        echo "   ✅ Summary certified flag: $CERT"
    else
        echo "   ❌ Summary certified flag MISSING"
        MISSING=$((MISSING + 1))
    fi

    if grep -q '"compliance_level":' "$SUMMARY"; then
        LEVEL=$(grep -o '"compliance_level": *[0-9]*' "$SUMMARY" | head -n 1 | grep -o '[0-9]*')
        echo "   ✅ Compliance level: $LEVEL"
    else
        echo "   ❌ Summary compliance level MISSING"
        MISSING=$((MISSING + 1))
    fi
fi

# 4. Cek provenance.json
PROV="${PREFIX}_provenance.json"
if [ -f "$PROV" ]; then
    if grep -q '"@context"' "$PROV"; then
        echo "   ✅ W3C PROV context found"
    else
        echo "   ❌ W3C PROV context MISSING"
        MISSING=$((MISSING + 1))
    fi
fi

# 5. Cek HTML dashboard
HTML="${PREFIX}_dashboard.html"
if [ -f "$HTML" ]; then
    if grep -q "ISO 8000" "$HTML"; then
        echo "   ✅ HTML dashboard title valid"
    else
        echo "   ❌ HTML dashboard title MISSING"
        MISSING=$((MISSING + 1))
    fi
    if grep -q "qualityDimensions" "$HTML" || grep -q "progress-bar" "$HTML"; then
        echo "   ✅ HTML quality metrics found"
    else
        echo "   ❌ HTML quality metrics MISSING"
        MISSING=$((MISSING + 1))
    fi
fi

# 6. Cek CSV metrics
CSV="${PREFIX}_metrics.csv"
if [ -f "$CSV" ]; then
    ROWS=$(wc -l < "$CSV")
    echo "   ✅ CSV metrics: $ROWS rows (including header)"
fi

# Optional JSON parse check bila jq tersedia
if command -v jq >/dev/null 2>&1; then
    echo ""
    echo "📦 jq validation enabled"
    for json_file in "$AUDIT" "$SUMMARY" "$PROV"; do
        if jq empty "$json_file" >/dev/null 2>&1; then
            echo "   ✅ $(basename "$json_file") is valid JSON"
        else
            echo "   ❌ $(basename "$json_file") is invalid JSON"
            MISSING=$((MISSING + 1))
        fi
    done
fi

# Final verdict
echo ""
if [ $MISSING -eq 0 ]; then
    echo "🎉 ALL VALIDATIONS PASSED"
    echo "✅ ISO 8000 output is complete and valid"
    exit 0
else
    echo "❌ VALIDATION FAILED: $MISSING issues found"
    exit 1
fi
