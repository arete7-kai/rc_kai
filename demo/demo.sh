#!/usr/bin/env bash
# demo.sh — 往 relay 打一批命中不同 destination 的通知，再查状态/死信，
# 让人亲眼看到：ad-system 成功、crm 重试几次后成功、inventory 重试到上限进死信。
#
# 前置：docker compose 起库并 seed、mock upstream 已起、relay 已起（含 SSRF_ALLOW_HOSTS=127.0.0.1）。
# 详见 README §10。依赖：curl（无需 jq）。Windows 用 Git Bash 运行。
set -euo pipefail

RELAY="${RELAY_URL:-http://localhost:8080}"

# 从 202 响应 {"id":"..."} 里抽出 id（不依赖 jq）。
extract_id() { sed -n 's/.*"id":"\([^"]*\)".*/\1/p'; }

post() { # $1=destination_id  $2=idempotency-key  $3=body-json
    curl -s -X POST "$RELAY/notifications" \
        -H 'Content-Type: application/json' \
        -H "Idempotency-Key: $2" \
        -d "{\"destination_id\":\"$1\",\"body\":$3}"
}

echo "== 1) 提交一批通知 =="
AD=$(post ad-system  demo-ad-1  '{"event":"signup","user":1}'            | extract_id)
CRM=$(post crm       demo-crm-1 '{"event":"paid","contact":42,"status":"paid"}' | extract_id)
INV=$(post inventory demo-inv-1 '{"event":"order","sku":"X-1","delta":-1}'      | extract_id)
echo "ad-system  -> $AD"
echo "crm        -> $CRM"
echo "inventory  -> $INV"

echo
echo "== 2) 入口幂等：用同一 Idempotency-Key 重复提交 ad-system，应返回同一 id =="
AD2=$(post ad-system demo-ad-1 '{"event":"signup","user":1}' | extract_id)
echo "重复提交 -> $AD2"
[ "$AD" = "$AD2" ] && echo "OK：id 相同，未重复入队" || echo "WARN：id 不同（预期应相同）"

echo
echo "== 3) 等待 worker 投递 + 退避重试（约 10s）=="
sleep 10

echo
echo "== 4) 查状态 =="
for id in "$AD" "$CRM" "$INV"; do
    echo "--- $id ---"
    curl -s "$RELAY/notifications/$id"
    echo
done
echo
echo "预期：ad-system=DELIVERED；crm=DELIVERED(attempts≈3)；inventory=DEAD(attempts=max=3)"

echo
echo "== 5) 重投死信：手动重投 inventory（会再次失败并重新进死信，演示“可重投”链路本身）=="
curl -s -X POST "$RELAY/notifications/$INV/retry"
echo
echo "（重投后状态回到 PENDING，稍后会再次重试到上限）"
