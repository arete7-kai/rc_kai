-- Demo 注册表种子：三个 destination，覆盖 成功 / flaky重试后成功 / 重试到上限进死信 三条演示路径。
-- URL 指向本地 mock upstream（见 mock/），行为由 query 参数控制。
-- secret_ref 用占位引用（vault://…），绝不放明文密钥。
--
-- 注意：mock 跑在 127.0.0.1，会被 SSRF 私网/回环拦截拦掉；运行 demo 时需给 relay 设
--       SSRF_ALLOW_HOSTS=127.0.0.1（仅本地 dev/demo 放行；生产必须留空）。见 README §6 / §10。

INSERT INTO destinations
    (id, url, method, headers, secret_ref, severity, max_attempts, backoff_cap_secs, alert_on_dead, requires_ordering)
VALUES
    -- ad-system：稳定 200 → 一次投递成功。低危、不告警。
    ('ad-system',
     'http://127.0.0.1:9090/?behavior=ok',
     'POST', '{"Content-Type":"application/json"}'::jsonb,
     'vault://ad-system/token', 'low', 5, 3600, false, false),

    -- crm：flaky（前 2 次 500、第 3 次 200）→ 重试几次后成功。高危、告警、顺序敏感（D5 仅标注不实现）。
    ('crm',
     'http://127.0.0.1:9090/?behavior=flaky&n=2',
     'POST', '{"Content-Type":"application/json"}'::jsonb,
     'vault://crm/token', 'high', 10, 60, true, true),

    -- inventory：稳定 500 → 重试到上限进死信。高危、告警。max_attempts 调小以便 demo 快速触顶。
    ('inventory',
     'http://127.0.0.1:9090/?behavior=fail',
     'POST', '{"Content-Type":"application/json"}'::jsonb,
     'vault://inventory/token', 'high', 3, 30, true, false)
ON CONFLICT (id) DO UPDATE SET
    url               = EXCLUDED.url,
    method            = EXCLUDED.method,
    headers           = EXCLUDED.headers,
    secret_ref        = EXCLUDED.secret_ref,
    severity          = EXCLUDED.severity,
    max_attempts      = EXCLUDED.max_attempts,
    backoff_cap_secs  = EXCLUDED.backoff_cap_secs,
    alert_on_dead     = EXCLUDED.alert_on_dead,
    requires_ordering = EXCLUDED.requires_ordering,
    updated_at        = now();
