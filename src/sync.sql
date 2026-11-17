-- BSCAT 分类模型效果监控：同步 5 张看板底表 + 收入/负债流汇总最近 7 天的分区到 input/
-- 每条 SELECT 对应 input/ 下的一个 CSV，顺序必须与 sync.py 的 QUERY_SPECS 一致。
-- 表由上游 model.tmp_eliam_risk_000008_dash_* / ..._log_* 构建（按 sample_datetime 分区），
-- 这里只做同步，不做加工。后两张 log 表只进 zip 归档，页面不读。
-- 7 条查询共用同一个时间窗口：前 5 张看板表日期对不上时 monitor.py 会中止。

-- 块 1：汇总指标 → input/risk_000008_dash_summary.csv
SELECT * FROM model.tmp_eliam_risk_000008_dash_summary
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');

-- 块 2：逐日趋势 → input/risk_000008_dash_trend.csv
SELECT * FROM model.tmp_eliam_risk_000008_dash_trend
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');

-- 块 3：业务板块 → input/risk_000008_dash_group.csv
SELECT * FROM model.tmp_eliam_risk_000008_dash_group
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');

-- 块 4：分类别指标 → input/risk_000008_dash_category.csv
SELECT * FROM model.tmp_eliam_risk_000008_dash_category
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');

-- 块 5：逐笔明细 → input/risk_000008_dash_detail.csv
SELECT * FROM model.tmp_eliam_risk_000008_dash_detail
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');

-- 块 6：收入流汇总 → input/risk_000008_log_income_summary.csv（仅归档进 zip）
SELECT * FROM model.tmp_eliam_risk_000008_log_income_summary
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');

-- 块 7：负债流汇总 → input/risk_000008_log_liability_summary.csv（仅归档进 zip）
SELECT * FROM model.tmp_eliam_risk_000008_log_liability_summary
WHERE sample_datetime >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd');
