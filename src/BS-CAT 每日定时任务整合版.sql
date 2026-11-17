-- ============================================================================
-- BS-CAT 每日定时任务整合版
-- 执行顺序：先阶段一 log_*（按 inserttime 最近 7 天增量），再阶段二 Dashboard（分区窗口取自 log_run）
-- 使用前：先完成一次性分区迁移（migrate_once.sql），并确认 SET 与写入在同一会话执行
-- ============================================================================

-- ============================================================================
--  阶段一：BS-CAT 策略运行日志 -> log_* 结果表（按 sample_datetime 日增量）
--
--  源表  : ods.aus_cbd_gidp_core_gidp_strategy_run_log（直连，无需前置清洗）
--  产出  : model.tmp_eliam_risk_000008_log_transactions       24 列
--          model.tmp_eliam_risk_000008_log_income_summary     22 列
--          model.tmp_eliam_risk_000008_log_liability_summary  20 列
--          model.tmp_eliam_risk_000008_log_accounts            4 列（银行 / 卡类型）
--  中间  : log_src（视图）/ log_run（**勿删**，看板靠它识别本批分区）/ log_out
--  临时  : *_stage（每次 DROP 重建）
--
--  ⚠️ 增量语义：stage -> 首次建表（只借 schema）-> INSERT OVERWRITE 整分区替换，
--     只替换本批窗口的 sample_datetime 分区，历史保留，同一天重跑幂等。
--     窗口里某天这次没有成功记录时，该天旧分区不会被清空。
--  ⚠️ 依赖一次性分区迁移，未迁移会在第一条分区写入当场报 "table ... is not partitioned"。
--  ⚠️ 必须全选一次执行：SET DYNAMIC 不在同一会话时，INSERT OVERWRITE 会先清空所有分区。
--  ⚠️ 策略码 risk_000008 写死在表名和 STEP 0 两处，换策略需全局替换。
-- ============================================================================


-- ---------------------------------------------------------------------------
-- STEP A0  会话设置（必须先于所有写入，全选一次执行）
-- DYNAMIC：INSERT OVERWRITE 只替换 stage 中出现的分区，否则清空所有分区（丢历史）。
-- 工具若逐条语句各开新会话，这里会失效——第一次请先单独确认回显 DYNAMIC。
-- ---------------------------------------------------------------------------
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;
SET hive.exec.dynamic.partition.mode = nonstrict;



-- ---------------------------------------------------------------------------
-- STEP 0  源表视图：滚动读取最近 7 天（含 7 天前 00:00，不含今天 00:00）入库的日志，
-- 与 sync.sql 的 7 天窗口对齐，缺数会在之后 7 天内自愈。sample_datetime 取
-- inserttime 前 10 位（异常时回落 par_dt），因此按入库日期写入对应分区。
-- ⚠️ 源表若按 par_dt 分区，当前 inserttime 条件可能全表扫描——上线前检查执行计划。
-- 同一策略若有多个模型 / release 混跑，再补 release_id。
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS model.tmp_eliam_risk_000008_log_src;
CREATE VIEW model.tmp_eliam_risk_000008_log_src AS
SELECT * FROM ods.aus_cbd_gidp_core_gidp_strategy_run_log
WHERE strategy_code = 'RISK_000008'          -- 包装分类模型的策略编码
  AND run_status    = 1                      -- 1=成功（error_code=0000），2=失败（error_code=2002）
  AND isactive      = 1                      -- 过滤逻辑删除
  -- 滚动 7 天窗口：每次重算最近 7 天，分区幂等覆盖；与 sync.sql 的窗口一致
  AND inserttime >= concat(date_format(date_sub(current_date(), 7), 'yyyy-MM-dd'), ' 00:00:00')
  AND inserttime <  concat(date_format(current_date(), 'yyyy-MM-dd'), ' 00:00:00')
;
-- 离线 CSV 备选通路（连不上 ODS 时才用）见 11_optional_csv_path.sql。


-- ---------------------------------------------------------------------------
-- STEP 1  剥壳：取出成功运行，并把模型出参从响应信封里拎出来
-- 成功 = run_status '1' 且 data 非空、非 'null'；run_status 两条源通路类型不同，统一 CAST 再比。
-- strategy_content.midVarJson 是备用源，strategy_result 被截断时可改取。
-- log_run / log_out 每次重建、只装本批窗口；log_run 勿删（看板靠它的分区集合）。
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_log_run;
CREATE TABLE model.tmp_eliam_risk_000008_log_run AS
SELECT
  l.`_id`                                              AS log_id,
  l.serial_number,
  l.request_sn,
  l.strategy_code,
  l.par_dt,
  -- 产出表的日期键：日志 inserttime 的前 10 位（形如 '2026-09-22'）；
  -- inserttime 异常（空/格式不对）时回落到源表分区 par_dt
  CASE WHEN l.inserttime RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
       THEN SUBSTRING(l.inserttime, 1, 10) ELSE l.par_dt END AS sample_datetime,
  l.start_time,
  l.user_id                                            AS log_user_id,
  get_json_object(l.strategy_input, '$.bizParams.jobId')         AS job_id,
  get_json_object(l.strategy_input, '$.bizParams.applicationId') AS input_application_id,
  get_json_object(l.strategy_input, '$.bizParams.userId')        AS input_user_id,
  get_json_object(l.strategy_input, '$.bizParams.payloadId')     AS payload_id,
  get_json_object(l.strategy_result, '$.code')                   AS result_code,
  --  模型出参所在层级（已实测确认）
  get_json_object(l.strategy_result, '$.data')                   AS data_json
FROM model.tmp_eliam_risk_000008_log_src l
WHERE CAST(l.run_status AS STRING) = '1'
  AND get_json_object(l.strategy_result, '$.data') IS NOT NULL
  AND get_json_object(l.strategy_result, '$.data') <> 'null'
;

-- 展开出参内部的各段，顺手把回显标识提出来
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_log_out;
CREATE TABLE model.tmp_eliam_risk_000008_log_out AS
SELECT
  r.*,
  get_json_object(r.data_json, '$.userId')        AS echo_user_id,
  get_json_object(r.data_json, '$.applicationId') AS echo_application_id,
  get_json_object(r.data_json, '$.flowTime')      AS echo_flow_time,
  get_json_object(r.data_json, '$.flow_id')       AS echo_flow_id,
  get_json_object(r.data_json, '$.bscat_stats.product')              AS product,
  get_json_object(r.data_json, '$.bscat_stats.txn_raw_input_cnt')    AS txn_raw_input_cnt,
  get_json_object(r.data_json, '$.bscat_stats.transaction_date_max') AS transaction_date_max,
  get_json_object(r.data_json, '$.bscat_transactions')                     AS txn_json,
  get_json_object(r.data_json, '$.bscat_summaries.income_summary')         AS income_json,
  get_json_object(r.data_json, '$.bscat_summaries.liability_summary')      AS liability_json,
  get_json_object(r.data_json, '$.bank_accounts')                          AS accounts_json
FROM model.tmp_eliam_risk_000008_log_run r
;


-- ---------------------------------------------------------------------------
-- STEP 2  表一：逐笔交易（24 列）
-- 固定 schema 只解析 fundo 报文；混入 wagego（列集不固定）会静默吃掉上游列，
-- 届时需补 schema 或整行转 MAP 再按 key 取。sample_datetime 必须是 SELECT 最后一列。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_log_transactions_stage;
CREATE TABLE model.tmp_eliam_risk_000008_log_transactions_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
SELECT
  -- 身份：三个回显标识沿用入参驼峰口径，落表转 snake_case
  COALESCE(v.input_user_id, v.echo_user_id)                       AS user_id,
  COALESCE(v.input_application_id, v.echo_application_id)         AS application_id,
  v.echo_flow_time                                                AS flow_time,
  v.job_id,
  v.payload_id,
  v.log_id,
  v.serial_number,
  v.strategy_code,
  v.product,
  -- 逐笔分类结果
  t.bank_account_id,
  t.transaction_id,
  t.transaction_date,
  t.amount,
  t.balance,
  t.dr_cr,
  t.trx_type,
  t.text,
  t.illion_trx_uuid,
  t.category,
  t.third_party,
  t.counterparty,
  t.bscat,
  t.stream_id,
  v.sample_datetime
FROM model.tmp_eliam_risk_000008_log_out v
LATERAL VIEW explode(
  from_json(v.txn_json, '
    ARRAY<STRUCT<
      bank_account_id:   BIGINT,
      transaction_id:    BIGINT,
      transaction_date:  STRING,
      amount:            DOUBLE,
      balance:           DOUBLE,
      dr_cr:             STRING,
      trx_type:          STRING,
      text:              STRING,
      category:          STRING,
      third_party:       STRING,
      counterparty:      STRING,
      bscat:             STRING,
      stream_id:         STRING,
      illion_trx_uuid:   STRING
    >>')
) et AS t
WHERE v.product IS NULL OR v.product = 'fundo' OR v.product = ''
;

-- 首次切增量时建表（只借 schema），之后每次都是 no-op
CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_log_transactions
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_log_transactions_stage WHERE FALSE
;

-- 每日写入：显式 PARTITION (sample_datetime)，只替换本批窗口的分区，历史天不动。
-- 目标表若还是旧的非分区表（忘了迁移），这里会当场报 "table ... is not partitioned"。
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_log_transactions PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_log_transactions_stage
;


-- ---------------------------------------------------------------------------
-- STEP 3  表二：收入流汇总（22 列）——所属应用从顶层回显标识取
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_log_income_summary_stage;
CREATE TABLE model.tmp_eliam_risk_000008_log_income_summary_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
SELECT
  COALESCE(v.input_user_id, v.echo_user_id)               AS user_id,
  COALESCE(v.input_application_id, v.echo_application_id) AS application_id,
  COALESCE(v.echo_flow_time, v.echo_flow_id)              AS flow_time,
  v.job_id,
  v.log_id,
  v.serial_number,
  v.product,
  s.stream_id,
  s.income_category,
  s.transaction_start_date,
  s.transaction_end_date,
  s.status,
  s.transaction_count,
  s.total_income_amount,
  s.average_income_amount,
  s.median_income_amount,
  s.latest_income_amount,
  s.estimated_monthly_income,
  s.frequency,
  s.frequency_day,
  s.predicted_next_income_date,
  v.sample_datetime
FROM model.tmp_eliam_risk_000008_log_out v
LATERAL VIEW explode(
  from_json(v.income_json, '
    ARRAY<STRUCT<
      stream_id:                 STRING,
      income_category:           STRING,
      transaction_start_date:    STRING,
      transaction_end_date:      STRING,
      status:                    STRING,
      transaction_count:         BIGINT,
      total_income_amount:       DOUBLE,
      average_income_amount:     DOUBLE,
      median_income_amount:      DOUBLE,
      latest_income_amount:      DOUBLE,
      estimated_monthly_income:  DOUBLE,
      frequency:                 STRING,
      frequency_day:             STRING,
      predicted_next_income_date: STRING
    >>')
) es AS s
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_log_income_summary
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_log_income_summary_stage WHERE FALSE
;

INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_log_income_summary PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_log_income_summary_stage
;


-- ---------------------------------------------------------------------------
-- STEP 4  表三：负债流汇总（20 列）
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_log_liability_summary_stage;
CREATE TABLE model.tmp_eliam_risk_000008_log_liability_summary_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
SELECT
  COALESCE(v.input_user_id, v.echo_user_id)               AS user_id,
  COALESCE(v.input_application_id, v.echo_application_id) AS application_id,
  COALESCE(v.echo_flow_time, v.echo_flow_id)              AS flow_time,
  v.job_id,
  v.log_id,
  v.serial_number,
  v.product,
  s.stream_id,
  s.liability_category,
  s.transaction_start_date,
  s.transaction_end_date,
  s.status,
  s.funded_amount,
  s.repaid_amount,
  s.repayment_amount,
  s.recent_fn_repay_amount,
  s.frequency,
  s.frequency_day,
  s.predicted_closing_date,
  v.sample_datetime
FROM model.tmp_eliam_risk_000008_log_out v
LATERAL VIEW explode(
  from_json(v.liability_json, '
    ARRAY<STRUCT<
      stream_id:                STRING,
      liability_category:       STRING,
      transaction_start_date:   STRING,
      transaction_end_date:     STRING,
      status:                   STRING,
      funded_amount:            DOUBLE,
      repaid_amount:            DOUBLE,
      repayment_amount:         DOUBLE,
      recent_fn_repay_amount:   DOUBLE,
      frequency:                STRING,
      frequency_day:            STRING,
      predicted_closing_date:   STRING
    >>')
) es AS s
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_log_liability_summary
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_log_liability_summary_stage WHERE FALSE
;

INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_log_liability_summary PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_log_liability_summary_stage
;


-- ---------------------------------------------------------------------------
-- STEP 5  表四：账户元数据（4 列）：按 (sample_datetime, bank_account_id) 去重，供 dash_detail 关联
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_log_accounts_stage;
CREATE TABLE model.tmp_eliam_risk_000008_log_accounts_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
SELECT
  a.bank_account_id,
  MAX(a.bank)         AS bank,
  MAX(a.account_type) AS account_type,
  v.sample_datetime
FROM model.tmp_eliam_risk_000008_log_out v
LATERAL VIEW explode(
  from_json(v.accounts_json, '
    ARRAY<STRUCT<
      bank_account_id: BIGINT,
      bank:            STRING,
      account_type:    STRING
    >>')
) et AS a
WHERE a.bank_account_id IS NOT NULL
GROUP BY v.sample_datetime, a.bank_account_id
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_log_accounts
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_log_accounts_stage WHERE FALSE
;

INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_log_accounts PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_log_accounts_stage
;


-- ============================================================================
-- 阶段二：Dashboard 监控数据刷新（必须在阶段一成功后执行，分区窗口取自 log_run）
-- ============================================================================

-- ============================================================================
--  Dashboard 差异看板（7 张 dash 表，按 sample_datetime 日增量）
--
--  上游  : model.tmp_eliam_risk_000008_log_transactions（逐日累计）
--  产出  : dash_base 口径基座 / dash_summary 每天 15 行 / dash_category 每 Category 一行 33 列 /
--          dash_flow 每流向一行 14 列 / dash_group 每天 6 行 20 列 /
--          dash_detail 每差异笔一行 24 列 / dash_trend 每天一行 12 列
--  临时  : *_stage（每次 DROP 重建）
--
--  ⚠️ 本阶段不设窗口：刷新哪些分区由 log_run 的 sample_datetime 决定，必须先跑阶段一。
--  ⚠️ 依赖一次性分区迁移，未迁移会在显式分区写入时当场报 "table ... is not partitioned"。
--  ⚠️ 必须全选一次执行：SET DYNAMIC 不在同一会话时，INSERT OVERWRITE 会先清空所有分区。
--  ⚠️ 产出列名不能含空格、逗号、分号等字符（Hive 拿列名做目录名，报
--     INVALID_COLUMN_NAME_AS_PATH，遇到第一个就抛，不代表只有一列有问题）。汉字合法。
--  ⚠️ 字符串和注释里都不要写 ASCII 分号：客户端按朴素 split 切语句，会从中间截断。
--     拼接分隔符一律写 CONCAT(CHR(59), ' ')。
--  ⚠️ 并列项用 row_ord 做 tie-break，只影响「流向 / 交易方向 / 跨组流向」的并列顺序。
--  ⚠️ 策略码写死在表名里，换策略全局替换。
--
--  口径来源 label_compare.py 的 compute_*：5 张有 Python 对标的表已逐行逐列对拍一致。
--  与 Python 版的有意差异：dash_detail 缺线上日志没有的列（另补银行名称 / 卡类型 2 列）、
--  dash_trend 为线上新增、列名里的空格改成下划线、各表按天分组。
-- ============================================================================


-- ---------------------------------------------------------------------------
-- STEP A0  会话设置（必须先于所有写入，全选一次执行）
-- DYNAMIC：INSERT OVERWRITE 只替换 stage 中出现的分区，否则清空所有分区（丢历史）。
-- 逐条语句各开新会话的工具会让这里失效，第一次请先单独确认回显 DYNAMIC。
-- ---------------------------------------------------------------------------
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;
SET hive.exec.dynamic.partition.mode = nonstrict;



-- ---------------------------------------------------------------------------
-- STEP D0  口径基座（增量：只替换本批窗口的分区）
--
-- 把逐笔交易加工成所有看板共用的形态，对应 label_compare.py 的
-- prepare_comparison_data + assign_group_columns：
--   ref = illion 侧，取 transactions.category；cand = 模型侧，取 transactions.bscat
-- 三段式 row_ord -> clean -> normalize，每层只写一次表达式：
--   clean(x)  = 折叠空白 + 去首尾 + 空值词表判空（保留原大小写）
--   normalize = 小写 -> '&' 变 ' and ' -> [-_/] 变空格 -> 非字母数字变空格 -> 折叠空白
--               SQL 用 [^\p{L}\p{N}_\s] 对应 Python 的 \w（UNICODE），本批 Category
--               全 ASCII，两者等价。
--
-- row_ord / 展示名 / 大类都在全表（含历史）上算，保证跨天一致；只有写入按 log_run
-- 的 sample_datetime 集合替换本批分区。
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_base_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_base_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
WITH ranked AS (
  SELECT
    t.*,
    -- tie-break 用的行序，见文件头说明（全表统一编号，跨天可比）
    ROW_NUMBER() OVER (ORDER BY t.log_id, t.transaction_id) AS row_ord,
    ABS(CAST(t.amount AS DOUBLE))                           AS abs_amount
  FROM model.tmp_eliam_risk_000008_log_transactions t
),
cleaned AS (
  SELECT
    r.*,
    CASE
      WHEN r.category IS NULL THEN NULL
      WHEN LOWER(TRIM(REGEXP_REPLACE(r.category, '\\s+', ' ')))
           IN ('', 'nan', 'none', 'null', '<na>', 'n/a', 'na', 'nat', 'nil') THEN NULL
      ELSE TRIM(REGEXP_REPLACE(r.category, '\\s+', ' '))
    END AS ref_clean,
    CASE
      WHEN r.bscat IS NULL THEN NULL
      WHEN LOWER(TRIM(REGEXP_REPLACE(r.bscat, '\\s+', ' ')))
           IN ('', 'nan', 'none', 'null', '<na>', 'n/a', 'na', 'nat', 'nil') THEN NULL
      ELSE TRIM(REGEXP_REPLACE(r.bscat, '\\s+', ' '))
    END AS cand_clean
  FROM ranked r
),
keyed AS (
  SELECT
    c.*,
    NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
      REPLACE(LOWER(c.ref_clean), '&', ' and '),
      '[-_/]+', ' '), '[^\\p{L}\\p{N}_\\s]', ' '), '\\s+', ' ')), '') AS ref_key,
    NULLIF(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
      REPLACE(LOWER(c.cand_clean), '&', ' and '),
      '[-_/]+', ' '), '[^\\p{L}\\p{N}_\\s]', ' '), '\\s+', ' ')), '') AS cand_key
  FROM cleaned c
),
-- 展示名：每个 key 取出现最多的 clean 值，并列取字典序最小
-- ⚠️ 必须分两趟：ref 侧先建，cand 侧只补 ref 侧没有的 key（合并统计会得到不同众数）
--    与 label_compare.build_display_map 的循环顺序一致。
disp_ref AS (
  SELECT ref_key AS k, ref_clean AS disp, COUNT(*) AS cnt
  FROM keyed WHERE ref_key IS NOT NULL
  GROUP BY ref_key, ref_clean
),
disp_cand AS (
  SELECT cand_key AS k, cand_clean AS disp, COUNT(*) AS cnt
  FROM keyed
  WHERE cand_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM disp_ref d WHERE d.k = keyed.cand_key)
  GROUP BY cand_key, cand_clean
),
disp AS (
  SELECT k, disp FROM (
    SELECT k, disp, ROW_NUMBER() OVER (PARTITION BY k ORDER BY cnt DESC, disp ASC) AS rn
    FROM (SELECT * FROM disp_ref UNION ALL SELECT * FROM disp_cand) u
  ) v WHERE rn = 1
)
SELECT
  kd.row_ord,
  kd.user_id, kd.application_id, kd.job_id, kd.log_id, kd.transaction_id,
  kd.bank_account_id, kd.transaction_date, kd.amount, kd.abs_amount,
  kd.balance, kd.dr_cr, kd.text, kd.third_party, kd.counterparty,
  kd.category, kd.bscat,
  kd.ref_clean, kd.cand_clean, kd.ref_key, kd.cand_key,

  COALESCE(dr.disp, kd.ref_key)  AS ref_display,
  COALESCE(dc.disp, kd.cand_key) AS cand_display,
  COALESCE(dr.disp, kd.ref_key,  '(空)') AS ref_matrix,
  COALESCE(dc.disp, kd.cand_key, '(空)') AS cand_matrix,

  -- 对应 label_compare.prepare_comparison_data 里的 np.select，顺序即优先级
  CASE
    WHEN kd.ref_key IS NULL AND kd.cand_key IS NULL THEN 'both_empty'
    WHEN kd.ref_key IS NULL                         THEN 'candidate_only'
    WHEN kd.cand_key IS NULL                        THEN 'reference_only'
    WHEN kd.ref_clean = kd.cand_clean               THEN 'exact_match'
    WHEN kd.ref_key = kd.cand_key                   THEN 'normalized_match'
    ELSE 'mismatch'
  END AS match_status,

  -- 业务大类聚类：关键词子串匹配，按下面 CASE 的先后判定（= dict 插入序）
  CASE
    WHEN kd.ref_key IS NULL THEN '未分类'
    WHEN INSTR(kd.ref_key, 'loan') > 0 OR INSTR(kd.ref_key, 'credit card') > 0
      OR INSTR(kd.ref_key, 'repayment') > 0 OR INSTR(kd.ref_key, 'sacc') > 0
      OR INSTR(kd.ref_key, 'overdrawn') > 0 OR INSTR(kd.ref_key, 'debt') > 0
      OR INSTR(kd.ref_key, 'loc') > 0 THEN '贷款类'
    WHEN INSTR(kd.ref_key, 'transfer') > 0 OR INSTR(kd.ref_key, 'osko') > 0
      OR INSTR(kd.ref_key, 'bpay') > 0 THEN '转账类'
    WHEN INSTR(kd.ref_key, 'wage') > 0 OR INSTR(kd.ref_key, 'salary') > 0
      OR INSTR(kd.ref_key, 'centrelink') > 0 OR INSTR(kd.ref_key, 'income') > 0
      OR INSTR(kd.ref_key, 'all other credit') > 0 OR INSTR(kd.ref_key, 'refund') > 0
      OR INSTR(kd.ref_key, 'deposit') > 0 THEN '收入类'
    WHEN INSTR(kd.ref_key, 'dining') > 0 OR INSTR(kd.ref_key, 'grocer') > 0
      OR INSTR(kd.ref_key, 'gambling') > 0 OR INSTR(kd.ref_key, 'automotive') > 0
      OR INSTR(kd.ref_key, 'transport') > 0 OR INSTR(kd.ref_key, 'fee') > 0
      OR INSTR(kd.ref_key, 'retail') > 0 OR INSTR(kd.ref_key, 'subscription') > 0
      OR INSTR(kd.ref_key, 'gym') > 0 OR INSTR(kd.ref_key, 'department') > 0
      OR INSTR(kd.ref_key, 'travel') > 0 OR INSTR(kd.ref_key, 'rent') > 0
      OR INSTR(kd.ref_key, 'telecom') > 0 OR INSTR(kd.ref_key, 'health') > 0
      OR INSTR(kd.ref_key, 'util') > 0 OR INSTR(kd.ref_key, 'entertainment') > 0
      OR INSTR(kd.ref_key, 'insurance') > 0 OR INSTR(kd.ref_key, 'personal care') > 0
      OR INSTR(kd.ref_key, 'home improvement') > 0 OR INSTR(kd.ref_key, 'education') > 0
      OR INSTR(kd.ref_key, 'pet') > 0 OR INSTR(kd.ref_key, 'information') > 0
      OR INSTR(kd.ref_key, 'donation') > 0 OR INSTR(kd.ref_key, 'dishonour') > 0
      THEN '支出类'
    ELSE '未分类'
  END AS ref_group,
  CASE
    WHEN kd.cand_key IS NULL THEN '未分类'
    WHEN INSTR(kd.cand_key, 'loan') > 0 OR INSTR(kd.cand_key, 'credit card') > 0
      OR INSTR(kd.cand_key, 'repayment') > 0 OR INSTR(kd.cand_key, 'sacc') > 0
      OR INSTR(kd.cand_key, 'overdrawn') > 0 OR INSTR(kd.cand_key, 'debt') > 0
      OR INSTR(kd.cand_key, 'loc') > 0 THEN '贷款类'
    WHEN INSTR(kd.cand_key, 'transfer') > 0 OR INSTR(kd.cand_key, 'osko') > 0
      OR INSTR(kd.cand_key, 'bpay') > 0 THEN '转账类'
    WHEN INSTR(kd.cand_key, 'wage') > 0 OR INSTR(kd.cand_key, 'salary') > 0
      OR INSTR(kd.cand_key, 'centrelink') > 0 OR INSTR(kd.cand_key, 'income') > 0
      OR INSTR(kd.cand_key, 'all other credit') > 0 OR INSTR(kd.cand_key, 'refund') > 0
      OR INSTR(kd.cand_key, 'deposit') > 0 THEN '收入类'
    WHEN INSTR(kd.cand_key, 'dining') > 0 OR INSTR(kd.cand_key, 'grocer') > 0
      OR INSTR(kd.cand_key, 'gambling') > 0 OR INSTR(kd.cand_key, 'automotive') > 0
      OR INSTR(kd.cand_key, 'transport') > 0 OR INSTR(kd.cand_key, 'fee') > 0
      OR INSTR(kd.cand_key, 'retail') > 0 OR INSTR(kd.cand_key, 'subscription') > 0
      OR INSTR(kd.cand_key, 'gym') > 0 OR INSTR(kd.cand_key, 'department') > 0
      OR INSTR(kd.cand_key, 'travel') > 0 OR INSTR(kd.cand_key, 'rent') > 0
      OR INSTR(kd.cand_key, 'telecom') > 0 OR INSTR(kd.cand_key, 'health') > 0
      OR INSTR(kd.cand_key, 'util') > 0 OR INSTR(kd.cand_key, 'entertainment') > 0
      OR INSTR(kd.cand_key, 'insurance') > 0 OR INSTR(kd.cand_key, 'personal care') > 0
      OR INSTR(kd.cand_key, 'home improvement') > 0 OR INSTR(kd.cand_key, 'education') > 0
      OR INSTR(kd.cand_key, 'pet') > 0 OR INSTR(kd.cand_key, 'information') > 0
      OR INSTR(kd.cand_key, 'donation') > 0 OR INSTR(kd.cand_key, 'dishonour') > 0
      THEN '支出类'
    ELSE '未分类'
  END AS cand_group,

  -- 关键 Category（关键词子串匹配）
  CASE WHEN kd.ref_key IS NOT NULL AND (
      INSTR(kd.ref_key, 'liability') > 0 OR INSTR(kd.ref_key, 'loan') > 0
      OR INSTR(kd.ref_key, 'credit card') > 0 OR INSTR(kd.ref_key, 'repayment') > 0
      OR INSTR(kd.ref_key, 'income') > 0 OR INSTR(kd.ref_key, 'wage') > 0
      OR INSTR(kd.ref_key, 'salary') > 0 OR INSTR(kd.ref_key, 'rent') > 0
      OR INSTR(kd.ref_key, 'gambling') > 0 OR INSTR(kd.ref_key, 'utilities') > 0
      OR INSTR(kd.ref_key, 'financial institution') > 0
      OR INSTR(kd.ref_key, 'financial service') > 0)
    THEN '是' ELSE '否' END AS ref_is_key,
  CASE WHEN kd.cand_key IS NOT NULL AND (
      INSTR(kd.cand_key, 'liability') > 0 OR INSTR(kd.cand_key, 'loan') > 0
      OR INSTR(kd.cand_key, 'credit card') > 0 OR INSTR(kd.cand_key, 'repayment') > 0
      OR INSTR(kd.cand_key, 'income') > 0 OR INSTR(kd.cand_key, 'wage') > 0
      OR INSTR(kd.cand_key, 'salary') > 0 OR INSTR(kd.cand_key, 'rent') > 0
      OR INSTR(kd.cand_key, 'gambling') > 0 OR INSTR(kd.cand_key, 'utilities') > 0
      OR INSTR(kd.cand_key, 'financial institution') > 0
      OR INSTR(kd.cand_key, 'financial service') > 0)
    THEN '是' ELSE '否' END AS cand_is_key,
  kd.sample_datetime

FROM keyed kd
LEFT JOIN disp dr ON dr.k = kd.ref_key
LEFT JOIN disp dc ON dc.k = kd.cand_key
WHERE kd.sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
;

-- 首次切增量时建表（只借 schema），之后每次都是 no-op
CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_base
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_base_stage WHERE FALSE
;

-- 每日写入：显式 PARTITION (sample_datetime)，只替换本批窗口的分区，历史天不动。
-- 目标表若还是旧的非分区表（忘了迁移），这里会当场报 "table ... is not partitioned"。
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_base PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_base_stage
;


-- ---------------------------------------------------------------------------
-- STEP D1  dash_summary  核心指标（每天 15 行，按 sample_datetime 分区增量）
--   对应 label_compare.compute_summary_v2：原口径「一批算一组 15 个指标」，现在按天各算一组。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_summary_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_summary_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
WITH m AS (
  SELECT
    sample_datetime,
    COUNT(*)                                                                   AS n,
    SUM(CASE WHEN ref_key  IS NOT NULL THEN 1 ELSE 0 END)                      AS ref_n,
    SUM(CASE WHEN cand_key IS NOT NULL THEN 1 ELSE 0 END)                      AS cand_n,
    SUM(CASE WHEN ref_key IS NOT NULL AND cand_key IS NOT NULL THEN 1 ELSE 0 END) AS both_n,
    SUM(CASE WHEN ref_key IS NOT NULL OR  cand_key IS NOT NULL THEN 1 ELSE 0 END) AS union_n,
    SUM(CASE WHEN match_status IN ('exact_match','normalized_match') THEN 1 ELSE 0 END) AS matched_n,
    SUM(CASE WHEN match_status = 'mismatch'       THEN 1 ELSE 0 END)           AS mismatch_n,
    SUM(CASE WHEN match_status = 'reference_only' THEN 1 ELSE 0 END)           AS ref_only_n,
    SUM(CASE WHEN match_status = 'candidate_only' THEN 1 ELSE 0 END)           AS cand_only_n,
    SUM(CASE WHEN match_status = 'both_empty'     THEN 1 ELSE 0 END)           AS both_empty_n,
    SUM(CASE WHEN match_status IN ('mismatch','reference_only','candidate_only')
             THEN 1 ELSE 0 END)                                                AS diff_n
  FROM model.tmp_eliam_risk_000008_dash_base
  WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
  GROUP BY sample_datetime
)
SELECT
  t.ord, t.`指标`, t.`结果`, t.`分子`, t.`分母`, t.`说明`, t.`格式`, t.sample_datetime
FROM (
  SELECT 1 AS ord, '总交易数' AS `指标`, CAST(n AS DOUBLE) AS `结果`,
         CAST(n AS BIGINT) AS `分子`, CAST(NULL AS BIGINT) AS `分母`,
         '输入数据总行数' AS `说明`, 'count' AS `格式`, sample_datetime FROM m
  UNION ALL SELECT 2, 'illion Category 覆盖率', CAST(CASE WHEN n=0 THEN 0.0 ELSE ref_n/n END AS DOUBLE), CAST(ref_n AS BIGINT), CAST(n AS BIGINT), 'category 标准化后非空', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 3, 'finv Category 覆盖率',  CAST(CASE WHEN n=0 THEN 0.0 ELSE cand_n/n END AS DOUBLE), CAST(cand_n AS BIGINT), CAST(n AS BIGINT), 'bscat 标准化后非空', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 4, '双方非空时一致率', CAST(CASE WHEN both_n=0 THEN 0.0 ELSE matched_n/both_n END AS DOUBLE), CAST(matched_n AS BIGINT), CAST(both_n AS BIGINT), '含原始值一致和标准化后一致', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 5, '双方非空时差异率', CAST(CASE WHEN both_n=0 THEN 0.0 ELSE mismatch_n/both_n END AS DOUBLE), CAST(mismatch_n AS BIGINT), CAST(both_n AS BIGINT), '双方均有 Category，但 Category 不同', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 6, '覆盖调整后一致率', CAST(CASE WHEN union_n=0 THEN 0.0 ELSE matched_n/union_n END AS DOUBLE), CAST(matched_n AS BIGINT), CAST(union_n AS BIGINT), '将单边缺失也计入差异', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 7, '联合非空样本差异率', CAST(CASE WHEN union_n=0 THEN 0.0 ELSE diff_n/union_n END AS DOUBLE), CAST(diff_n AS BIGINT), CAST(union_n AS BIGINT), '分类不一致 + 两侧单边缺失，分母为至少一侧有分类的交易', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 8, '覆盖重叠率', CAST(CASE WHEN union_n=0 THEN 0.0 ELSE both_n/union_n END AS DOUBLE), CAST(both_n AS BIGINT), CAST(union_n AS BIGINT), '双方均有分类 / 至少一侧有分类；只衡量覆盖是否重叠', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 9, '覆盖不重叠率', CAST(CASE WHEN union_n=0 THEN 0.0 ELSE (ref_only_n+cand_only_n)/union_n END AS DOUBLE), CAST(ref_only_n+cand_only_n AS BIGINT), CAST(union_n AS BIGINT), '仅一侧有分类 / 至少一侧有分类；覆盖重叠率的补集', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 10, '相对illion的方向性差异率', CAST(CASE WHEN ref_n=0 THEN 0.0 ELSE (mismatch_n+ref_only_n)/ref_n END AS DOUBLE), CAST(mismatch_n+ref_only_n AS BIGINT), CAST(ref_n AS BIGINT), '双方不一致 + 仅illion有值，分母为illion有分类数量；不代表准确率', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 11, '相对finv的方向性差异率', CAST(CASE WHEN cand_n=0 THEN 0.0 ELSE (mismatch_n+cand_only_n)/cand_n END AS DOUBLE), CAST(mismatch_n+cand_only_n AS BIGINT), CAST(cand_n AS BIGINT), '双方不一致 + 仅finv有值，分母为finv有分类数量；不代表准确率', 'percentage', sample_datetime FROM m
  UNION ALL SELECT 12, 'Category 差异总数', CAST(diff_n AS DOUBLE), CAST(diff_n AS BIGINT), CAST(union_n AS BIGINT), '分类不一致 + 仅 illion 有值 + 仅 finv 有值', 'count', sample_datetime FROM m
  UNION ALL SELECT 13, '仅 illion 有 Category', CAST(ref_only_n AS DOUBLE), CAST(ref_only_n AS BIGINT), CAST(ref_n AS BIGINT), 'finv 侧为空', 'count', sample_datetime FROM m
  UNION ALL SELECT 14, '仅 finv 有 Category', CAST(cand_only_n AS DOUBLE), CAST(cand_only_n AS BIGINT), CAST(cand_n AS BIGINT), 'illion 侧为空', 'count', sample_datetime FROM m
  UNION ALL SELECT 15, '双方均为空', CAST(both_empty_n AS DOUBLE), CAST(both_empty_n AS BIGINT), CAST(n AS BIGINT), '不进入热力图和差异明细', 'count', sample_datetime FROM m
) t ORDER BY t.sample_datetime, t.ord
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_summary
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_summary_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口的分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_summary PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_summary_stage
;


-- ---------------------------------------------------------------------------
-- STEP D2  dash_category  逐 Category 差异（32 列 + sample_datetime = 33 列）
--   对应 label_compare.compute_category_comparison_v2
--   行 = (sample_datetime, Category)，按天各算一份、按 sample_datetime 分区增量。
--   2026-11-13 起不再输出「建议优先级」：P 档由看板按业务板块定（monitor.py 的
--   LEVEL_BY_SEGMENT），上游按分位数切 P1/P2/P3 的判定和切点计算一并删掉。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_category_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_category_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
WITH src AS (
  SELECT * FROM model.tmp_eliam_risk_000008_dash_base
  WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
),
keys AS (
  SELECT DISTINCT sample_datetime, ref_key AS k  FROM src WHERE ref_key  IS NOT NULL
  UNION
  SELECT DISTINCT sample_datetime, cand_key     FROM src WHERE cand_key IS NOT NULL
),
-- ref 侧聚合（只统计 ref_key 非空的行，按天各一份）
ra AS (
  SELECT
    sample_datetime,
    ref_key AS k,
    COUNT(*) AS ref_cnt,
    SUM(CASE WHEN cand_key = ref_key THEN 1 ELSE 0 END)                              AS matched,
    SUM(CASE WHEN cand_key IS NOT NULL AND cand_key <> ref_key THEN 1 ELSE 0 END)    AS mismatch_out,
    SUM(CASE WHEN cand_key IS NULL THEN 1 ELSE 0 END)                                AS cand_missing,
    SIZE(COLLECT_SET(user_id))        AS ref_users,
    SIZE(COLLECT_SET(application_id)) AS ref_apps,
    SUM(abs_amount)                   AS ref_amt
  FROM src
  WHERE ref_key IS NOT NULL
  GROUP BY sample_datetime, ref_key
),
-- cand 侧聚合（按天各一份）
ca AS (
  SELECT
    sample_datetime,
    cand_key AS k,
    COUNT(*) AS cand_cnt,
    SUM(CASE WHEN ref_key IS NOT NULL AND ref_key <> cand_key THEN 1 ELSE 0 END)     AS mismatch_in,
    SUM(CASE WHEN ref_key IS NULL THEN 1 ELSE 0 END)                                 AS ref_missing,
    SUM(abs_amount) AS cand_amt
  FROM src
  WHERE cand_key IS NOT NULL
  GROUP BY sample_datetime, cand_key
),
-- ref 侧差异行（cand 分到别的 Category 或 cand 为空，按天各一份）
da AS (
  SELECT
    sample_datetime,
    ref_key AS k,
    COUNT(*) AS ref_diff_cnt,
    SIZE(COLLECT_SET(user_id))        AS diff_users,
    SIZE(COLLECT_SET(application_id)) AS diff_apps,
    SUM(abs_amount)                   AS diff_amt
  FROM src
  WHERE ref_key IS NOT NULL AND (cand_key IS NULL OR cand_key <> ref_key)
  GROUP BY sample_datetime, ref_key
),
-- 「主要流出去向」前三名：ref 侧流出到哪些 cand Category
-- ⚠️ 先 GROUP BY 出计数，再在纯列上开窗。窗口 ORDER BY 里直接写 COUNT(*)/MIN(row_ord)
--    会在聚合结果上再套聚合，Spark 报 "aggregate function in the argument of another aggregate"。
of_cnt AS (
  SELECT sample_datetime, ref_key AS k, cand_display AS disp,
         COUNT(*) AS cnt, MIN(row_ord) AS min_ord
  FROM src
  WHERE ref_key IS NOT NULL AND cand_key IS NOT NULL AND cand_key <> ref_key
  GROUP BY sample_datetime, ref_key, cand_display
),
of_rn AS (
  SELECT sample_datetime, k, rn, CONCAT(disp, ': ', FORMAT_NUMBER(cnt, 0)) AS txt
  FROM (
    SELECT sample_datetime, k, disp, cnt,
           ROW_NUMBER() OVER (PARTITION BY sample_datetime, k ORDER BY cnt DESC, min_ord ASC) AS rn
    FROM of_cnt
  ) x
),
of_a AS (
  SELECT sample_datetime, k,
    CONCAT_WS(CONCAT(CHR(59), ' '),
      MAX(CASE WHEN rn = 1 THEN txt END),
      MAX(CASE WHEN rn = 2 THEN txt END),
      MAX(CASE WHEN rn = 3 THEN txt END)) AS s
  FROM of_rn WHERE rn <= 3
  GROUP BY sample_datetime, k
),
-- 「主要流入来源」前三名
if_cnt AS (
  SELECT sample_datetime, cand_key AS k, ref_display AS disp,
         COUNT(*) AS cnt, MIN(row_ord) AS min_ord
  FROM src
  WHERE cand_key IS NOT NULL AND ref_key IS NOT NULL AND ref_key <> cand_key
  GROUP BY sample_datetime, cand_key, ref_display
),
if_rn AS (
  SELECT sample_datetime, k, rn, CONCAT(disp, ': ', FORMAT_NUMBER(cnt, 0)) AS txt
  FROM (
    SELECT sample_datetime, k, disp, cnt,
           ROW_NUMBER() OVER (PARTITION BY sample_datetime, k ORDER BY cnt DESC, min_ord ASC) AS rn
    FROM if_cnt
  ) x
),
if_a AS (
  SELECT sample_datetime, k,
    CONCAT_WS(CONCAT(CHR(59), ' '),
      MAX(CASE WHEN rn = 1 THEN txt END),
      MAX(CASE WHEN rn = 2 THEN txt END),
      MAX(CASE WHEN rn = 3 THEN txt END)) AS s
  FROM if_rn WHERE rn <= 3
  GROUP BY sample_datetime, k
),
-- 每个 key 的展示名（按天各一份）：ref 侧优先，cand 侧只补 ref 侧没有的 key
-- dash_base 里同一 key 的 ref_display / cand_display 每行都相同，MAX 只是取那一个值
dr_k AS (
  SELECT sample_datetime, ref_key AS k, MAX(ref_display) AS disp
  FROM src WHERE ref_key IS NOT NULL GROUP BY sample_datetime, ref_key
),
dc_k AS (
  SELECT sample_datetime, cand_key AS k, MAX(cand_display) AS disp
  FROM src WHERE cand_key IS NOT NULL GROUP BY sample_datetime, cand_key
),
-- 关键 Category 是 ref_key 的纯函数，取值恒定
kf AS (
  SELECT sample_datetime, ref_key AS k, MAX(ref_is_key) AS is_key
  FROM src WHERE ref_key IS NOT NULL GROUP BY sample_datetime, ref_key
),
tot AS (SELECT sample_datetime, COUNT(*) AS n FROM src GROUP BY sample_datetime),
-- 组装：先算未排序的明细（按天）
raw AS (
  SELECT
    ks.sample_datetime,
    ks.k                                   AS category_key,
    COALESCE(dr_k.disp, dc_k.disp, ks.k)   AS `Category`,
    CASE WHEN kf.is_key = '是' THEN '是' ELSE '否' END AS `关键Category`,
    COALESCE(ra.ref_cnt, 0)                AS ref_cnt,
    COALESCE(ca.cand_cnt, 0)               AS cand_cnt,
    COALESCE(ra.matched, 0)                AS matched,
    COALESCE(ra.mismatch_out, 0)           AS mismatch_out,
    COALESCE(ra.cand_missing, 0)           AS cand_missing,
    COALESCE(ca.mismatch_in, 0)            AS mismatch_in,
    COALESCE(ca.ref_missing, 0)            AS ref_missing,
    COALESCE(da.ref_diff_cnt, 0)           AS ref_diff_cnt,
    COALESCE(da.diff_users, 0)             AS diff_users,
    COALESCE(da.diff_apps, 0)              AS diff_apps,
    ra.ref_users, ra.ref_apps,
    ra.ref_amt, ca.cand_amt,
    -- 无差异行 -> 0.0；有差异行但金额全为空 -> SUM 得 NULL，也归 0.0
    CASE WHEN da.ref_diff_cnt IS NULL THEN 0.0
         ELSE COALESCE(da.diff_amt, 0.0) END AS diff_amt,
    CAST(tot.n AS BIGINT)                  AS total_n,
    COALESCE(of_a.s, '-')                  AS out_text,
    COALESCE(if_a.s, '-')                  AS in_text
  FROM keys ks
  LEFT JOIN dr_k  ON dr_k.sample_datetime  = ks.sample_datetime AND dr_k.k  = ks.k
  LEFT JOIN dc_k  ON dc_k.sample_datetime  = ks.sample_datetime AND dc_k.k  = ks.k
  LEFT JOIN kf    ON kf.sample_datetime    = ks.sample_datetime AND kf.k    = ks.k
  LEFT JOIN ra    ON ra.sample_datetime    = ks.sample_datetime AND ra.k    = ks.k
  LEFT JOIN ca    ON ca.sample_datetime    = ks.sample_datetime AND ca.k    = ks.k
  LEFT JOIN da    ON da.sample_datetime    = ks.sample_datetime AND da.k    = ks.k
  LEFT JOIN of_a  ON of_a.sample_datetime  = ks.sample_datetime AND of_a.k  = ks.k
  LEFT JOIN if_a  ON if_a.sample_datetime  = ks.sample_datetime AND if_a.k  = ks.k
  JOIN tot        ON tot.sample_datetime   = ks.sample_datetime
),
-- 把总差异数挂到每一行（按天）
sorted AS (
  SELECT r.*,
    SUM(r.ref_diff_cnt) OVER (PARTITION BY r.sample_datetime) AS total_ref_diff
  FROM raw r
),
calc AS (
  SELECT s.*,
    CAST(s.ref_cnt + s.cand_cnt - s.matched AS BIGINT)          AS union_cnt,
    CAST(s.cand_cnt - s.matched AS BIGINT)                      AS cand_only_cnt,
    CAST(s.ref_diff_cnt + s.mismatch_in + s.ref_missing AS BIGINT) AS bidirectional,
    CASE WHEN s.ref_cnt = 0 THEN 0.0 ELSE CAST(s.ref_diff_cnt AS DOUBLE)/s.ref_cnt END AS ref_diff_rate
  FROM sorted s
),
derived AS (
  SELECT c.*,
    CASE WHEN c.union_cnt = 0 THEN 0.0
         ELSE CAST(c.matched AS DOUBLE)/c.union_cnt END          AS inter_share,
    CASE WHEN c.union_cnt = 0 THEN 0.0
         ELSE CAST(c.ref_diff_cnt AS DOUBLE)/c.union_cnt END     AS ref_only_share,
    CASE WHEN c.union_cnt = 0 THEN 0.0
         ELSE CAST(c.cand_only_cnt AS DOUBLE)/c.union_cnt END    AS cand_only_share,
    CASE WHEN c.total_ref_diff = 0 THEN 0.0
         ELSE CAST(c.ref_diff_cnt AS DOUBLE)/c.total_ref_diff END AS contrib
  FROM calc c
),
-- 排序：交集占比（并集）升序 -> 并集数量降序 -> Category 升序
-- 累计贡献率必须在排序之后再算，所以序号和 cumsum 放在同一个窗口定义里
ranked AS (
  SELECT d.*,
    ROW_NUMBER() OVER (PARTITION BY d.sample_datetime
                       ORDER BY d.inter_share ASC, d.union_cnt DESC, d.`Category` ASC,
                                d.category_key ASC) AS seq,
    SUM(d.contrib) OVER (PARTITION BY d.sample_datetime
                         ORDER BY d.inter_share ASC, d.union_cnt DESC, d.`Category` ASC,
                                  d.category_key ASC
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_contrib
  FROM derived d
)
SELECT
  `Category`,
  `关键Category`,
  CAST(ref_cnt AS BIGINT)  AS `illion数量`,
  CAST(cand_cnt AS BIGINT) AS `finv数量`,
  CASE WHEN total_n = 0 THEN 0.0
       ELSE CAST(ref_cnt AS DOUBLE)/total_n END                     AS `illion覆盖率`,
  CASE WHEN total_n = 0 THEN 0.0
       ELSE CAST(cand_cnt AS DOUBLE)/total_n END                    AS `finv覆盖率`,
  CAST(union_cnt AS BIGINT)                                        AS `并集数量`,
  CAST(cand_only_cnt AS BIGINT)                                    AS `finv独有数量`,
  CAST(inter_share AS DOUBLE)                                      AS `交集占比（并集）`,
  CAST(ref_only_share AS DOUBLE)                                   AS `illion独有占比（并集）`,
  CAST(cand_only_share AS DOUBLE)                                  AS `finv独有占比（并集）`,
  CAST(cand_cnt - ref_cnt AS BIGINT)                               AS `数量净变化`,
  CAST(matched AS BIGINT)                                          AS `一致数量`,
  CAST(ref_diff_cnt AS BIGINT)                                     AS `illion侧差异数`,
  CAST(ref_diff_rate AS DOUBLE)                                    AS `illion侧差异率`,
  CAST(contrib AS DOUBLE)                                          AS `差异贡献率`,
  CAST(LEAST(cum_contrib, 1.0) AS DOUBLE)                          AS `累计差异贡献率`,
  CAST(diff_users AS BIGINT)                                       AS `差异用户数`,
  CASE WHEN ref_users IS NULL OR ref_users = 0 THEN 0.0
       ELSE CAST(diff_users AS DOUBLE)/ref_users END               AS `差异用户占比`,
  CAST(diff_apps AS BIGINT)                                        AS `差异申请数`,
  CASE WHEN ref_apps IS NULL OR ref_apps = 0 THEN 0.0
       ELSE CAST(diff_apps AS DOUBLE)/ref_apps END                 AS `差异申请占比`,
  CAST(ref_amt AS DOUBLE)                                          AS `illion金额`,
  CAST(cand_amt AS DOUBLE)                                         AS `finv金额`,
  CAST(diff_amt AS DOUBLE)                                         AS `差异交易金额`,
  CASE WHEN ref_amt IS NULL THEN NULL
       ELSE CAST(diff_amt AS DOUBLE)/ref_amt END                   AS `差异金额占比`,
  CAST(mismatch_out AS BIGINT)                                     AS `流向其他Category`,
  CAST(cand_missing AS BIGINT)                                     AS `finv缺失`,
  CAST(mismatch_in AS BIGINT)                                      AS `来自其他Category`,
  CAST(ref_missing AS BIGINT)                                      AS `illion缺失`,
  CAST(bidirectional AS BIGINT)                                    AS `双向变动数`,
  out_text                                                         AS `主要流出去向`,
  in_text                                                          AS `主要流入来源`,
  sample_datetime
FROM ranked
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_category
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_category_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口的分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_category PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_category_stage
;


-- ---------------------------------------------------------------------------
-- STEP D3  dash_flow  差异流向（13 列 + sample_datetime = 14 列）
--   对应 label_compare.compute_difference_flows
--   行 = (sample_datetime, illion×finv×差异类型)，按天各算一份、按 sample_datetime 分区增量。
--   2026-11-13 起不再输出「排名」「建议优先级」，也不再回查 D2。
--   注：展示名是某个 key 的 clean 众数，normalize(展示名) 恒等于该 key 本身，
--   所以直接用 ref_key / cand_key 做关键词判定，与 Python 版完全等价。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_flow_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_flow_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
WITH src AS (
  SELECT * FROM model.tmp_eliam_risk_000008_dash_base
  WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
),
diff AS (
  SELECT * FROM src
  WHERE match_status IN ('mismatch', 'reference_only', 'candidate_only')
),
-- ref 侧各 Category 的交易总数 / 差异数（按天，都不含 (空) 桶）
rt  AS (SELECT sample_datetime, ref_matrix AS rm, COUNT(*) AS n FROM src  WHERE ref_matrix <> '(空)' GROUP BY sample_datetime, ref_matrix),
rdt AS (SELECT sample_datetime, ref_matrix AS rm, COUNT(*) AS n FROM diff WHERE ref_matrix <> '(空)' GROUP BY sample_datetime, ref_matrix),
g AS (
  SELECT
    sample_datetime,
    ref_matrix AS rm, cand_matrix AS cm, match_status AS st,
    COUNT(*)                          AS cnt,
    SIZE(COLLECT_SET(user_id))        AS users,
    SIZE(COLLECT_SET(application_id)) AS apps,
    SUM(abs_amount)                   AS amt,
    MAX(ref_key)                      AS rk,
    MAX(cand_key)                     AS ck
  FROM diff
  GROUP BY sample_datetime, ref_matrix, cand_matrix, match_status
),
-- 「主要交易方向」：dr_cr 的前二名（同 D2，先聚合再开窗）
dir_cnt AS (
  SELECT sample_datetime, ref_matrix AS rm, cand_matrix AS cm, match_status AS st,
         dr_cr AS disp, COUNT(*) AS c, MIN(row_ord) AS min_ord
  FROM diff WHERE dr_cr IS NOT NULL
  GROUP BY sample_datetime, ref_matrix, cand_matrix, match_status, dr_cr
),
dir AS (
  SELECT sample_datetime, rm, cm, st,
    CONCAT_WS(CONCAT(CHR(59), ' '),
      MAX(CASE WHEN rn = 1 THEN txt END),
      MAX(CASE WHEN rn = 2 THEN txt END)) AS s
  FROM (
    SELECT sample_datetime, rm, cm, st, rn, CONCAT(disp, ': ', FORMAT_NUMBER(c, 0)) AS txt
    FROM (
      SELECT sample_datetime, rm, cm, st, disp, c,
             ROW_NUMBER() OVER (PARTITION BY sample_datetime, rm, cm, st
                                ORDER BY c DESC, min_ord ASC) AS rn
      FROM dir_cnt
    ) x
  ) y WHERE rn <= 2
  GROUP BY sample_datetime, rm, cm, st
),
joined AS (
  SELECT g.*, d.s AS dir_s, rt.n AS rt_n, rdt.n AS rdt_n,
         CAST(SUM(g.cnt) OVER (PARTITION BY g.sample_datetime) AS BIGINT) AS total_diff
  FROM g
  LEFT JOIN dir d ON d.sample_datetime = g.sample_datetime AND d.rm = g.rm AND d.cm = g.cm AND d.st = g.st
  LEFT JOIN rt  ON rt.sample_datetime  = g.sample_datetime AND rt.rm  = g.rm
  LEFT JOIN rdt ON rdt.sample_datetime = g.sample_datetime AND rdt.rm = g.rm
),
flagged AS (
  SELECT j.*,
    CASE WHEN (j.rm <> '(空)' AND j.rk IS NOT NULL AND (
              INSTR(j.rk, 'liability') > 0 OR INSTR(j.rk, 'loan') > 0
           OR INSTR(j.rk, 'credit card') > 0 OR INSTR(j.rk, 'repayment') > 0
           OR INSTR(j.rk, 'income') > 0 OR INSTR(j.rk, 'wage') > 0
           OR INSTR(j.rk, 'salary') > 0 OR INSTR(j.rk, 'rent') > 0
           OR INSTR(j.rk, 'gambling') > 0 OR INSTR(j.rk, 'utilities') > 0
           OR INSTR(j.rk, 'financial institution') > 0
           OR INSTR(j.rk, 'financial service') > 0))
         OR (j.cm <> '(空)' AND j.ck IS NOT NULL AND (
              INSTR(j.ck, 'liability') > 0 OR INSTR(j.ck, 'loan') > 0
           OR INSTR(j.ck, 'credit card') > 0 OR INSTR(j.ck, 'repayment') > 0
           OR INSTR(j.ck, 'income') > 0 OR INSTR(j.ck, 'wage') > 0
           OR INSTR(j.ck, 'salary') > 0 OR INSTR(j.ck, 'rent') > 0
           OR INSTR(j.ck, 'gambling') > 0 OR INSTR(j.ck, 'utilities') > 0
           OR INSTR(j.ck, 'financial institution') > 0
           OR INSTR(j.ck, 'financial service') > 0))
         THEN '是' ELSE '否' END AS key_flag
  FROM joined j
)
SELECT
  key_flag                                     AS `是否关键Category`,
  rm                                           AS `illion_Category`,
  cm                                           AS `finv_Category`,
  CASE st WHEN 'mismatch'       THEN '分类不一致'
          WHEN 'reference_only' THEN '仅illion有分类'
          ELSE                       '仅finv有分类' END AS `差异类型`,
  CAST(cnt AS BIGINT)                          AS `数量`,
  CAST(CASE WHEN total_diff = 0 THEN 0.0
            ELSE CAST(cnt AS DOUBLE)/total_diff END AS DOUBLE) AS `占全部差异比例`,
  CASE WHEN rm = '(空)' THEN NULL
       WHEN rt_n IS NULL OR rt_n = 0 THEN 0.0
       ELSE CAST(cnt AS DOUBLE)/rt_n END       AS `占illion该Category比例`,
  CASE WHEN rm = '(空)' THEN NULL
       WHEN rdt_n IS NULL OR rdt_n = 0 THEN 0.0
       ELSE CAST(cnt AS DOUBLE)/rdt_n END      AS `占illion该Category差异比例`,
  CAST(users AS BIGINT)                        AS `影响用户数`,
  CAST(apps AS BIGINT)                         AS `影响申请数`,
  CAST(amt AS DOUBLE)                          AS `差异金额`,
  COALESCE(dir_s, '-')                         AS `主要交易方向`,
  CASE st
    WHEN 'reference_only' THEN '检查finv漏识别：文本清洗、商户覆盖、别名和兜底规则'
    WHEN 'candidate_only' THEN '核验finv新增识别是否合理，并确认illion是否存在漏标'
    ELSE '检查分类边界、关键词/规则优先级及商户知识库'
  END                                          AS `排查建议`,
  sample_datetime
FROM flagged
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_flow
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_flow_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口的分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_flow PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_flow_stage
;


-- ---------------------------------------------------------------------------
-- STEP D4  dash_group  业务大类聚类对比（19 列 + sample_datetime = 20 列）
--   对应 label_compare.compute_group_comparison
--   每天 6 行 = 合计 + 收入类 / 支出类 / 贷款类 / 转账类 / 未分类，按此展示顺序。
--   注：大类判定固化在 D0 基座，按 贷款类 -> 转账类 -> 收入类 -> 支出类 的判定优先级
--   匹配，先命中先归类——那是判定顺序，上面的行序只是展示顺序，别混。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_group_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_group_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
WITH src AS (
  SELECT * FROM model.tmp_eliam_risk_000008_dash_base
  WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
),
grp_def AS (
  SELECT '合计' AS grp, 0 AS ord
  UNION ALL SELECT '收入类', 1
  UNION ALL SELECT '支出类', 2
  UNION ALL SELECT '贷款类', 3
  UNION ALL SELECT '转账类', 4
  UNION ALL SELECT '未分类', 5
),
-- 成员 Category（按天）：ref 侧 ∪ cand 侧，按规范 key 排序
dr_k AS (SELECT sample_datetime, ref_key  AS k, MAX(ref_display)  AS disp FROM src WHERE ref_key  IS NOT NULL GROUP BY sample_datetime, ref_key),
dc_k AS (SELECT sample_datetime, cand_key AS k, MAX(cand_display) AS disp FROM src WHERE cand_key IS NOT NULL GROUP BY sample_datetime, cand_key),
mbr AS (
  SELECT sample_datetime, ref_group  AS grp, ref_key  AS k FROM src WHERE ref_key  IS NOT NULL
  UNION
  SELECT sample_datetime, cand_group AS grp, cand_key AS k FROM src WHERE cand_key IS NOT NULL
),
mbr_all AS (
  SELECT sample_datetime, '合计' AS grp, k FROM (
    SELECT sample_datetime, ref_key AS k FROM src WHERE ref_key IS NOT NULL
    UNION
    SELECT sample_datetime, cand_key     FROM src WHERE cand_key IS NOT NULL) u
),
mbr_disp AS (
  SELECT m.sample_datetime, m.grp, m.k, COALESCE(dr.disp, dc.disp, m.k) AS d
  FROM (SELECT * FROM mbr UNION ALL SELECT * FROM mbr_all) m
  LEFT JOIN dr_k dr ON dr.sample_datetime = m.sample_datetime AND dr.k = m.k
  LEFT JOIN dc_k dc ON dc.sample_datetime = m.sample_datetime AND dc.k = m.k
),
member_agg AS (
  SELECT sample_datetime, grp,
    CAST(COUNT(*) AS BIGINT) AS member_n,
    -- 先按 key 排序（拼上展示名再切回来），与 Python 的 sorted(set(...)) 一致
    CONCAT_WS(CONCAT(CHR(59), ' '), TRANSFORM(
      SORT_ARRAY(COLLECT_LIST(CONCAT(k, '|~|', d))),
      e -> SUBSTRING(e, INSTR(e, '|~|') + 3))) AS member_s
  FROM mbr_disp GROUP BY sample_datetime, grp
),
-- 每个大类一份计数（按天；合计行用 ref_in / cand_in 全 1 表示「全部」）
agg AS (
  SELECT
    t.sample_datetime,
    t.grp,
    SUM(t.ref_in)                                  AS ref_cnt,
    SUM(t.cand_in)                                 AS cand_cnt,
    SUM(t.ref_in  * t.same_g)                      AS same_g_n,
    SUM(t.ref_in  * t.same_g * t.exact)            AS exact_g_n,
    SUM(t.ref_in  * t.same_g * (1 - t.exact))      AS diff_cat_n,
    SUM(t.ref_in  * t.ref_has  * (1 - t.cand_has)) AS ref_only_n,
    SUM(t.cand_in * t.cand_has * (1 - t.ref_has))  AS cand_only_n,
    SUM(t.ref_in  * t.ref_has  * t.cand_has * (1 - t.same_g)) AS cross_n
  FROM (
    SELECT b.*, gg.grp,
      CASE WHEN gg.grp = '合计' OR b.ref_group  = gg.grp THEN 1 ELSE 0 END AS ref_in,
      CASE WHEN gg.grp = '合计' OR b.cand_group = gg.grp THEN 1 ELSE 0 END AS cand_in,
      CASE WHEN b.ref_key IS NOT NULL AND b.cand_key IS NOT NULL
                AND b.ref_group = b.cand_group THEN 1 ELSE 0 END AS same_g,
      CASE WHEN b.match_status IN ('exact_match','normalized_match') THEN 1 ELSE 0 END AS exact,
      CASE WHEN b.ref_key  IS NOT NULL THEN 1 ELSE 0 END AS ref_has,
      CASE WHEN b.cand_key IS NOT NULL THEN 1 ELSE 0 END AS cand_has
    FROM src b CROSS JOIN grp_def gg
  ) t
  GROUP BY t.sample_datetime, t.grp
),
-- 「主要跨组流向」：流出到的 cand 大类前三名（同 D2，先聚合再开窗）
cross_cnt AS (
  SELECT b.sample_datetime, gg.grp AS grp, b.cand_group AS disp,
         COUNT(*) AS c, MIN(b.row_ord) AS min_ord
  FROM src b CROSS JOIN grp_def gg
  WHERE (gg.grp = '合计' OR b.ref_group = gg.grp)
    AND b.ref_key IS NOT NULL AND b.cand_key IS NOT NULL
    AND b.ref_group <> b.cand_group
  GROUP BY b.sample_datetime, gg.grp, b.cand_group
),
cross_a AS (
  SELECT sample_datetime, grp,
    CONCAT_WS(CONCAT(CHR(59), ' '),
      MAX(CASE WHEN rn = 1 THEN txt END),
      MAX(CASE WHEN rn = 2 THEN txt END),
      MAX(CASE WHEN rn = 3 THEN txt END)) AS s
  FROM (
    SELECT sample_datetime, grp, rn, CONCAT(disp, ': ', FORMAT_NUMBER(c, 0)) AS txt
    FROM (
      SELECT sample_datetime, grp, disp, c,
             ROW_NUMBER() OVER (PARTITION BY sample_datetime, grp ORDER BY c DESC, min_ord ASC) AS rn
      FROM cross_cnt
    ) x
  ) y WHERE rn <= 3
  GROUP BY sample_datetime, grp
),
tot AS (SELECT sample_datetime, COUNT(*) AS n FROM src GROUP BY sample_datetime),
w AS (
  -- 不能写 a.*：agg 里也有 grp，会和 b.grp 撞名，Spark 报 "Reference 'grp' is ambiguous"。
  -- 这里显式列出 agg 的列并跳过它的 grp。先铺出「天 × 大类」的笛卡尔，再挂各天计数
  SELECT b.sample_datetime, b.grp, b.ord,
         a.ref_cnt, a.cand_cnt, a.same_g_n, a.exact_g_n, a.diff_cat_n,
         a.ref_only_n, a.cand_only_n, a.cross_n,
         ma.member_n, ma.member_s, ca.s AS cross_s, t.n,
         CASE WHEN t.n = 0 THEN 0.0 ELSE CAST(a.ref_cnt  AS DOUBLE)/t.n END AS ref_rate,
         CASE WHEN t.n = 0 THEN 0.0 ELSE CAST(a.cand_cnt AS DOUBLE)/t.n END AS cand_rate
  FROM (
    SELECT d.sample_datetime, g.grp, g.ord
    FROM (SELECT DISTINCT sample_datetime FROM src) d CROSS JOIN grp_def g
  ) b
  LEFT JOIN agg        a  ON a.sample_datetime  = b.sample_datetime AND a.grp  = b.grp
  LEFT JOIN member_agg ma ON ma.sample_datetime = b.sample_datetime AND ma.grp = b.grp
  LEFT JOIN cross_a    ca ON ca.sample_datetime = b.sample_datetime AND ca.grp = b.grp
  LEFT JOIN tot        t  ON t.sample_datetime  = b.sample_datetime
)
SELECT
  grp                                                       AS `业务大类`,
  CAST(COALESCE(member_n, 0) AS BIGINT)                     AS `成员数量`,
  COALESCE(NULLIF(member_s, ''), '-')                       AS `成员Category`,
  CAST(ref_cnt  AS BIGINT)                                  AS `illion数量`,
  CAST(ref_rate  AS DOUBLE)                                 AS `illion覆盖率`,
  CAST(cand_cnt AS BIGINT)                                  AS `finv数量`,
  CAST(cand_rate AS DOUBLE)                                 AS `finv覆盖率`,
  CAST(cand_rate - ref_rate AS DOUBLE)                      AS `覆盖率差`,
  CAST(same_g_n    AS BIGINT)                               AS `双方同大类数量`,
  CAST(CASE WHEN ref_cnt = 0 THEN 0.0 ELSE CAST(same_g_n    AS DOUBLE)/ref_cnt END AS DOUBLE) AS `大类一致率`,
  CAST(exact_g_n   AS BIGINT)                               AS `精确一致数量`,
  CAST(CASE WHEN ref_cnt = 0 THEN 0.0 ELSE CAST(exact_g_n   AS DOUBLE)/ref_cnt END AS DOUBLE) AS `精确一致率`,
  CAST(diff_cat_n  AS BIGINT)                               AS `组内类别不一致数量`,
  CAST(CASE WHEN ref_cnt = 0 THEN 0.0 ELSE CAST(diff_cat_n  AS DOUBLE)/ref_cnt END AS DOUBLE) AS `组内类别不一致率`,
  CAST(cross_n     AS BIGINT)                               AS `跨组流出数量`,
  CAST(CASE WHEN ref_cnt = 0 THEN 0.0 ELSE CAST(cross_n     AS DOUBLE)/ref_cnt END AS DOUBLE) AS `跨组差异率`,
  CAST(ref_only_n  AS BIGINT)                               AS `仅illion有值`,
  CAST(cand_only_n AS BIGINT)                               AS `仅finv有值`,
  COALESCE(cross_s, '-')                                    AS `主要跨组流向`,
  sample_datetime
FROM w
ORDER BY sample_datetime, ord
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_group
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_group_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口的分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_group PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_group_stage
;


-- ---------------------------------------------------------------------------
-- STEP D5  dash_detail  逐笔排查明细（23 列 + sample_datetime = 24 列）
--   对应 label_compare.build_difference_details
--   ⚠️ 线上日志没有 account_type / classification_status / engine / rule_id / reason
--      等列，2026-11-13 起也不输出「排查优先级」（P 档由看板定），所以是 21 列而非原版
--      28 列；2026-11-16 起关联 data.bank_accounts 补回银行名称、银行卡类型，共 23 列。
--   只出差异行（分类不一致 + 仅 illion 有值 + 仅 finv 有值），双方为空不进。
--   每行带自己的 sample_datetime（= 这笔日志的日期），按天筛。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_detail_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_detail_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
WITH src AS (
  SELECT * FROM model.tmp_eliam_risk_000008_dash_base
  WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
),
diff AS (
  SELECT * FROM src
  WHERE match_status IN ('mismatch', 'reference_only', 'candidate_only')
),
-- 差异流向数量：同一 (sample_datetime, ref_matrix, cand_matrix) 在 dash_flow 里的条数
flow_n AS (
  SELECT sample_datetime, `illion_Category` AS rm, `finv_Category` AS cm, MAX(`数量`) AS n
  FROM model.tmp_eliam_risk_000008_dash_flow
  GROUP BY sample_datetime, `illion_Category`, `finv_Category`
),
acct AS (
  SELECT sample_datetime, bank_account_id, bank, account_type
  FROM model.tmp_eliam_risk_000008_log_accounts
  WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
),
enriched AS (
  SELECT
    d.*,
    ac.bank,
    ac.account_type,
    COALESCE(d.ref_display,  '(空)') AS rd,
    COALESCE(d.cand_display, '(空)') AS cd
  FROM diff d
  LEFT JOIN acct ac
         ON ac.sample_datetime = d.sample_datetime
        AND ac.bank_account_id = d.bank_account_id
),
flagged AS (
  SELECT e.*,
    CASE WHEN (e.rd <> '(空)' AND e.ref_key IS NOT NULL AND (
              INSTR(e.ref_key, 'liability') > 0 OR INSTR(e.ref_key, 'loan') > 0
           OR INSTR(e.ref_key, 'credit card') > 0 OR INSTR(e.ref_key, 'repayment') > 0
           OR INSTR(e.ref_key, 'income') > 0 OR INSTR(e.ref_key, 'wage') > 0
           OR INSTR(e.ref_key, 'salary') > 0 OR INSTR(e.ref_key, 'rent') > 0
           OR INSTR(e.ref_key, 'gambling') > 0 OR INSTR(e.ref_key, 'utilities') > 0
           OR INSTR(e.ref_key, 'financial institution') > 0
           OR INSTR(e.ref_key, 'financial service') > 0))
         OR (e.cd <> '(空)' AND e.cand_key IS NOT NULL AND (
              INSTR(e.cand_key, 'liability') > 0 OR INSTR(e.cand_key, 'loan') > 0
           OR INSTR(e.cand_key, 'credit card') > 0 OR INSTR(e.cand_key, 'repayment') > 0
           OR INSTR(e.cand_key, 'income') > 0 OR INSTR(e.cand_key, 'wage') > 0
           OR INSTR(e.cand_key, 'salary') > 0 OR INSTR(e.cand_key, 'rent') > 0
           OR INSTR(e.cand_key, 'gambling') > 0 OR INSTR(e.cand_key, 'utilities') > 0
           OR INSTR(e.cand_key, 'financial institution') > 0
           OR INSTR(e.cand_key, 'financial service') > 0))
         THEN '是' ELSE '否' END AS key_flag
  FROM enriched e
),
final AS (
  SELECT f.*,
    COALESCE(fn.n, 0) AS flow_cnt
  FROM flagged f
  LEFT JOIN flow_n fn
         ON fn.sample_datetime = f.sample_datetime AND fn.rm = f.ref_matrix AND fn.cm = f.cand_matrix
)
SELECT
  CASE match_status
    WHEN 'mismatch'       THEN '分类边界冲突'
    WHEN 'reference_only' THEN 'finv漏识别'
    ELSE                       'finv新增识别' END AS `排查类型`,
  CONCAT(rd, ' → ', cd)                        AS `差异流向`,
  CAST(flow_cnt AS BIGINT)                     AS `差异流向数量`,
  key_flag                                     AS `是否关键Category`,
  transaction_date, amount, balance, dr_cr, text,
  third_party, counterparty,
  rd                                           AS `illion_Category`,
  cd                                           AS `finv_Category`,
  user_id, application_id, transaction_id, job_id, bank_account_id,
  bank                                         AS `银行名称`,
  account_type                                 AS `银行卡类型`,
  category                                     AS `illion_Category_原始`,
  bscat                                        AS `finv_Category_原始`,
  CASE match_status
    WHEN 'mismatch'       THEN '分类不一致'
    WHEN 'reference_only' THEN '仅illion有分类'
    ELSE                       '仅finv有分类' END AS `Category比对状态`,
  sample_datetime
FROM final
ORDER BY sample_datetime, flow_cnt DESC, rd ASC, cd ASC,
         user_id ASC NULLS LAST, transaction_date DESC NULLS LAST, row_ord ASC
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_detail
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_detail_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口的分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_detail PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_detail_stage
;


-- ---------------------------------------------------------------------------
-- STEP D6  dash_trend  按日期的趋势监控（增量：只替换本批窗口的分区）
--   线上新增：原 04_模型监控 sheet 靠本地 .monitor/snapshots.jsonl 存历史，线上没有这个
--   文件，改成按 sample_datetime 分批算同一组指标。「引擎命中率趋势」做不了（字段不在日志里）。
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS model.tmp_eliam_risk_000008_dash_trend_stage;
CREATE TABLE model.tmp_eliam_risk_000008_dash_trend_stage
USING PARQUET PARTITIONED BY (sample_datetime) AS
SELECT
  CAST(COUNT(*) AS BIGINT) AS total_rows,
  CAST(CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE
       CAST(SUM(CASE WHEN ref_key  IS NOT NULL THEN 1 ELSE 0 END) AS DOUBLE)/COUNT(*) END AS DOUBLE) AS illion_coverage,
  CAST(CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE
       CAST(SUM(CASE WHEN cand_key IS NOT NULL THEN 1 ELSE 0 END) AS DOUBLE)/COUNT(*) END AS DOUBLE) AS finv_coverage,
  CAST(CASE WHEN SUM(CASE WHEN ref_key IS NOT NULL AND cand_key IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 0.0 ELSE
       CAST(SUM(CASE WHEN match_status IN ('exact_match','normalized_match') THEN 1 ELSE 0 END) AS DOUBLE)
       / SUM(CASE WHEN ref_key IS NOT NULL AND cand_key IS NOT NULL THEN 1 ELSE 0 END) END AS DOUBLE) AS agreement_rate,
  CAST(SUM(CASE WHEN match_status = 'mismatch' THEN 1 ELSE 0 END) AS BIGINT) AS mismatch_count,
  CAST(CASE WHEN SUM(CASE WHEN ref_key IS NOT NULL AND cand_key IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 0.0 ELSE
       CAST(SUM(CASE WHEN match_status = 'mismatch' THEN 1 ELSE 0 END) AS DOUBLE)
       / SUM(CASE WHEN ref_key IS NOT NULL AND cand_key IS NOT NULL THEN 1 ELSE 0 END) END AS DOUBLE) AS mismatch_rate,
  CAST(CASE WHEN SUM(CASE WHEN ref_key IS NOT NULL OR cand_key IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 0.0 ELSE
       CAST(SUM(CASE WHEN match_status IN ('exact_match','normalized_match') THEN 1 ELSE 0 END) AS DOUBLE)
       / SUM(CASE WHEN ref_key IS NOT NULL OR cand_key IS NOT NULL THEN 1 ELSE 0 END) END AS DOUBLE) AS coverage_adjusted_agreement,
  CAST(SUM(CASE WHEN match_status IN ('mismatch','reference_only','candidate_only') THEN 1 ELSE 0 END) AS BIGINT) AS all_difference_count,
  CAST(CASE WHEN SUM(CASE WHEN ref_key IS NOT NULL OR cand_key IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 0.0 ELSE
       CAST(SUM(CASE WHEN match_status IN ('mismatch','reference_only','candidate_only') THEN 1 ELSE 0 END) AS DOUBLE)
       / SUM(CASE WHEN ref_key IS NOT NULL OR cand_key IS NOT NULL THEN 1 ELSE 0 END) END AS DOUBLE) AS all_difference_rate,
  CAST(SUM(CASE WHEN match_status = 'reference_only' THEN 1 ELSE 0 END) AS BIGINT) AS illion_only_count,
  CAST(SUM(CASE WHEN match_status = 'candidate_only' THEN 1 ELSE 0 END) AS BIGINT) AS finv_only_count,
  sample_datetime
FROM model.tmp_eliam_risk_000008_dash_base
WHERE sample_datetime IN (SELECT DISTINCT sample_datetime FROM model.tmp_eliam_risk_000008_log_run)
GROUP BY sample_datetime
ORDER BY sample_datetime
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_trend
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_trend_stage WHERE FALSE
;

INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_trend PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_trend_stage
;