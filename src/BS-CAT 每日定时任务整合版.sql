-- ============================================================================
-- BS-CAT 每日定时任务整合版
-- 执行顺序：
--   1. 上游：按 inserttime 读取每日增量，生成 log_* 结果表并更新 log_run
--   2. 下游：依据 log_run 中的 sample_datetime 刷新 Dashboard 结果表
-- 使用前注意：
--   * 需要先单独完成上游、Dashboard 目标表的一次性分区迁移（如尚未迁移）。
--   * 请确认调度任务中的 SET 配置对后续写入生效；本文件不包含独立的配置查询语句。
--   * 日常调度建议在每日目标数据完整入库后运行；重跑应使用相同业务窗口。
-- ============================================================================

-- ============================================================================
--  BS-CAT 策略运行日志 -> 三张结果表（按 inserttime 每日增量版）
--
--  引擎   : Hive / Spark SQL（Spark 3.x 语法，from_json / LATERAL VIEW explode）
--  源表   : ods.aus_cbd_gidp_core_gidp_strategy_run_log
--           直连即可，**不需要前置清洗**——日志 CSV 里的引号错位是「导出成
--           CSV」这一步造成的，ODS 表里 strategy_result 本来就是合法 JSON 字符串。
--           （只有走离线 CSV 那条路才需要 00_prepare_log.py，
--             见同目录 11_optional_csv_path.sql）
--  产出   : 全部落在 model 库，命名 = model.tmp_eliam_<策略码小写>_log_<用途>
--             **日增量（按 sample_datetime 分区，只替换本批窗口覆盖到的分区，历史保留）**
--               model.tmp_eliam_risk_000008_log_transactions       24 列
--               model.tmp_eliam_risk_000008_log_income_summary     22 列
--               model.tmp_eliam_risk_000008_log_liability_summary  20 列
----             中间对象（每次重建，不是产出）
--               model.tmp_eliam_risk_000008_log_src   源表过滤视图
--               model.tmp_eliam_risk_000008_log_run   剥信封后（**勿删**：
--                  看板脚本 compel_label.sql 靠它的 sample_datetime 识别本批窗口）
--               model.tmp_eliam_risk_000008_log_out   拆四段 JSON 后
--             临时 stage（每次 DROP 重建，可随时手删，不算产出）
--               model.tmp_eliam_risk_000008_log_<用途>_stage
--
--  ⚠️⚠️ **增量语义（本版与旧「整表 DROP+重建」版最大的区别）**
--     产出表是 PARQUET + 按 sample_datetime 分区，写入走「stage 暂存 -> 目标表
--     首次建（只借 schema）-> INSERT OVERWRITE 整分区替换」：**只替换本批窗口
--     覆盖到的 sample_datetime 分区，其它历史分区原样保留**，不再 DROP 整表。
--     同一天重跑幂等。由此带来的三个行为，使用前必须知道：
--       1. 窗口里某天这次没有任何成功记录 -> 该天旧分区不会被清空（写入集合
--          里没有它）。要清就手动 ALTER TABLE ... DROP PARTITION 后重跑。
--       2. **sample_datetime** 是产出表的最后一列（分区列），取日志 inserttime 前 10 位。
--          本版本按 inserttime 昨日窗口读取，因此正常情况下写入昨日 sample_datetime 分区。
--       3. 看板脚本从 log_run 的 sample_datetime 集合识别本批需要刷新的分区。
--
--  ⚠️⚠️ **依赖一次性迁移**：旧版产出表是无分区的，结构与本版不同。第一次切
--     增量模式时，先单独跑一次同目录的 migrate_once.sql，再按正常流程跑本文件。
--     忘了迁移的话：分区写入都带显式 PARTITION (sample_datetime)，会在第一条写入当场报
--     "table ... is not partitioned"（不会静默写错位数据）；显式分区写入也会报同样的错。
--
--  ⚠️⚠️ **务必「本文件全选 + 一次执行」**（SET 要和写入在同一会话里才生效）。
--     STEP A0 把 spark.sql.sources.partitionOverwriteMode 设成 DYNAMIC——没有
--     它，INSERT OVERWRITE 会先清掉**所有**分区再写窗口，历史全丢。
--     切增量后的第一次，请先单独跑一次 STEP A0 最后那行不带等号的 SET，
--     确认回显是 DYNAMIC 再全选跑整份。若回显不是 DYNAMIC，停下来反馈。
--
--  ⚠️ 策略码写死在表名和 STEP 0 的 WHERE 两处，换策略（如 RISK_000009）时
--     要同时改：搜索替换 risk_000008 → risk_000009，以及 WHERE strategy_code。
--
--  列集口径 = 当前版本出参契约（classification_core/service.py 的
--  _ILLION_ROW_EXCLUDE / _STREAM_SUMMARY_EXCLUDE），不是 8/5 那份 report1.xlsx。
--  差异：report1.xlsx 的 transactions 29 列里有 9 列是平台侧的
--        （job_id / bsb / account_no / classification_*），模型出参不产生；
--        income_summary 的 bscat / bank_account_id / application_id / counterparty
--        与账户元数据 account_type / bank / credit_limit 也一样。按约定不补。
--
--  已用 spark3-35563.csv（10 应用 / product=fundo / run_status=1）实测核对：
--    · 出参挂在 strategy_result.$.data                      ✅
--    · 逐笔行 14 字段、income 14、liability 12、category 8  ✅ 与下面的 schema 逐一对应
--    · 逐笔 explode 行数 == bscat_stats.txn_raw_input_cnt   ✅ 10/10 一致
--  但**本机没有 pyspark / java，这份 SQL 没在 Spark 上真跑过**，只在
--  sql/99_dryrun_check.py 里用 pandas 等价复现验证了逻辑与列名。
--  改增量版后，结构自检换成了 sql/97_lint_incremental.py（查 stage/目标表
--  配对、分区列位置、DROP 白名单、SET DYNAMIC 是否在位）。
--
--  旧版（整表 DROP+重建）留档在 sql/10_report1_tables.sql。
-- ============================================================================


-- ---------------------------------------------------------------------------
-- STEP A0  会话设置（必须先于所有写入执行，全选跑自然带上）
--
-- 动态分区覆盖：INSERT OVERWRITE 只替换「stage 里实际出现的 sample_datetime 分区」，
-- 窗口外的历史分区原样保留。这是整个增量逻辑的地基。
-- ⚠️ 会话级设置：客户端里把本文件全选、一次执行即可。若你的工具是「逐条语句
--    各开一个新会话」，这里会失效，而失效后 INSERT OVERWRITE 会先清空所有
--    分区再写窗口 = 丢历史。切增量后的第一次请先单独跑下面最后一行，确认
--    回显 DYNAMIC 再继续。
-- ---------------------------------------------------------------------------
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;
SET hive.exec.dynamic.partition.mode = nonstrict;



-- ---------------------------------------------------------------------------
-- STEP 0  源表视图：每日按 inserttime 增量读取
--
-- 每日调度时自动读取「昨天 00:00（含）至今天 00:00（不含）」入库的日志。
-- sample_datetime 仍取 inserttime 前 10 位，因此本次读取的数据按入库日期写入对应分区。
-- 注意：如果源表按 par_dt 分区，而查询没有 par_dt 条件，可能会扫描大量源表分区；
--       若要做分区裁剪，需确认 par_dt 与 inserttime 的关系，再增加安全的 par_dt 范围。
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS model.tmp_eliam_risk_000008_log_src;
CREATE VIEW model.tmp_eliam_risk_000008_log_src AS
SELECT * FROM ods.aus_cbd_gidp_core_gidp_strategy_run_log
WHERE strategy_code = 'RISK_000008'          -- 包装分类模型的策略编码
  AND run_status    = 1                      -- 1=成功（error_code=0000），2=失败（error_code=2002）
  AND isactive      = 1                      -- 过滤逻辑删除
  AND inserttime >= concat(date_format(date_sub(current_date(), 1), 'yyyy-MM-dd'), ' 00:00:00')
  AND inserttime <  concat(date_format(current_date(), 'yyyy-MM-dd'), ' 00:00:00')
;
-- 若同一策略下有多个模型/多个 release 混跑，再补 release_id（本批为 118）。
--
-- ⚠️ 性能提醒：当前增量条件是 inserttime，而源表可能按 par_dt 分区。
--    若 par_dt 与 inserttime 不是严格同日关系，不能随意用 par_dt 限制数据，否则可能漏数。
--    上线前请检查执行计划/扫描分区数；若全表扫描，需要结合数据延迟规律增加安全的
--    par_dt 分区裁剪范围，或采用调度平台维护的增量水位。

-- ── 离线 CSV 通路已挪走 ─────────────────────────────────────────────────
-- 备选方案（只在「手上只有导出的 CSV、连不上 ODS 表」时才用）挪到了
-- 同目录的 11_optional_csv_path.sql。
--
-- 本文件按当前日期自动读取昨日 inserttime 窗口，无需每天手动修改日期。
-- ─────────────────────────────────────────────────────────────────────────


-- ---------------------------------------------------------------------------
-- STEP 1  剥壳：取出成功运行，并把模型出参从响应信封里拎出来
--
-- 出参形状（已实测）：
--   成功  strategy_result = {"code":"0000","data":{...模型出参...},...}
--   失败  strategy_result = {"code":"2002","data":null,"message":"...",...}
-- 所以 run_status = '1' 且 data 非空、非字面量 'null' 就是一次有效运行。
-- 失败记录里 code 恒为 2002，成功恒为 0000，两者可互为交叉校验。
--
-- 另注：strategy_content.midVarJson 里另存了一份同样的出参，是备用源；
--      本 SQL 用 strategy_result，因为它才是响应本体、且带 code 可判成败。
--      若某批日志的 strategy_result 被截断，可改从 midVarJson 取。
-- 注意 run_status 在两条源通路里类型不同（ODS 表是 INT，CSV 外部表是 STRING），
-- 所以统一 CAST 成字符串再比，这样同一份 SQL 两边都能跑。
--
-- 本批窗口 = STEP 0 中 inserttime 的昨日时间范围：log_run / log_out 每次重建，只装本批新增日志。
-- ⚠️ log_run 别删——看板脚本靠它的 sample_datetime 集合识别「本批要刷新哪些分区」。
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
-- STEP 2  表一：逐笔交易
--
-- v1 / illion（product = fundo）：行是固定字段集，用下面的固定 schema 解析。
--
-- ⚠️ v2 / wagego 的逐笔行 = 上游字段原顺序透传 + counterparty / bscat / stream_id，
--    列集由入参报文决定、不固定。本批日志 10 个应用全是 fundo，走不到 wagego；
--    若将来日志里混入 wagego 报文，固定 schema 会**静默吃掉上游列**（不报错），
--    届时要么按报文列集补 schema，要么整行转 MAP<STRING,STRING> 再按 key 取。
--
-- 增量写法：stage 只装窗口数据 -> 目标表首次建（只借 schema，空表）-> 整分区替换。
-- ⚠️ sample_datetime 必须是 SELECT 的最后一列（分区列约定，值 = inserttime 前
--    10 位），下面已挪到末尾。
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

-- 首次切增量时建表（借 stage 的 schema，不装数据）；之后每次跑都是 no-op
CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_log_transactions
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_log_transactions_stage WHERE FALSE
;

-- 每日写入：只替换窗口覆盖到的 sample_datetime 分区，历史分区不动。
-- 显式 PARTITION (sample_datetime)：目标表若还是旧的非分区表（忘了迁移），这里会当场
-- 报 "table ... is not partitioned"，不会静默按位置写错位数据。
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_log_transactions PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_log_transactions_stage
;


-- ---------------------------------------------------------------------------
-- STEP 3  表二：收入流汇总（14 列）
--
-- 出参已剔除 bscat / bank_account_id / application_id / counterparty 与
-- 账户元数据三列（_STREAM_SUMMARY_EXCLUDE），所属应用从顶层回显标识取。
-- 增量写法同 STEP 2（stage -> 首次建表 -> 整分区替换），sample_datetime 在末尾。
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
-- STEP 4  表三：负债流汇总（12 列）
-- 增量写法同 STEP 2（stage -> 首次建表 -> 整分区替换），sample_datetime 在末尾。
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


-- 上游自检查询已移除：该任务用于定时写入，不输出独立查询结果。


-- ============================================================================
-- 阶段二：Dashboard 监控数据刷新
-- 必须在阶段一成功完成后执行；本阶段通过 log_run 获取本批刷新日期。
-- ============================================================================

-- ============================================================================
--  蓝鲸策略运行日志  ->  Category 差异看板（6 张表，**每日增量版**）
--
--  引擎   : Hive / Spark SQL（用到 CONCAT_WS / COLLECT_LIST / 窗口函数 / 幂等 CASE）
--  上游   : model.tmp_eliam_risk_000008_log_transactions   （先跑 report_table.sql）
--           它是**逐日累计**的：本文件读到的历史天数取决于上游累计过多少。
--  产出   : 全部落在 model 库，命名 = model.tmp_eliam_risk_000008_dash_<用途>
--             **全部按天累积（每张表都带 sample_datetime 列、按 sample_datetime 分区，只替换本批
--             窗口覆盖到的天，历史天不动）**
--               ..._dash_base         口径基座（clean/normalize/status/大类）
--               ..._dash_summary      核心指标         每天 15 行 x  8 列
--               ..._dash_category     逐Category差异   每天 各Category一行 x 34 列
--               ..._dash_flow         差异流向        每天 各流向一行 x 16 列
--               ..._dash_group        业务大类聚类      每天 6 行 x 20 列
--               ..._dash_detail       逐笔排查明细      每天 各差异笔一行 x 23 列
--               ..._dash_trend        按日趋势          每天一行 x 12 列
--             临时 stage（每次 DROP 重建，可随时手删，不算产出）
--               ..._dash_<用途>_stage
--
--  ⚠️⚠️ **本版与旧「整表 DROP+重建」版的行为差异**
--     1. **本文件不设窗口**：要刷新哪些 sample_datetime 分区，从 log_run 的 sample_datetime 集合读
--        （那是 report_table.sql 每跑一次重建的、只装本批窗口的中间表）。
--        所以正确顺序永远是：先 report_table.sql，再本文件。
--     2. **七张 dash 表全部按天累积**：五张看板（summary/category/flow/group/
--        detail）都改成「每天一组行」——优先级切点、差异贡献率等也是**各天内部**
--        计算（= 原来「一批一次」的语义，线上每天跑一批，效果一致），每张表都
--        带 sample_datetime 列、按 sample_datetime 分区。重跑同一批 = 只替换那几天的分区。
--        base / trend 同理（trend 本来就是按天）。不删任何表对象。
--
--  ⚠️ 产出表的日期键叫 **sample_datetime**（2026-10-09 起，替代原来的 par_dt）：
--     值 = 日志 inserttime 的前 10 位（形如 '2026-09-22'），11 张表都按它分区；
--     inserttime 异常时上游会回落到源表分区 par_dt。源表 par_dt 只在上游
--     report_table.sql 的取数窗口里出现，产出里没有这个名字。
--
--  ⚠️⚠️ **和 report_table.sql 一样依赖一次性迁移**（旧产出表结构不同）。
--     切换时先跑一次同目录的 migrate_once.sql，再按 report_table -> 本文件顺序跑。
--     忘了迁移的话：base / trend 的写入带显式 PARTITION (sample_datetime)，会当场报
--     "table ... is not partitioned"（不会静默写错位数据）；D7 的
--     SHOW PARTITIONS 守卫也会报同样的错。
--
--  ⚠️⚠️ **必须「本文件全选 + 一次执行」**。STEP A0 把
--     spark.sql.sources.partitionOverwriteMode 设成 DYNAMIC；没有它，
--     base / trend 的 INSERT OVERWRITE 会先清掉所有分区再写窗口，历史全丢。
--     第一次请先单独跑 STEP A0 最后那行不带等号的 SET，确认回显 DYNAMIC 再继续。
--
--  口径来源 = D:\project\蓝鲸\label_compare.py 的 compute_* 函数。
--  **本 SQL 的口径已逐表 diff 验证过**：把同样的逻辑先用 pandas 复现，再与
--  label_compare.py 的真实函数对拍，5 张有 Python 对标的表全部逐行逐列一致
--  （见 sql/99_dryrun_dashboard.py）。改口径要同步改那个脚本再跑一遍。
--
--  四处**刻意的**与 Python 版不一致：
--    1. dash_detail 是 22 列（另加 sample_datetime 列 = 23 列）而非 28 列——线上日志没有
--       account_type、classification_status / engine / rule_id / reason、
--       sample_datetime 这 6 列。
--    2. dash_trend 是新增的：原 04_模型监控 sheet 靠本地 .monitor/snapshots.jsonl
--       存历史，线上没有这个文件，改成按 sample_datetime 分批。原版的「引擎命中率趋势」
--       做不了（classification_engine 不在日志里）。
--    3. **列名里的空格换成了下划线**（只是表头名字，数据一字未动）：
--         dash_flow   : illion Category      -> illion_Category
--                       finv Category        -> finv_Category
--         dash_detail : 上面两个 + illion Category_原始 -> illion_Category_原始
--                                finv Category_原始   -> finv_Category_原始
--       原因见下面那条 ⚠️。
--    4. 所有看板**按天分组、各带 sample_datetime 列**（Python 版是「一批一份」的单批
--       视角；线上每天跑一批，于是每天一组行累积在表里，效果等价）。
--
--  ⚠️⚠️ **产出列名里不能有空格**（以及逗号、分号、花括号、圆括号、等号、制表符、换行）。
--     Hive 写表时拿列名当子目录名，Spark 的 DataSourceUtils.checkFieldNames
--     在 FileFormatWriter.write 里无条件检查（跟文件格式无关，USING PARQUET 也躲不掉），
--     命中就抛 [INVALID_COLUMN_NAME_AS_PATH]，而且 **按 schema 顺序遇到第一个就抛**,
--     所以报错只显示一个名字，不代表只有一列有问题。汉字是合法的（dash_flow 的
--     `排名` / `建议优先级` 都通过了检查），**只有空格这类字符不行**。
--     sql/97_lint_incremental.py 会静态检查产出列名。
--     注意：只有**产出列名**受限，字符串字面量里的空格随便写。
--
--  ⚠️ 并列时的顺序：Python 版用 pandas `value_counts()`，并列按**首次出现顺序**。
--     SQL 没有天然的「行序」，所以本脚本用 ROW_NUMBER() OVER (ORDER BY log_id,
--     transaction_id) 造了一个 row_ord 当 tie-break。它只影响「主要流出去向 /
--     主要流入来源 / 主要交易方向 / 主要跨组流向」里**并列项之间的先后**，
--     不影响任何计数与比率。若你的表恰好与导出前的行序一致，结果就与 Python 版
--     完全一样；否则并列项可能换个位置。真在意就把 row_ord 的 ORDER BY 换成
--     你自己的行序口径。
--
--  ⚠️ 策略码写死在表名里（risk_000008）。换策略要全局搜索替换。
--
--  ⚠️⚠️ **不要在字符串字面量或注释里写 ASCII 分号。** 蓝鲸/Kyuubi 的 SQL 客户端
--     是拿朴素 split 切语句的（不认引号），一句里只要有 `引号-分号-空格` 就会被
--     从中间切成两半，后半段以「引号逗号」开头，Spark 直接 PARSE_SYNTAX_ERROR。
--     所以拼接分隔符一律写成 CONCAT(CHR(59), ' ')，不要直接写那个两字符字面量。
--     sql/97_lint_incremental.py 会静态检查这一点（连这条注释本身也查）。
--
--  ⚠️ 本机没有 pyspark / java，这份 SQL 没在 Spark 上真跑过；校验是在 pandas
--     层做的等价复现 + 差分对拍。上线前请在测试环境完成数据核对。
--  旧版（整表 DROP+重建）留档在 sql/20_dashboard_tables.sql。
-- ============================================================================


-- ---------------------------------------------------------------------------
-- STEP A0  会话设置（必须先于所有写入执行，全选跑自然带上）
--
-- 动态分区覆盖：base / trend 的 INSERT OVERWRITE 只替换「stage 里实际出现的
-- sample_datetime 分区」，其它历史分区原样保留。
-- ⚠️ 会话级设置：全选一次执行即可。若工具是「逐条语句各开新会话」这里会失效，
--    失效后 INSERT OVERWRITE 会先清空所有分区再写窗口 = 丢历史。
--    切增量后的第一次请先单独跑下面最后一行，确认回显 DYNAMIC 再继续。
-- ---------------------------------------------------------------------------
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;
SET hive.exec.dynamic.partition.mode = nonstrict;



-- ---------------------------------------------------------------------------
-- STEP D0  口径基座（增量：只替换本批窗口的 sample_datetime 分区）
--
-- 把逐笔交易表加工成后面所有看板共用的形态，对应 label_compare.py 的
-- prepare_comparison_data + assign_group_columns。
--
--   ref  = illion 侧，取 transactions.category
--   cand = 模型侧，取 transactions.bscat
--
-- 三段式：row_ord -> clean -> normalize。每一层只写一次表达式，避免抄错。
--
-- clean(x)   = 折叠空白 + 去首尾 + 空值词表判空（保留原大小写）
-- normalize  = 小写 -> '&' 变 ' and ' -> [-_/] 变空格 -> 非字母数字变空格 -> 折叠空白
--              ⚠️ SQL 用 [^\p{L}\p{N}_\s] 对应 Python 的 \w(UNICODE)；
--                 本批 Category 全 ASCII，两者等价。
--
-- 增量写法：row_ord / 展示名 / 大类都在**全表（含历史）**上算，保证跨天一致
-- （tie-break 行序、展示名众数与全量重建逐字节一致）；只有写入按「本批窗口的
-- sample_datetime 分区」替换 —— 窗口 = log_run 的 sample_datetime 集合。
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
-- 展示名：每个 key 取出现最多的 clean 值；并列取字典序最小。
-- ⚠️ 必须分两趟：ref 侧先建，cand 侧只补 ref 侧没有的 key——两侧合并统计会得到
--    不同的众数。这与 label_compare.build_display_map 的循环顺序一致。
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

-- 首次切增量时建表（借 stage 的 schema，不装数据）；之后每次跑都是 no-op
CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_base
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_base_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口覆盖到的 sample_datetime 分区，历史天不动。
-- 显式 PARTITION (sample_datetime)：目标表若还是旧的非分区表（忘了迁移），这里会当场
-- 报 "table ... is not partitioned"，不会静默按位置写错位数据。
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_base PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_base_stage
;


-- ---------------------------------------------------------------------------
-- STEP D1  dash_summary  核心指标（**按天**：每天 15 行，按 sample_datetime 分区增量）
--   对应 label_compare.compute_summary_v2——原来口径就是「一批（一天）算一组
--   15 个指标」，现在按 sample_datetime 各算一组，每天 15 行累积在表里。
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

-- 每日写入：只替换本批窗口覆盖到的 sample_datetime 分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_summary PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_summary_stage
;


-- ---------------------------------------------------------------------------
-- STEP D2  dash_category  逐 Category 差异与优化优先级（33 列 + sample_datetime = 34 列）
--   对应 label_compare.compute_category_comparison_v2
--
--   优先级切点用**精确分位数**（pandas quantile 是线性插值），不用
--   percentile_approx——后者返回真实数据点而非插值，边界上会把 P1/P2 判错。
--
--   **按天**：同一套口径按 sample_datetime 各算一份（优先级切点也是各天内部算），
--   行 = （sample_datetime, Category），按 sample_datetime 分区增量。
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
-- ⚠️ 必须先 GROUP BY 出计数，再在**纯列**上开窗。不要在窗口的 ORDER BY 里直接写
--    COUNT(*)/MIN(row_ord)——那是在聚合结果上再套聚合，Spark 的解析器会报
--    "aggregate function in the argument of another aggregate"。分两层就没有歧义。
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
-- （dash_base 里 ref_display / cand_display 已经是「该 key 的众数 clean 值」，
--   同一个 key 的每一行都相同，所以 MAX 只是取那一个值）
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
-- 优先级切点：positive = 差异数>0；high = q0.75，medium = median（线性插值）
-- 按天各算一份：切点 = 该天内部的分位数
pos AS (
  SELECT sample_datetime, ref_diff_cnt AS v,
         ROW_NUMBER() OVER (PARTITION BY sample_datetime ORDER BY ref_diff_cnt) - 1 AS idx,
         COUNT(*) OVER (PARTITION BY sample_datetime) AS n
  FROM raw WHERE ref_diff_cnt > 0
),
cut AS (
  SELECT
    sample_datetime,
    MAX(n) AS n,
    MAX(CASE WHEN idx = CAST(FLOOR(0.75*(n-1)) AS BIGINT) THEN v END) AS hi_lo,
    MAX(CASE WHEN idx = CAST(CEIL (0.75*(n-1)) AS BIGINT) THEN v END) AS hi_hi,
    MAX(CASE WHEN idx = CAST(FLOOR(0.50*(n-1)) AS BIGINT) THEN v END) AS md_lo,
    MAX(CASE WHEN idx = CAST(CEIL (0.50*(n-1)) AS BIGINT) THEN v END) AS md_hi
  FROM pos
  GROUP BY sample_datetime
),
-- 把切点和总差异数挂到每一行（按天）
sorted AS (
  SELECT r.*,
    CASE WHEN cut.n IS NULL OR cut.n = 0 THEN 0.0
         ELSE cut.hi_lo + (cut.hi_hi - cut.hi_lo)
              * (0.75*(cut.n-1) - FLOOR(0.75*(cut.n-1)))
    END AS p1_cutoff,
    CASE WHEN cut.n IS NULL OR cut.n = 0 THEN 0.0
         ELSE cut.md_lo + (cut.md_hi - cut.md_lo)
              * (0.50*(cut.n-1) - FLOOR(0.50*(cut.n-1)))
    END AS p2_cutoff,
    SUM(r.ref_diff_cnt) OVER (PARTITION BY r.sample_datetime) AS total_ref_diff
  FROM raw r LEFT JOIN cut ON cut.sample_datetime = r.sample_datetime
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
-- 累计差异贡献率必须在**排好序之后**再算，所以序号和 cumsum 放在同一个窗口定义里
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
  -- determine_priority：逐字对应 label_compare._determine_priority
  CASE
    WHEN ref_diff_cnt <= 0 THEN 'P3'
    WHEN (ref_diff_cnt >= p1_cutoff AND (contrib >= 0.05 OR ref_diff_rate >= 0.30))
      OR (`关键Category` = '是' AND (ref_diff_cnt >= p1_cutoff OR contrib >= 0.05))
      THEN 'P1'
    WHEN ref_cnt < 20 AND `关键Category` = '否' AND contrib < 0.02 THEN 'P3'
    WHEN ref_diff_cnt >= p2_cutoff OR contrib >= 0.02 OR ref_diff_rate >= 0.15
      OR `关键Category` = '是' THEN 'P2'
    ELSE 'P3'
  END AS `建议优先级`,
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

-- 每日写入：只替换本批窗口覆盖到的 sample_datetime 分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_category PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_category_stage
;


-- ---------------------------------------------------------------------------
-- STEP D3  dash_flow  差异流向（15 列 + sample_datetime = 16 列）
--   对应 label_compare.compute_difference_flows
--   **按天**：行 = （sample_datetime, illion×finv×差异类型），按 sample_datetime 分区增量。
--   ⚠️ 依赖 D2（优先级按 (sample_datetime, 展示名) 回查刚重算过的 dash_category），
--      必须紧跟 D2 之后跑。
--
--   注：Python 里 `sql_normalize(rm)` 的 rm 是**展示名**，而「展示名是某个 key 的
--   clean 众数」，且 normalize(clean) 对这个 key 恒等于 key 本身——所以直接用
--   ref_key / cand_key 做关键词判定，与 normalize(展示名) 完全等价。
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
    MIN(row_ord)                      AS first_ord,
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
  SELECT g.*, d.s AS dir_s, c.`建议优先级` AS cat_pri, rt.n AS rt_n, rdt.n AS rdt_n,
         CAST(SUM(g.cnt) OVER (PARTITION BY g.sample_datetime) AS BIGINT) AS total_diff
  FROM g
  LEFT JOIN dir d ON d.sample_datetime = g.sample_datetime AND d.rm = g.rm AND d.cm = g.cm AND d.st = g.st
  LEFT JOIN rt  ON rt.sample_datetime  = g.sample_datetime AND rt.rm  = g.rm
  LEFT JOIN rdt ON rdt.sample_datetime = g.sample_datetime AND rdt.rm = g.rm
  LEFT JOIN model.tmp_eliam_risk_000008_dash_category c
         ON c.sample_datetime = g.sample_datetime AND c.`Category` = g.rm
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
),
final AS (
  SELECT f.*,
    CASE
      WHEN COALESCE(f.cat_pri, 'P3') NOT IN ('P1','P2','P3') THEN 'P3'
      WHEN f.st = 'candidate_only' AND f.key_flag = '是'
           AND COALESCE(f.cat_pri, 'P3') = 'P3' THEN 'P2'
      ELSE COALESCE(f.cat_pri, 'P3')
    END AS pri
  FROM flagged f
)
SELECT
  ROW_NUMBER() OVER (
    PARTITION BY sample_datetime
    ORDER BY CASE pri WHEN 'P1' THEN 0 WHEN 'P2' THEN 1 WHEN 'P3' THEN 2 ELSE 3 END ASC,
             cnt DESC, apps DESC, amt DESC NULLS LAST, first_ord ASC) AS `排名`,
  pri                                          AS `建议优先级`,
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
FROM final
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_flow
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_flow_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口覆盖到的 sample_datetime 分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_flow PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_flow_stage
;


-- ---------------------------------------------------------------------------
-- STEP D4  dash_group  业务大类聚类对比（19 列 + sample_datetime = 20 列）
--   对应 label_compare.compute_group_comparison
--   **每天** 6 行 = 合计 + 收入类 / 支出类 / 贷款类 / 转账类 / 未分类（合计在最前）
--
--   注：大类判定已固化在 D0 的基座里，关键词按
--   贷款类 -> 转账类 -> 收入类 -> 支出类 的**判定**优先级匹配，先命中先归类；
--   上面这个行序是**展示**顺序，两者不是一回事，别混。
--   **按天**：每天 6 行（合计 + 5 大类），按 sample_datetime 分区增量。
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
  -- 注意：不能写 a.*，agg 里也有 grp，会和 b.grp 撞名，最终 SELECT 引用 grp 时
  -- Spark 报 "Reference 'grp' is ambiguous"。这里显式列出 agg 的列并跳过它的 grp。
  -- 先铺出「天 × 大类」（合计 + 5 类）的笛卡尔，再挂各天各自的计数
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

-- 每日写入：只替换本批窗口覆盖到的 sample_datetime 分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_group PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_group_stage
;


-- ---------------------------------------------------------------------------
-- STEP D5  dash_detail  逐笔排查明细（22 列 + sample_datetime = 23 列）
--   对应 label_compare.build_difference_details
--   ⚠️ 线上日志没有 account_type / classification_status / engine / rule_id /
--      reason / sample_datetime 这 6 列，所以是 22 列（+ sample_datetime = 23 列）而非
--      原版的 28 列。
--   只出差异行（分类不一致 + 仅 illion 有值 + 仅 finv 有值），双方为空不进。
--   **按天**：每行带自己的 sample_datetime（= 这笔日志的日期），按 sample_datetime 分区增量、
--   历史累积，可以按天筛。
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
enriched AS (
  SELECT
    d.*,
    COALESCE(d.ref_display,  '(空)') AS rd,
    COALESCE(d.cand_display, '(空)') AS cd,
    c.`建议优先级`                    AS cat_pri
  FROM diff d
  LEFT JOIN model.tmp_eliam_risk_000008_dash_category c
         ON c.sample_datetime = d.sample_datetime AND c.`Category` = d.ref_display
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
    -- 同 D3：candidate_only + 关键Category + P3 -> 提到 P2
    CASE
      WHEN COALESCE(f.cat_pri, 'P3') NOT IN ('P1','P2','P3') THEN 'P3'
      WHEN f.match_status = 'candidate_only' AND f.key_flag = '是'
           AND COALESCE(f.cat_pri, 'P3') = 'P3' THEN 'P2'
      ELSE COALESCE(f.cat_pri, 'P3')
    END AS pri,
    COALESCE(fn.n, 0) AS flow_cnt
  FROM flagged f
  LEFT JOIN flow_n fn
         ON fn.sample_datetime = f.sample_datetime AND fn.rm = f.ref_matrix AND fn.cm = f.cand_matrix
)
SELECT
  pri                                          AS `排查优先级`,
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
  category                                     AS `illion_Category_原始`,
  bscat                                        AS `finv_Category_原始`,
  CASE match_status
    WHEN 'mismatch'       THEN '分类不一致'
    WHEN 'reference_only' THEN '仅illion有分类'
    ELSE                       '仅finv有分类' END AS `Category比对状态`,
  sample_datetime
FROM final
ORDER BY sample_datetime, pri ASC, flow_cnt DESC, rd ASC, cd ASC,
         user_id ASC NULLS LAST, transaction_date DESC NULLS LAST, row_ord ASC
;

CREATE TABLE IF NOT EXISTS model.tmp_eliam_risk_000008_dash_detail
USING PARQUET PARTITIONED BY (sample_datetime)
AS SELECT * FROM model.tmp_eliam_risk_000008_dash_detail_stage WHERE FALSE
;

-- 每日写入：只替换本批窗口覆盖到的 sample_datetime 分区，历史天不动
INSERT OVERWRITE TABLE model.tmp_eliam_risk_000008_dash_detail PARTITION (sample_datetime)
SELECT * FROM model.tmp_eliam_risk_000008_dash_detail_stage
;


-- ---------------------------------------------------------------------------
-- STEP D6  dash_trend  按日期的趋势监控（增量：只替换本批窗口的 sample_datetime 分区）
--   线上新增：原 04_模型监控 sheet 靠本地 .monitor/snapshots.jsonl 存历史，
--   线上没有这个文件，改成按 sample_datetime 分批算同一组指标（本版按天累计保留，
--   窗口外的历史天不动）。
--   原版的「引擎命中率趋势」做不了——classification_engine 不在日志里。
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