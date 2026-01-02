import great_expectations as gx
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import traceback
import os
import json
import threading
import gc

# --- 全局排队控制 ---
_SHARED_GX_CONTEXT = None
# 使用递归锁 (RLock) 确保线程安全
gx_lock = threading.RLock() 

def get_gx_context():
    """初始化并复用 GX Ephemeral Context"""
    global _SHARED_GX_CONTEXT
    with gx_lock:
        if _SHARED_GX_CONTEXT is None:
            # ephemeral 模式不写磁盘，适合流式处理
            _SHARED_GX_CONTEXT = gx.get_context(mode="ephemeral")
            print("✅ Initialised new Ephemeral Context (Serial Queue Mode)")
        return _SHARED_GX_CONTEXT

def load_suite_if_not_exists(context, suite_name):
    """从 Volumes 安全加载 Expectation Suite"""
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        suite_path = f"/Volumes/traffic_dev_catalog/gx/gx_configs/expectations/{suite_name}.json"
        if os.path.exists(suite_path):
            with open(suite_path, "r") as f:
                suite_dict = json.load(f)
            
            # 清理 1.x 兼容性字段
            suite_dict.pop("name", None) 
            suite_dict.pop("data_context_id", None)
            expectations = suite_dict.get("expectations", [])
            
            new_suite = gx.ExpectationSuite(
                name=suite_name,
                expectations=expectations
            )
            return context.suites.add(new_suite)
        else:
            raise FileNotFoundError(f"Missing Suite JSON at: {suite_path}")

def validate_and_insert_process_batch(df, catalog, schema, batch_id, table_name): 
    """串行排队处理 GX 验证与数据持久化"""
    
    # 获取当前 DataFrame 的 SparkSession，避免 NameError: name 'spark' is not defined
    spark_internal = df.sparkSession

    # 1. 快速检查空批次
    if df.limit(1).count() == 0:
        return

    # 路径与配置定义
    full_target_table = f"{catalog}.{schema}.{table_name}"
    quarantine_table = f"{catalog}.gx.data_quality_quarantine"
    temp_id_col = "_dq_batch_id"
    ds_name = f"ds_{table_name}_{batch_id}"
    val_def_name = f"val_{table_name}_{batch_id}"
    
    # 2. 为验证准备数据（带唯一 ID）
    # 如果 Driver 内存持续报错 Allocation Failure，请移除底部的 .persist()
    df_with_id = df.withColumn(temp_id_col, F.monotonically_increasing_id()).persist()
    
    result = None 
    
    # --- 串行锁开始 (核心排队区) ---
    with gx_lock:
        try:
            print(f"🔒 Batch {batch_id}: Processing {table_name} (Serial Lock)...", flush=True)
            context = get_gx_context()
            
            # 清理历史残留，防止内存泄漏
            try: context.validation_definitions.delete(val_def_name)
            except: pass
            try: context.data_sources.delete(ds_name)
            except: pass

            # 配置 GX 对象
            datasource = context.data_sources.add_spark(name=ds_name)
            asset = datasource.add_dataframe_asset(name=f"asset_{batch_id}")
            batch_def = asset.add_batch_definition_whole_dataframe(name="batch_def")
            suite = load_suite_if_not_exists(context, f"{table_name}_suite")
            
            val_definition = context.validation_definitions.add(
                gx.ValidationDefinition(name=val_def_name, data=batch_def, suite=suite)
            )

            # 运行验证
            print(f"🚀 Batch {batch_id}: Running GX validation...", flush=True)
            result = val_definition.run(
                batch_parameters={"dataframe": df_with_id},
                result_format={
                    "result_format": "COMPLETE", 
                    "unexpected_index_column_names": [temp_id_col]
                }
            )
            
            # 【优化】立即清理 GX 内存引用
            context.validation_definitions.delete(val_def_name)
            context.data_sources.delete(ds_name)
            del val_definition, asset, datasource
            
        except Exception as e:
            print(f"❌ Batch {batch_id} GX Error: {str(e)}")
            # 降级处理：验证报错则直接全量入库
            df_with_id.drop(temp_id_col).write.mode("append").saveAsTable(full_target_table)
            return 
        finally:
            gc.collect() # 显式触发 Python 垃圾回收
            print(f"🔓 Batch {batch_id}: Released Lock.")

    # --- 数据分流与入库 (锁外执行以提高 IO 并行度) ---
    try:
        if result and result.success:
            print(f"✅ Batch {batch_id}: {table_name} Validation Passed.")
            df_with_id.drop(temp_id_col).write.mode("append") \
                .option("mergeSchema", "true").saveAsTable(full_target_table)
        elif result:
            # 收集错误行 ID 和规则
            errors = []
            for r in result.results:
                if not r.success:
                    col = r.expectation_config.kwargs.get("column", "Table")
                    rule = r.expectation_config.type
                    ids = r.result.get("unexpected_index_list")
                    if ids:
                        for row_id_dict in ids:
                            val = row_id_dict.get(temp_id_col)
                            if val is not None:
                                errors.append((val, f"[{col}] {rule}"))
            
            if not errors: # 结果失败但无具体错误行（表级规则失败）
                df_with_id.drop(temp_id_col).write.mode("append").saveAsTable(full_target_table)
                return

            # 创建错误详情表
            error_schema = StructType([
                StructField(temp_id_col, LongType(), True),
                StructField("violated_rule", StringType(), True)
            ])
            error_info_df = spark_internal.createDataFrame(errors, schema=error_schema) \
                .groupBy(temp_id_col).agg(F.concat_ws("; ", F.collect_list("violated_rule")).alias("violated_rules"))

            bad_row_ids = [e[0] for e in errors]
            
            # 坏数据分流入 Quarantine
            bad_df = df_with_id.filter(F.col(temp_id_col).isin(bad_row_ids)) \
                .join(error_info_df, on=temp_id_col, how="left") \
                .withColumn("raw_data", F.to_json(F.struct([c for c in df.columns]))) \
                .withColumn("origin_table", F.lit(table_name)) \
                .withColumn("ingestion_time", F.current_timestamp()) \
                .select(
                    F.col("origin_table").alias("table_name"),
                    F.lit(str(batch_id)).alias("gx_batch_id"),
                    "violated_rules", "raw_data", "ingestion_time"
                )
            bad_df.write.mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)
            
            # 好数据分流入 Target
            good_df = df_with_id.filter(~F.col(temp_id_col).isin(bad_row_ids)).drop(temp_id_col)
            if good_df.limit(1).count() > 0:
                good_df.write.mode("append").option("mergeSchema", "true").saveAsTable(full_target_table)
            
            print(f"⚠️ Batch {batch_id}: Quarantined {len(set(bad_row_ids))} rows.")

    except Exception as e:
        print(f"❌ Batch {batch_id} Write Error: {str(e)}")
        # 最后的兜底：如果入库逻辑崩溃，确保数据至少保存到目标表
        df_with_id.drop(temp_id_col).write.mode("append").saveAsTable(full_target_table)
    finally:
        # 【关键】清理 Spark 内存占用
        if df_with_id.is_cached:
            df_with_id.unpersist()
        
        # 仅针对本批次做元数据清理，比 clearCache 更轻量
        del result
        gc.collect()