package configProperties

import org.rocksdb._
import Config._

object RocksDBConfig {
  val config = ServerConfig.config

  val prefix = "rocksdb."

  //DBOptions
  val stats_dump_period_sec = "stats_dump_period_sec"
  val max_manifest_file_size = "max_manifest_file_size"
  val bytes_per_sync = "bytes_per_sync"
  val delayed_write_rate = "delayed_write_rate"
  val WAL_ttl_seconds = "WAL_ttl_seconds"
  val WAL_size_limit_MB = "WAL_size_limit_MB"
  val max_subcompactions = "max_subcompactions"
  val wal_dir = "wal_dir="
  val wal_bytes_per_sync = "wal_bytes_per_sync"
  val db_write_buffer_size = "db_write_buffer_size"
  val keep_log_file_num = "keep_log_file_num"
  val table_cache_numshardbits = "table_cache_numshardbits"
  val max_file_opening_threads = "max_file_opening_threads"
  val writable_file_max_buffer_size = "writable_file_max_buffer_size"
  val random_access_max_buffer_size = "random_access_max_buffer_size"
  val use_fsync = "use_fsync"
  val max_total_wal_size = "max_total_wal_size"
  val max_open_files = "max_open_files"
  val skip_stats_update_on_db_open = "skip_stats_update_on_db_open"
  val max_background_compactions = "max_background_compactions"
  val manifest_preallocation_size = "manifest_preallocation_size"
  val max_background_flushes = "max_background_flushes"
  val is_fd_close_on_exec = "is_fd_close_on_exec"
  val max_log_file_size = "max_log_file_size"
  val advise_random_on_open = "advise_random_on_open"
  val create_missing_column_families = "create_missing_column_families"
  val paranoid_checks = "paranoid_checks"
  val delete_obsolete_files_period_micros = "delete_obsolete_files_period_micros"
  val disable_data_sync = "disable_data_sync"
  val log_file_time_to_roll = "log_file_time_to_roll"
  val compaction_readahead_size = "compaction_readahead_size"
  val create_if_missing = "create_if_missing"
  val use_adaptive_mutex = "use_adaptive_mutex"
  val enable_thread_tracking = "enable_thread_tracking"
  val disableDataSync = "disableDataSync"
  val allow_fallocate = "allow_fallocate"
  val error_if_exists = "error_if_exists"
  val recycle_log_file_num = "recycle_log_file_num"
  val skip_log_error_on_recovery = "skip_log_error_on_recovery"
  val allow_mmap_reads = "allow_mmap_reads"
  val allow_os_buffer = "allow_os_buffer"
  val db_log_dir = "db_log_dir"
  val new_table_reader_for_compaction_inputs = "new_table_reader_for_compaction_inputs"
  val allow_mmap_writes = "allow_mmap_writes"

  //CFOptions
  val compaction_style = "compaction_style"
  val compaction_filter = "compaction_filter"
  val num_levels = "num_levels"
  val table_factory = "table_factory"
  val comparator = "comparator"
  val max_sequential_skip_in_iterations = "max_sequential_skip_in_iterations"
  val soft_rate_limit = "soft_rate_limit"
  val max_bytes_for_level_base = "max_bytes_for_level_base"
  val memtable_prefix_bloom_probes = "memtable_prefix_bloom_probes"
  val memtable_prefix_bloom_bits = "memtable_prefix_bloom_bits"
  val memtable_prefix_bloom_huge_page_tlb_size = "memtable_prefix_bloom_huge_page_tlb_size"
  val max_successive_merges = "max_successive_merges"
  val arena_block_size = "arena_block_size"
  val min_write_buffer_number_to_merge = "min_write_buffer_number_to_merge"
  val target_file_size_multiplier = "target_file_size_multiplier"
  val source_compaction_factor = "source_compaction_factor"
  val max_bytes_for_level_multiplier = "max_bytes_for_level_multiplier"
  val max_bytes_for_level_multiplier_additional = "max_bytes_for_level_multiplier_additional"
  val compaction_filter_factory = "compaction_filter_factory"
  val max_write_buffer_number = "max_write_buffer_number"
  val level0_stop_writes_trigger = "level0_stop_writes_trigger"
  val compression = "compression"
  val level0_file_num_compaction_trigger = "level0_file_num_compaction_trigger"
  val purge_redundant_kvs_while_flush = "purge_redundant_kvs_while_flush"
  val max_write_buffer_number_to_maintain = "max_write_buffer_number_to_maintain"
  val memtable_factory = "memtable_factory"
  val max_grandparent_overlap_factor = "max_grandparent_overlap_factor"
  val expanded_compaction_factor = "expanded_compaction_factor"
  val hard_pending_compaction_bytes_limit = "hard_pending_compaction_bytes_limit"
  val inplace_update_num_locks = "inplace_update_num_locks"
  val level_compaction_dynamic_level_bytes = "level_compaction_dynamic_level_bytes"
  val level0_slowdown_writes_trigger = "level0_slowdown_writes_trigger"
  val filter_deletes = "filter_deletes"
  val verify_checksums_in_compaction = "verify_checksums_in_compaction"
  val min_partial_merge_operands = "min_partial_merge_operands"
  val paranoid_file_checks = "paranoid_file_checks"
  val target_file_size_base = "target_file_size_base"
  val optimize_filters_for_hits = "optimize_filters_for_hits"
  val merge_operator = "merge_operator"
  val compression_per_level = "compression_per_level"
  val compaction_measure_io_stats = "compaction_measure_io_stats"
  val prefix_extractor = "prefix_extractor"
  val bloom_locality = "bloom_locality"
  val write_buffer_size = "write_buffer_size"
  val disable_auto_compactions = "disable_auto_compactions"
  val inplace_update_support = "inplace_update_support"

  import scala.collection.JavaConversions._
  val rocksDBProperties = {
    val properties = config.getAllProperties(prefix).map { case (key, value) => (key.splitAt(prefix.length)._2, value) }
    val options = new Options()
    val filter: PartialFunction[String, Options] = {
      //DBOptions
      case `stats_dump_period_sec` => options.setStatsDumpPeriodSec(properties(stats_dump_period_sec))
      case `max_manifest_file_size` => options.setMaxManifestFileSize(properties(max_manifest_file_size))
      case `bytes_per_sync` => options.setBytesPerSync(properties(bytes_per_sync))
      //      case `delayed_write_rate` => options.
      case `WAL_ttl_seconds` => options.setWalTtlSeconds(properties(WAL_ttl_seconds))
      case `WAL_size_limit_MB` => options.setWalSizeLimitMB(properties(WAL_size_limit_MB))
      //      case `max_subcompactions` => options.
      case `wal_dir` => options.setWalDir(properties(wal_dir))
      //      case `wal_bytes_per_sync` => options.
      case `db_write_buffer_size` => options.setWriteBufferSize(properties(db_write_buffer_size))
      case `keep_log_file_num` => options.setKeepLogFileNum(properties(keep_log_file_num))
      case `table_cache_numshardbits` => options.setTableCacheNumshardbits(properties(table_cache_numshardbits))
      //      case `max_file_opening_threads` => options
      case `writable_file_max_buffer_size` => options.setMaxWriteBufferNumber(properties(writable_file_max_buffer_size))
      //      case `random_access_max_buffer_size` => options.
      case `use_fsync` => options.setUseFsync(properties(use_fsync))
      case `max_total_wal_size` => options.setMaxTotalWalSize(properties(max_total_wal_size))
      case `max_open_files` => options.setMaxOpenFiles(properties(max_open_files))
      //      case `skip_stats_update_on_db_open` => options.
      case `max_background_compactions` => options.setMaxBackgroundCompactions(properties(max_background_compactions))
      case `manifest_preallocation_size` => options.setManifestPreallocationSize(properties(manifest_preallocation_size))
      case `max_background_flushes` => options.setMaxBackgroundFlushes(properties(max_background_flushes))
      case `is_fd_close_on_exec` => options.setIsFdCloseOnExec(properties(is_fd_close_on_exec))
      case `max_log_file_size` => options.setMaxLogFileSize(properties(max_log_file_size))
      case `advise_random_on_open` => options.setAdviseRandomOnOpen(properties(advise_random_on_open))
      case `create_missing_column_families` => options.setCreateMissingColumnFamilies(properties(create_missing_column_families))
      case `paranoid_checks` => options.setParanoidChecks(properties(paranoid_checks))
      case `delete_obsolete_files_period_micros` => options.setDeleteObsoleteFilesPeriodMicros(properties(delete_obsolete_files_period_micros))
      case `disable_data_sync` => options.setDisableDataSync(properties(disable_data_sync))
      case `log_file_time_to_roll` => options.setLogFileTimeToRoll(properties(log_file_time_to_roll))
      //      case `compaction_readahead_size` => options.
      case `create_if_missing` => options.setCreateIfMissing(properties(create_if_missing))
      case `use_adaptive_mutex` => options.setUseAdaptiveMutex(properties(use_adaptive_mutex))
      //      case `enable_thread_tracking` => options.
      case `disableDataSync` => options.setDisableDataSync(properties(disableDataSync))
      //      case `allow_fallocate` => options.opt
      case `error_if_exists` => options.setErrorIfExists(properties(error_if_exists))
      //      case `recycle_log_file_num` => options.
      //      case `skip_log_error_on_recovery` => options.
      case `allow_mmap_reads` => options.setAllowMmapReads(properties(allow_mmap_reads))
      case `allow_os_buffer` => options.setAllowOsBuffer(properties(allow_os_buffer))
      case `db_log_dir` => options.setDbLogDir(properties(db_log_dir))
      //      case `new_table_reader_for_compaction_inputs` => options.
      case `allow_mmap_writes` => options.setAllowMmapWrites(properties(allow_mmap_writes))

      //CFOptions
      case `compaction_style` => options.setCompactionStyle(CompactionStyle.valueOf(properties(compaction_style)))
      //      case `compaction_filter` => options.
      case `num_levels` => options.setNumLevels(properties(num_levels))
      //      case `table_factory` => options.
      case `comparator` => options.setComparator(BuiltinComparator.valueOf(properties(comparator)))
      case `max_sequential_skip_in_iterations` => options.setMaxSequentialSkipInIterations(properties(max_sequential_skip_in_iterations))
      case `soft_rate_limit` => options.setSoftRateLimit(properties(soft_rate_limit))
      case `max_bytes_for_level_base` => options.setMaxBytesForLevelBase(properties(max_bytes_for_level_base))
      //      case `memtable_prefix_bloom_probes` => options.setMe
      //      case `memtable_prefix_bloom_bits` => options.
      //      case `memtable_prefix_bloom_huge_page_tlb_size` =>options.
      case `max_successive_merges` => options.setMaxSuccessiveMerges(properties(max_successive_merges))
      case `arena_block_size` => options.setArenaBlockSize(properties(arena_block_size))
      case `min_write_buffer_number_to_merge` => options.setMinWriteBufferNumberToMerge(properties(min_write_buffer_number_to_merge))
      case `target_file_size_multiplier` => options.setTargetFileSizeMultiplier(properties(target_file_size_multiplier))
      case `source_compaction_factor` => options.setSourceCompactionFactor(properties(source_compaction_factor))
      case `max_bytes_for_level_multiplier` => options.setMaxBytesForLevelMultiplier(properties(max_bytes_for_level_multiplier))
      //      case `max_bytes_for_level_multiplier_additional` => options.
      //      case `compaction_filter_factory` => options.
      case `max_write_buffer_number` => options.setMaxWriteBufferNumber(properties(max_write_buffer_number))
      case `level0_stop_writes_trigger` => options.setLevelZeroStopWritesTrigger(properties(level0_stop_writes_trigger))
      case `compression` => options.setCompressionType(CompressionType.valueOf(properties(compression)))
      case `level0_file_num_compaction_trigger` => options.setLevelZeroFileNumCompactionTrigger(properties(level0_file_num_compaction_trigger))
      case `purge_redundant_kvs_while_flush` => options.setPurgeRedundantKvsWhileFlush(properties(purge_redundant_kvs_while_flush))
      //      case `max_write_buffer_number_to_maintain` => options.
      //      case `memtable_factory` => options.
      case `max_grandparent_overlap_factor` => options.setMaxGrandparentOverlapFactor(properties(max_grandparent_overlap_factor))
      case `expanded_compaction_factor` => options.setExpandedCompactionFactor(properties(expanded_compaction_factor))
      //      case `hard_pending_compaction_bytes_limit` => options.
      case `inplace_update_num_locks` => options.setInplaceUpdateNumLocks(properties(inplace_update_num_locks))
      case `level_compaction_dynamic_level_bytes` => options.setLevelCompactionDynamicLevelBytes(properties(level_compaction_dynamic_level_bytes))
      case `level0_slowdown_writes_trigger` => options.setLevelZeroSlowdownWritesTrigger(properties(level0_slowdown_writes_trigger))
      //      case `filter_deletes` => options.
      case `verify_checksums_in_compaction` => options.setVerifyChecksumsInCompaction(properties(verify_checksums_in_compaction))
      case `min_partial_merge_operands` => options.setMinPartialMergeOperands(properties(min_partial_merge_operands))
      case `paranoid_file_checks` => options.setParanoidChecks(properties(paranoid_file_checks))
      case `target_file_size_base` => options.setTargetFileSizeBase(properties(target_file_size_base))
      case `optimize_filters_for_hits` => options.setOptimizeFiltersForHits(properties(optimize_filters_for_hits))
      case `merge_operator` => options.setMergeOperatorName(properties(merge_operator))
      case `compression_per_level` => options.setCompressionPerLevel(properties(compression_per_level).split(':').map(x => CompressionType.valueOf(x)).toList)
      //      case `compaction_measure_io_stats` => options.
      //      case `prefix_extractor` => options.
      case `bloom_locality` => options.setBloomLocality(properties(bloom_locality))
      case `write_buffer_size` => options.setWriteBufferSize(properties(write_buffer_size))
      case `disable_auto_compactions` => options.setDisableAutoCompactions(properties(disable_auto_compactions))
      case `inplace_update_support` => options.setInplaceUpdateSupport(properties(inplace_update_support))
    }
    properties foreach {case (key, _) => filter(key)}
    options
  }
}
