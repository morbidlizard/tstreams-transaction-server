package configProperties

import scala.collection.JavaConverters
import org.rocksdb._
import Config._

class RocksDBConfig(config: Config) {
  private val prefix = "rocksdb."

  //DBOptions
  private val stats_dump_period_sec = "stats_dump_period_sec"
  private val max_manifest_file_size = "max_manifest_file_size"
  private val bytes_per_sync = "bytes_per_sync"
  private val delayed_write_rate = "delayed_write_rate"
  private val WAL_ttl_seconds = "WAL_ttl_seconds"
  private val WAL_size_limit_MB = "WAL_size_limit_MB"
  private val max_subcompactions = "max_subcompactions"
  private val wal_dir = "wal_dir="
  private val wal_bytes_per_sync = "wal_bytes_per_sync"
  private val db_write_buffer_size = "db_write_buffer_size"
  private val keep_log_file_num = "keep_log_file_num"
  private val table_cache_numshardbits = "table_cache_numshardbits"
  private val max_file_opening_threads = "max_file_opening_threads"
  private val writable_file_max_buffer_size = "writable_file_max_buffer_size"
  private val random_access_max_buffer_size = "random_access_max_buffer_size"
  private val use_fsync = "use_fsync"
  private val max_total_wal_size = "max_total_wal_size"
  private val max_open_files = "max_open_files"
  private val skip_stats_update_on_db_open = "skip_stats_update_on_db_open"
  private val max_background_compactions = "max_background_compactions"
  private val manifest_preallocation_size = "manifest_preallocation_size"
  private val max_background_flushes = "max_background_flushes"
  private val is_fd_close_on_exec = "is_fd_close_on_exec"
  private val max_log_file_size = "max_log_file_size"
  private val advise_random_on_open = "advise_random_on_open"
  private val create_missing_column_families = "create_missing_column_families"
  private val paranoid_checks = "paranoid_checks"
  private val delete_obsolete_files_period_micros = "delete_obsolete_files_period_micros"
  private val disable_data_sync = "disable_data_sync"
  private val log_file_time_to_roll = "log_file_time_to_roll"
  private val compaction_readahead_size = "compaction_readahead_size"
  private val create_if_missing = "create_if_missing"
  private val use_adaptive_mutex = "use_adaptive_mutex"
  private val enable_thread_tracking = "enable_thread_tracking"
  private val disableDataSync = "disableDataSync"
  private val allow_fallocate = "allow_fallocate"
  private val error_if_exists = "error_if_exists"
  private val recycle_log_file_num = "recycle_log_file_num"
  private val skip_log_error_on_recovery = "skip_log_error_on_recovery"
  private val allow_mmap_reads = "allow_mmap_reads"
  private val allow_os_buffer = "allow_os_buffer"
  private val db_log_dir = "db_log_dir"
  private val new_table_reader_for_compaction_inputs = "new_table_reader_for_compaction_inputs"
  private val allow_mmap_writes = "allow_mmap_writes"

  //CFOptions
  private val compaction_style = "compaction_style"
  private val compaction_filter = "compaction_filter"
  private val num_levels = "num_levels"
  private val table_factory = "table_factory"
  private val comparator = "comparator"
  private val max_sequential_skip_in_iterations = "max_sequential_skip_in_iterations"
  private val soft_rate_limit = "soft_rate_limit"
  private val max_bytes_for_level_base = "max_bytes_for_level_base"
  private val memtable_prefix_bloom_probes = "memtable_prefix_bloom_probes"
  private val memtable_prefix_bloom_bits = "memtable_prefix_bloom_bits"
  private val memtable_prefix_bloom_huge_page_tlb_size = "memtable_prefix_bloom_huge_page_tlb_size"
  private val max_successive_merges = "max_successive_merges"
  private val arena_block_size = "arena_block_size"
  private val min_write_buffer_number_to_merge = "min_write_buffer_number_to_merge"
  private val target_file_size_multiplier = "target_file_size_multiplier"
  private val source_compaction_factor = "source_compaction_factor"
  private val max_bytes_for_level_multiplier = "max_bytes_for_level_multiplier"
  private val max_bytes_for_level_multiplier_additional = "max_bytes_for_level_multiplier_additional"
  private val compaction_filter_factory = "compaction_filter_factory"
  private val max_write_buffer_number = "max_write_buffer_number"
  private val level0_stop_writes_trigger = "level0_stop_writes_trigger"
  private val compression = "compression"
  private val level0_file_num_compaction_trigger = "level0_file_num_compaction_trigger"
  private val purge_redundant_kvs_while_flush = "purge_redundant_kvs_while_flush"
  private val max_write_buffer_number_to_maintain = "max_write_buffer_number_to_maintain"
  private val memtable_factory = "memtable_factory"
  private val max_grandparent_overlap_factor = "max_grandparent_overlap_factor"
  private val expanded_compaction_factor = "expanded_compaction_factor"
  private val hard_pending_compaction_bytes_limit = "hard_pending_compaction_bytes_limit"
  private val inplace_update_num_locks = "inplace_update_num_locks"
  private val level_compaction_dynamic_level_bytes = "level_compaction_dynamic_level_bytes"
  private val level0_slowdown_writes_trigger = "level0_slowdown_writes_trigger"
  private val filter_deletes = "filter_deletes"
  private val verify_checksums_in_compaction = "verify_checksums_in_compaction"
  private val min_partial_merge_operands = "min_partial_merge_operands"
  private val paranoid_file_checks = "paranoid_file_checks"
  private val target_file_size_base = "target_file_size_base"
  private val optimize_filters_for_hits = "optimize_filters_for_hits"
  private val merge_operator = "merge_operator"
  private val compression_per_level = "compression_per_level"
  private val compaction_measure_io_stats = "compaction_measure_io_stats"
  private val prefix_extractor = "prefix_extractor"
  private val bloom_locality = "bloom_locality"
  private val write_buffer_size = "write_buffer_size"
  private val disable_auto_compactions = "disable_auto_compactions"
  private val inplace_update_support = "inplace_update_support"

  val rocksDBProperties = {
    val properties = config.getAllProperties(prefix).map { case (key, value) => (key.splitAt(prefix.length)._2, value) }

//    val tableConfig = new PlainTableConfig()
//      .setKeySize(transactionService.server.transactionDataService.Key.size)
//      .setEncodingType(EncodingType.kPlain)

    val tableConfig = new BlockBasedTableConfig()

//    val env = Env.getDefault
    val options = new Options()

    val filter: PartialFunction[String, Unit] = {
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
//      case `max_background_compactions` => env.setBackgroundThreads(properties(max_background_compactions), Env.COMPACTION_POOL)
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
//      case `memtable_prefix_bloom_bits` => tableConfig.setBloomBitsPerKey(properties(memtable_prefix_bloom_bits))
//      case `memtable_prefix_bloom_huge_page_tlb_size` => tableConfig.setHugePageTlbSize(properties(memtable_prefix_bloom_huge_page_tlb_size))
      case `max_successive_merges` => options.setMaxSuccessiveMerges(properties(max_successive_merges))
      case `arena_block_size` => options.setArenaBlockSize(properties(arena_block_size))
      case `min_write_buffer_number_to_merge` => options.setMinWriteBufferNumberToMerge(properties(min_write_buffer_number_to_merge))
      case `target_file_size_multiplier` => options.setTargetFileSizeMultiplier(properties(target_file_size_multiplier))
//      case `source_compaction_factor` => options.setSourceCompactionFactor(properties(source_compaction_factor))
      case `max_bytes_for_level_multiplier` => options.setMaxBytesForLevelMultiplier(properties(max_bytes_for_level_multiplier))
      //      case `max_bytes_for_level_multiplier_additional` => options.
      //      case `compaction_filter_factory` => options.
      case `max_write_buffer_number` => options.setMaxWriteBufferNumber(properties(max_write_buffer_number))
      case `level0_stop_writes_trigger` => options.setLevelZeroStopWritesTrigger(properties(level0_stop_writes_trigger))
      case `compression` => options.setCompressionType(CompressionType.getCompressionType(properties(compression)))
      case `level0_file_num_compaction_trigger` => options.setLevelZeroFileNumCompactionTrigger(properties(level0_file_num_compaction_trigger))
      case `purge_redundant_kvs_while_flush` => options.setPurgeRedundantKvsWhileFlush(properties(purge_redundant_kvs_while_flush))
      //      case `max_write_buffer_number_to_maintain` => options.
      //      case `memtable_factory` => options.
//      case `max_grandparent_overlap_factor` => options.setMaxGrandparentOverlapFactor(properties(max_grandparent_overlap_factor))
//      case `expanded_compaction_factor` => options.setExpandedCompactionFactor(properties(expanded_compaction_factor))
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
      case `compression_per_level` => options.setCompressionPerLevel(JavaConverters.seqAsJavaList(properties(compression_per_level).split(':').map(x => CompressionType.valueOf(x))))
      //      case `compaction_measure_io_stats` => options.
      //      case `prefix_extractor` => options.
      case `bloom_locality` => options.setBloomLocality(properties(bloom_locality))
      case `write_buffer_size` => options.setWriteBufferSize(properties(write_buffer_size))
      case `disable_auto_compactions` => options.setDisableAutoCompactions(properties(disable_auto_compactions))
      case `inplace_update_support` => options.setInplaceUpdateSupport(properties(inplace_update_support))
      case _ => ()
    }
    properties foreach {case (key, _) => filter(key)}
//    options.setEnv(env).setTableFormatConfig(tableConfig)
    options
  }
}
