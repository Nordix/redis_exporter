package exporter

import (
	"math"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// moduleMetricRenames maps INFO MODULES fields to the metric name they are
// exported as, instead of their sanitized field name.
var moduleMetricRenames = map[string]string{
	// RediSearch module
	"search_used_memory_indexes": "search_used_memory_indexes_bytes",
	"search_total_indexing_time": "search_indexing_time_ms_total",
	// Legacy redis-stack v7.4 metrics
	"search_bytes_collected": "search_collected_bytes",
	"search_total_cycles":    "search_cycles_total",
	"search_total_ms_run":    "search_run_ms_total",
	// RediSearch module v8.0
	"search_smallest_memory_index":         "search_smallest_memory_index_bytes",
	"search_largest_memory_index":          "search_largest_memory_index_bytes",
	"search_used_memory_vector_index":      "search_used_memory_vector_index_bytes",
	"search_gc_bytes_collected":            "search_gc_collected_bytes", // search_bytes_collected was renamed in https://github.com/RediSearch/RediSearch/pull/5616
	"search_gc_total_cycles":               "search_gc_cycles_total",    // search_gc metrics were renamed
	"search_gc_total_ms_run":               "search_gc_run_ms_total",    // in PR: https://github.com/RediSearch/RediSearch/pull/5616
	"search_total_queries_processed":       "search_queries_processed_total",
	"search_total_query_commands":          "search_query_commands_total",
	"search_total_query_execution_time_ms": "search_query_execution_time_ms_total",
	"search_total_active_queries":          "search_active_queries_total",
	// Valkey v8 metrics
	"bf_bloom_defrag_hits":                        "bf_bloom_defrag_hits_total",
	"bf_bloom_defrag_misses":                      "bf_bloom_defrag_misses_total",
	"search_worker_pool_suspend_cnt":              "search_worker_pool_suspend_count",
	"search_writer_resumed_cnt":                   "search_writer_resumed_count",
	"search_reader_resumed_cnt":                   "search_reader_resumed_count",
	"search_writer_suspension_expired_cnt":        "search_writer_suspension_expired_count",
	"search_rdb_load_success_cnt":                 "search_rdb_load_success_count",
	"search_rdb_load_failure_cnt":                 "search_rdb_load_failure_count",
	"search_rdb_save_success_cnt":                 "search_rdb_save_success_count",
	"search_rdb_save_failure_cnt":                 "search_rdb_save_failure_count",
	"search_vector_externing_generated_value_cnt": "search_vector_externing_generated_value_count",
	"search_vector_externing_lru_promote_cnt":     "search_vector_externing_lru_promote_count",
	"search_vector_externing_deferred_entry_cnt":  "search_vector_externing_deferred_entry_count",
}

// moduleMetricTypes sets the type of INFO MODULES fields whose name suffix
// suggests the wrong one.
var moduleMetricTypes = map[string]prometheus.ValueType{
	"search_global_total": prometheus.GaugeValue, // open cursors
	// valkey-search: current sizes, not cumulative counts
	"search_vector_registry_entry_cnt":           prometheus.GaugeValue,
	"search_vector_registry_pending_unshare_cnt": prometheus.GaugeValue,
	"search_vector_registry_chunk_count":         prometheus.GaugeValue,
}

func (e *Exporter) extractModulesMetrics(ch chan<- prometheus.Metric, c redis.Conn) {
	info, err := redis.String(doRedisCmd(c, "INFO", "MODULES"))
	if err != nil {
		log.Errorf("extractSearchMetrics() err: %s", err)
		return
	}

	exportedNames := map[string]bool{}
	lines := strings.SplitSeq(info, "\r\n")
	for line := range lines {
		log.Debugf("info: %s", line)

		split := strings.Split(line, ":")
		if len(split) != 2 {
			continue
		}

		if split[0] == "module" {
			// module format: 'module:name=<module-name>,ver=21005,api=1,filters=0,usedby=[],using=[],options=[]'
			module := strings.Split(split[1], ",")
			if len(module) != 7 {
				continue
			}
			extractModuleVal := func(s string) string {
				parts := strings.SplitN(s, "=", 2)
				if len(parts) != 2 {
					return ""
				}
				return parts[1]
			}
			e.registerConstMetricGauge(ch, "module_info", 1,
				extractModuleVal(module[0]),
				extractModuleVal(module[1]),
				extractModuleVal(module[2]),
				extractModuleVal(module[3]),
				extractModuleVal(module[4]),
				extractModuleVal(module[5]),
			)
			continue
		}

		fieldKey := split[0]
		fieldValue := split[1]

		e.registerModuleMetric(ch, fieldKey, fieldValue, exportedNames)
	}
}

// registerModuleMetric exports a module INFO field under its renamed or
// sanitized name, typed by moduleMetricTypes or the name suffix. Counters must
// be finite and >= 0, since bad samples corrupt rate(). A field without a table
// entry is skipped if its name is reserved or already exported in this reply.
func (e *Exporter) registerModuleMetric(ch chan<- prometheus.Metric, fieldKey, fieldValue string, exported map[string]bool) {
	_, renamed := moduleMetricRenames[fieldKey]
	valueType, typed := moduleMetricTypes[fieldKey]
	metricName := moduleMetricName(fieldKey)

	// Skip *_human fields that duplicate another field in human-readable form.
	if strings.HasSuffix(metricName, "_human") {
		return
	}

	if !typed {
		valueType = prometheus.GaugeValue
		if isCounterFieldName(metricName) {
			valueType = prometheus.CounterValue
		}
	}

	var val float64
	var err error
	if valueType == prometheus.CounterValue {
		val, err = strconv.ParseFloat(fieldValue, 64)
		if err == nil && (val < 0 || math.IsNaN(val) || math.IsInf(val, 1)) {
			log.Debugf("skipping module field %s: %q is not a valid counter value", fieldKey, fieldValue)
			return
		}
	} else {
		val, err = parseMetricValue(fieldValue)
	}
	if err != nil {
		return
	}

	if e.reservedMetricNames[metricName] && !renamed && !typed {
		log.Debugf("skipping module field %s: %s is used by another metric", fieldKey, metricName)
		return
	}
	if exported[metricName] {
		log.Debugf("skipping module field %s: %s was already exported", fieldKey, metricName)
		return
	}
	exported[metricName] = true

	e.registerConstMetric(ch, metricName, val, valueType)
}

// moduleMetricName returns the metric name a module field is exported as.
func moduleMetricName(fieldKey string) string {
	if name, ok := moduleMetricRenames[fieldKey]; ok {
		return name
	}
	return sanitizeMetricName(fieldKey)
}

// isCounterFieldName reports whether a sanitized field name looks like a
// monotonic counter based on its suffix. This is a heuristic: an override is
// needed where it is wrong.
func isCounterFieldName(name string) bool {
	for _, suffix := range []string{"_total", "_count", "_cnt"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// buildReservedMetricNames returns the metric names that module fields must not
// use: map outputs, module table entries and the startup description table. Must run after
// metricDescriptions has been populated.
func (e *Exporter) buildReservedMetricNames() map[string]bool {
	names := map[string]bool{}
	for _, m := range []map[string]string{e.metricMapGauges, e.metricMapCounters} {
		for _, name := range m {
			names[name] = true
		}
	}
	for _, name := range moduleMetricRenames {
		names[name] = true
	}
	for field := range moduleMetricTypes {
		names[moduleMetricName(field)] = true
	}
	for name := range e.metricDescriptions {
		names[name] = true
	}
	return names
}
