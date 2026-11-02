package exporter

import (
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func testModuleMetrics(t *testing.T, addr string, wantedMetrics []string) {
	t.Helper()

	for _, inclModules := range []bool{true, false} {
		t.Run(fmt.Sprintf("inclModulesMetrics:%t", inclModules), func(t *testing.T) {
			e, _ := NewRedisExporter(addr, Options{Namespace: "test", InclModulesMetrics: inclModules})

			chM := make(chan prometheus.Metric)
			go func() {
				e.Collect(chM)
				close(chM)
			}()

			found := map[string]bool{}
			for m := range chM {
				desc := m.Desc().String()
				for _, want := range wantedMetrics {
					if strings.Contains(desc, want) {
						found[want] = true
					}
				}
			}

			for _, want := range wantedMetrics {
				if inclModules && !found[want] {
					t.Errorf("%s was *not* found in Redis Modules metrics but expected", want)
				}
				if !inclModules && found[want] {
					t.Errorf("%s was *found* in Redis Modules metrics but *not* expected", want)
				}
			}
		})
	}
}

func TestModulesv80(t *testing.T) {
	if os.Getenv("TEST_REDIS8_URI") == "" {
		t.Skipf("TEST_REDIS8_URI not set - skipping")
	}

	testModuleMetrics(t, os.Getenv("TEST_REDIS8_URI"), []string{
		"module_info",
		"search_number_of_indexes",
		"search_number_of_active_indexes",
		"search_number_of_active_indexes_running_queries",
		"search_number_of_active_indexes_indexing",
		"search_total_active_write_threads",
		"search_indexing_time_ms_total",
		"search_total_num_docs_in_indexes",
	})
}

func TestModulesValkey(t *testing.T) {
	if os.Getenv("TEST_VALKEY9_BUNDLE_URI") == "" {
		t.Skipf("TEST_VALKEY9_BUNDLE_URI not set - skipping")
	}

	testModuleMetrics(t, os.Getenv("TEST_VALKEY9_BUNDLE_URI"), []string{
		"module_info",
		"search_number_of_indexes",
		"bf_bloom_total_memory_bytes",
		"bf_bloom_num_objects",
		"bf_bloom_num_filters_across_objects",
		"bf_bloom_num_items_across_objects",
		"bf_bloom_capacity_across_objects",
		"json_total_memory_bytes",
		"json_num_documents",
		"search_used_memory_bytes",
		"search_number_of_attributes",
		"search_total_indexed_documents",
		"search_query_queue_size",
		"search_writer_queue_size",
		"search_string_interning_store_size",
		"search_vector_externing_hash_extern_errors",
		"search_vector_externing_num_lru_entries",
		"bf_bloom_defrag_hits_total",
		"bf_bloom_defrag_misses_total",
		"search_worker_pool_suspend_count",
		"search_writer_resumed_count",
		"search_reader_resumed_count",
		"search_writer_suspension_expired_count",
		"search_rdb_load_success_count",
		"search_rdb_load_failure_count",
		"search_rdb_save_success_count",
		"search_rdb_save_failure_count",
		"search_successful_requests_count",
		"search_failure_requests_count",
		"search_hybrid_requests_count",
		"search_inline_filtering_requests_count",
		"search_hnsw_add_exceptions_count",
		"search_hnsw_remove_exceptions_count",
		"search_hnsw_modify_exceptions_count",
		"search_hnsw_search_exceptions_count",
		"search_hnsw_create_exceptions_count",
		"search_vector_externing_entry_count",
		"search_vector_externing_generated_value_count",
		"search_vector_externing_lru_promote_count",
		"search_vector_externing_deferred_entry_count",
	})
}

// moduleInfoConn is a minimal redis.Conn that returns a canned INFO MODULES
// reply, used to unit-test extractModulesMetrics without a live server.
type moduleInfoConn struct {
	reply any
}

func (c *moduleInfoConn) Close() error                   { return nil }
func (c *moduleInfoConn) Err() error                     { return nil }
func (c *moduleInfoConn) Do(string, ...any) (any, error) { return c.reply, nil }
func (c *moduleInfoConn) Send(string, ...any) error      { return nil }
func (c *moduleInfoConn) Flush() error                   { return nil }
func (c *moduleInfoConn) Receive() (any, error)          { return nil, nil }

// moduleCollector feeds a canned reply through extractModulesMetrics so tests
// can use a real registry. Gather fails on duplicate metrics, which a plain
// channel read would not catch. after, if set, emits metrics of other features
// once the module metrics are collected, as in a real scrape.
type moduleCollector struct {
	e     *Exporter
	reply string
	after func(e *Exporter, ch chan<- prometheus.Metric)
}

func (c moduleCollector) Describe(chan<- *prometheus.Desc) {}
func (c moduleCollector) Collect(ch chan<- prometheus.Metric) {
	c.e.extractModulesMetrics(ch, &moduleInfoConn{reply: []byte(c.reply)})
	if c.after != nil {
		c.after(c.e, ch)
	}
}

// gatherModuleMetrics returns the gathered metric families keyed by their name
// without the "test_" namespace prefix.
func gatherModuleMetrics(t *testing.T, reply string) map[string]*dto.MetricFamily {
	t.Helper()

	e, err := NewRedisExporter("", Options{Namespace: "test", InclModulesMetrics: true})
	if err != nil {
		t.Fatalf("NewRedisExporter: %v", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(moduleCollector{e: e, reply: reply})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[strings.TrimPrefix(mf.GetName(), "test_")] = mf
	}
	return out
}

func moduleReply(fields map[string]string) string {
	var b strings.Builder
	b.WriteString("# Modules\r\n")
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		b.WriteString(k + ":" + fields[k] + "\r\n")
	}
	return b.String()
}

func singleValue(mf *dto.MetricFamily) float64 {
	m := mf.GetMetric()[0]
	if mf.GetType() == dto.MetricType_COUNTER {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

func checkModuleMetric(t *testing.T, got map[string]*dto.MetricFamily, field, name string, counter bool, want float64) {
	t.Helper()
	mf, ok := got[name]
	if !ok {
		t.Errorf("%s: metric %s not exported", field, name)
		return
	}
	wantType := dto.MetricType_GAUGE
	if counter {
		wantType = dto.MetricType_COUNTER
	}
	if mf.GetType() != wantType {
		t.Errorf("%s: %s has type %s, want %s", field, name, mf.GetType(), wantType)
	}
	if v := singleValue(mf); v != want {
		t.Errorf("%s: %s = %v, want %v", field, name, v, want)
	}
}

// moduleMetricContract pins the metric name and type of module fields that
// users already rely on, whether they come from the tables or default naming.
var moduleMetricContract = []struct {
	field   string
	name    string
	counter bool
}{
	{"bf_bloom_capacity_across_objects", "bf_bloom_capacity_across_objects", false},
	{"bf_bloom_defrag_hits", "bf_bloom_defrag_hits_total", true},
	{"bf_bloom_defrag_misses", "bf_bloom_defrag_misses_total", true},
	{"bf_bloom_num_filters_across_objects", "bf_bloom_num_filters_across_objects", false},
	{"bf_bloom_num_items_across_objects", "bf_bloom_num_items_across_objects", false},
	{"bf_bloom_num_objects", "bf_bloom_num_objects", false},
	{"bf_bloom_total_memory_bytes", "bf_bloom_total_memory_bytes", false},
	{"json_num_documents", "json_num_documents", false},
	{"json_total_memory_bytes", "json_total_memory_bytes", false},
	{"search_bytes_collected", "search_collected_bytes", false},
	{"search_dialect_1", "search_dialect_1", false},
	{"search_dialect_2", "search_dialect_2", false},
	{"search_dialect_3", "search_dialect_3", false},
	{"search_dialect_4", "search_dialect_4", false},
	{"search_errors_indexing_failures", "search_errors_indexing_failures", false},
	{"search_failure_requests_count", "search_failure_requests_count", true},
	{"search_gc_bytes_collected", "search_gc_collected_bytes", false},
	{"search_gc_marked_deleted_vectors", "search_gc_marked_deleted_vectors", false},
	{"search_gc_total_cycles", "search_gc_cycles_total", true},
	{"search_gc_total_docs_not_collected", "search_gc_total_docs_not_collected", false},
	{"search_gc_total_ms_run", "search_gc_run_ms_total", true},
	{"search_global_idle", "search_global_idle", false},
	{"search_global_idle_internal", "search_global_idle_internal", false},
	{"search_global_idle_user", "search_global_idle_user", false},
	{"search_global_total", "search_global_total", false},
	{"search_global_total_internal", "search_global_total_internal", false},
	{"search_global_total_user", "search_global_total_user", false},
	{"search_hnsw_add_exceptions_count", "search_hnsw_add_exceptions_count", true},
	{"search_hnsw_create_exceptions_count", "search_hnsw_create_exceptions_count", true},
	{"search_hnsw_modify_exceptions_count", "search_hnsw_modify_exceptions_count", true},
	{"search_hnsw_remove_exceptions_count", "search_hnsw_remove_exceptions_count", true},
	{"search_hnsw_search_exceptions_count", "search_hnsw_search_exceptions_count", true},
	{"search_hybrid_requests_count", "search_hybrid_requests_count", true},
	{"search_inline_filtering_requests_count", "search_inline_filtering_requests_count", true},
	{"search_largest_memory_index", "search_largest_memory_index_bytes", false},
	{"search_number_of_active_indexes", "search_number_of_active_indexes", false},
	{"search_number_of_active_indexes_indexing", "search_number_of_active_indexes_indexing", false},
	{"search_number_of_active_indexes_running_queries", "search_number_of_active_indexes_running_queries", false},
	{"search_number_of_attributes", "search_number_of_attributes", false},
	{"search_number_of_indexes", "search_number_of_indexes", false},
	{"search_query_queue_size", "search_query_queue_size", false},
	{"search_rdb_load_failure_cnt", "search_rdb_load_failure_count", true},
	{"search_rdb_load_success_cnt", "search_rdb_load_success_count", true},
	{"search_rdb_save_failure_cnt", "search_rdb_save_failure_count", true},
	{"search_rdb_save_success_cnt", "search_rdb_save_success_count", true},
	{"search_reader_resumed_cnt", "search_reader_resumed_count", true},
	{"search_smallest_memory_index", "search_smallest_memory_index_bytes", false},
	{"search_string_interning_store_size", "search_string_interning_store_size", false},
	{"search_successful_requests_count", "search_successful_requests_count", true},
	{"search_total_active_queries", "search_active_queries_total", true},
	{"search_total_active_write_threads", "search_total_active_write_threads", false},
	{"search_total_cycles", "search_cycles_total", true},
	{"search_total_indexed_documents", "search_total_indexed_documents", false},
	{"search_total_indexing_time", "search_indexing_time_ms_total", true},
	{"search_total_ms_run", "search_run_ms_total", true},
	{"search_total_num_docs_in_indexes", "search_total_num_docs_in_indexes", false},
	{"search_total_queries_processed", "search_queries_processed_total", true},
	{"search_total_query_commands", "search_query_commands_total", true},
	{"search_total_query_execution_time_ms", "search_query_execution_time_ms_total", true},
	{"search_used_memory_bytes", "search_used_memory_bytes", false},
	{"search_used_memory_indexes", "search_used_memory_indexes_bytes", false},
	{"search_used_memory_vector_index", "search_used_memory_vector_index_bytes", false},
	{"search_vector_externing_deferred_entry_cnt", "search_vector_externing_deferred_entry_count", true},
	{"search_vector_externing_entry_count", "search_vector_externing_entry_count", true},
	{"search_vector_externing_generated_value_cnt", "search_vector_externing_generated_value_count", true},
	{"search_vector_externing_hash_extern_errors", "search_vector_externing_hash_extern_errors", false},
	{"search_vector_externing_lru_promote_cnt", "search_vector_externing_lru_promote_count", true},
	{"search_vector_externing_num_lru_entries", "search_vector_externing_num_lru_entries", false},
	{"search_worker_pool_suspend_cnt", "search_worker_pool_suspend_count", true},
	{"search_writer_queue_size", "search_writer_queue_size", false},
	{"search_writer_resumed_cnt", "search_writer_resumed_count", true},
	{"search_writer_suspension_expired_cnt", "search_writer_suspension_expired_count", true},
}

func TestModulesMetricContract(t *testing.T) {
	fields := map[string]string{}
	for _, c := range moduleMetricContract {
		fields[c.field] = "7"
	}
	got := gatherModuleMetrics(t, moduleReply(fields))

	for _, c := range moduleMetricContract {
		checkModuleMetric(t, got, c.field, c.name, c.counter, 7)
	}
	if len(got) != len(moduleMetricContract) {
		t.Errorf("got %d metrics, want %d", len(got), len(moduleMetricContract))
	}
}

// Every rename and type entry must change the default; an entry that matches it
// is redundant.
func TestModulesOverridesDifferFromDefault(t *testing.T) {
	for field, name := range moduleMetricRenames {
		if name == sanitizeMetricName(field) {
			t.Errorf("rename %s -> %s matches the sanitized field name; remove it", field, name)
		}
	}
	for field, valueType := range moduleMetricTypes {
		if (valueType == prometheus.CounterValue) == isCounterFieldName(moduleMetricName(field)) {
			t.Errorf("type of %s matches its name suffix; remove it", field)
		}
	}
}

// Fields without an override get their sanitized name and suffix type; string
// values and *_human fields are skipped. Field names are from valkey-search.
func TestModulesDefaultNaming(t *testing.T) {
	got := gatherModuleMetrics(t, moduleReply(map[string]string{
		"search_vector_requests_count":    "11",
		"search_nonvector_requests_count": "4",
		"search_index_reclaimable_memory": "2048",
		"search_used_read_cpu":            "0.25",
		"search_num_cursors":              "1",
		// skipped: string values
		"search_hnsw_vector_index_search_latency_usec": "p50=1.000,p99=2.000,p99.9=3.000",
		"search_used_memory_human":                     "1.50M",
		"search_background_indexing_status":            "NO_ACTIVITY",
		// skipped: RediSearch reports *_human as plain MiB numbers
		"search_used_memory_indexes_human": "0.0079469680786132812",
	}))

	for _, c := range []struct {
		field   string
		counter bool
		want    float64
	}{
		{"search_vector_requests_count", true, 11},
		{"search_nonvector_requests_count", true, 4},
		{"search_index_reclaimable_memory", false, 2048},
		{"search_used_read_cpu", false, 0.25},
		{"search_num_cursors", false, 1},
	} {
		checkModuleMetric(t, got, c.field, c.field, c.counter, c.want)
	}
	if len(got) != 5 {
		t.Errorf("got %v, want only the 5 numeric fields", slices.Sorted(maps.Keys(got)))
	}
}

// The suffix heuristic is wrong for some fields. Known current sizes are fixed by
// moduleMetricTypes; the remaining cases document the limitation.
func TestModulesTypeInferenceEdgeCases(t *testing.T) {
	got := gatherModuleMetrics(t, moduleReply(map[string]string{
		"search_vector_registry_entry_cnt":           "100",
		"search_vector_registry_pending_unshare_cnt": "3",
		"search_coordinator_bytes_in":                "4096",
		"search_compatibility-tag_rejects_non_utf8":  "2",
		"graph_node_count":                           "50",
	}))

	// Current sizes with a counter suffix, fixed by moduleMetricTypes.
	checkModuleMetric(t, got, "search_vector_registry_entry_cnt", "search_vector_registry_entry_cnt", false, 100)
	checkModuleMetric(t, got, "search_vector_registry_pending_unshare_cnt", "search_vector_registry_pending_unshare_cnt", false, 3)
	// Cumulative values without a counter suffix, exported as gauges.
	checkModuleMetric(t, got, "search_coordinator_bytes_in", "search_coordinator_bytes_in", false, 4096)
	// The hyphen is sanitized to an underscore.
	checkModuleMetric(t, got, "search_compatibility-tag_rejects_non_utf8", "search_compatibility_tag_rejects_non_utf8", false, 2)
	// An unchecked current size with a counter suffix, exported as a counter.
	checkModuleMetric(t, got, "graph_node_count", "graph_node_count", true, 50)
}

func TestModulesNameCollisions(t *testing.T) {
	t.Run("override output wins over a field with the same name", func(t *testing.T) {
		// bf_bloom_defrag_hits has an override to bf_bloom_defrag_hits_total.
		got := gatherModuleMetrics(t, moduleReply(map[string]string{
			"bf_bloom_defrag_hits":       "1",
			"bf_bloom_defrag_hits_total": "2",
		}))
		checkModuleMetric(t, got, "bf_bloom_defrag_hits", "bf_bloom_defrag_hits_total", true, 1)
		if len(got) != 1 {
			t.Errorf("got %d metrics, want 1", len(got))
		}
	})

	t.Run("fields that sanitize to the same name", func(t *testing.T) {
		got := gatherModuleMetrics(t, "# Modules\r\nacme-x:1\r\nacme_x:2\r\n")
		checkModuleMetric(t, got, "acme-x", "acme_x", false, 1)
		if len(got) != 1 {
			t.Errorf("got %d metrics, want 1", len(got))
		}
	})

	// search_index_num_docs{index_name} belongs to --include-search-indexes-metrics
	// and shares valkey-search's search_index_ prefix. The second scrape matters: a
	// collision would also corrupt the cached description.
	for _, role := range []bool{false, true} {
		t.Run(fmt.Sprintf("metric of another feature, instance_role=%t", role), func(t *testing.T) {
			e, err := NewRedisExporter("", Options{Namespace: "test", InclModulesMetrics: true, AppendInstanceRoleLabel: role})
			if err != nil {
				t.Fatalf("NewRedisExporter: %v", err)
			}
			reg := prometheus.NewRegistry()
			reg.MustRegister(moduleCollector{e: e, reply: "# M\r\nsearch_index_num_docs:9\r\n", after: func(e *Exporter, ch chan<- prometheus.Metric) {
				e.registerConstMetricGauge(ch, "search_index_num_docs", 42, "idx1")
			}})
			for scrape := 1; scrape <= 2; scrape++ {
				mfs, err := reg.Gather()
				if err != nil {
					t.Fatalf("scrape %d: Gather: %v", scrape, err)
				}
				if len(mfs) != 1 || mfs[0].GetMetric()[0].GetGauge().GetValue() != 42 {
					t.Errorf("scrape %d: want only search_index_num_docs{index_name=\"idx1\"} = 42, got %v", scrape, mfs)
				}
			}
		})
	}
}

// valkey-ldap reports a dictionary per LDAP server (add_ldap_status_section in
// the valkey-ldap source). Valkey prefixes field names with the module name.
// The values are not numeric, so nothing besides module_info is exported.
func TestModulesLdapNotExported(t *testing.T) {
	got := gatherModuleMetrics(t, "# ldap_status\r\n"+
		"module:name=ldap,ver=10000,api=1,filters=0,usedby=[],using=[],options=[]\r\n"+
		"ldap_server_0:host=ldap,status=healthy,ping_time_ms=1.234\r\n"+
		"ldap_server_1:host=ldap-2,status=unhealthy,error=connection refused\r\n")

	if _, ok := got["module_info"]; !ok {
		t.Errorf("module_info not exported")
	}
	if len(got) != 1 {
		t.Errorf("got %v, want only module_info", slices.Sorted(maps.Keys(got)))
	}
}

// Counters inferred from the name only accept finite values >= 0; gauges accept
// everything parseMetricValue accepts.
func TestModulesCounterValueValidation(t *testing.T) {
	values := []string{"-5", "nan", "inf", "-inf", "ok", "false", "0", "12"}
	fields := map[string]string{}
	for i, v := range values {
		fields[fmt.Sprintf("acme_v%d_total", i)] = v
		fields[fmt.Sprintf("acme_v%d_size", i)] = v
	}
	got := gatherModuleMetrics(t, moduleReply(fields))

	for i, v := range values {
		counter := fmt.Sprintf("acme_v%d_total", i)
		_, exported := got[counter]
		wantExported := v == "0" || v == "12"
		if exported != wantExported {
			t.Errorf("counter %s=%q: exported=%t, want %t", counter, v, exported, wantExported)
		}
		if wantExported {
			want, _ := strconv.ParseFloat(v, 64)
			checkModuleMetric(t, got, counter, counter, true, want)
		}

		gauge := fmt.Sprintf("acme_v%d_size", i)
		mf, ok := got[gauge]
		if !ok {
			t.Errorf("gauge %s=%q not exported", gauge, v)
			continue
		}
		want, err := parseMetricValue(v)
		if err != nil {
			t.Fatalf("parseMetricValue(%q): %v", v, err)
		}
		gv := singleValue(mf)
		if mf.GetType() != dto.MetricType_GAUGE || !(gv == want || (math.IsNaN(gv) && math.IsNaN(want))) {
			t.Errorf("gauge %s=%q: got %s %v, want GAUGE %v", gauge, v, mf.GetType(), gv, want)
		}
	}
}

// Core INFO rules must not apply to module fields: neither core mappings
// (evicted_keys is exported by core INFO as evicted_keys_total) nor the db,
// cmdstat_ and cluster_ prefixes that includeMetric accepts.
func TestModulesIgnoreCoreInfoRules(t *testing.T) {
	got := gatherModuleMetrics(t, moduleReply(map[string]string{
		"evicted_keys":         "5",
		"dbtools_hits_total":   "5",
		"cluster_x_ops_count":  "-1",
		"cmdstat_y_queue_size": "3",
	}))
	checkModuleMetric(t, got, "evicted_keys", "evicted_keys", false, 5)
	checkModuleMetric(t, got, "dbtools_hits_total", "dbtools_hits_total", true, 5)
	checkModuleMetric(t, got, "cmdstat_y_queue_size", "cmdstat_y_queue_size", false, 3)
	if len(got) != 3 {
		t.Errorf("got %v, want evicted_keys, dbtools_hits_total and cmdstat_y_queue_size", slices.Sorted(maps.Keys(got)))
	}
}
