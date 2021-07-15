package carbon2

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/serializers/carbon2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	testcases := []struct {
		name       string
		input      []byte
		wantErr    bool
		wantedFunc func() []telegraf.Metric
	}{
		{
			name:  "basic1",
			input: []byte("className=HealthTrackerKafkaDataQueueWriter cluster=open-receiver deployment=nite fullClassName=com.sumologic.health.io.HealthTrackerKafkaDataQueueWriter metric=kafka.queue.alpha_health_tracker_incidents.offer.timer mtype=count node=nite-open-receiver-1 service=open-receiver stat=p75  _primaryMetricType=carbon 0.00 1625855958"),
			wantedFunc: func() []telegraf.Metric {
				tags := map[string]string{
					"className":     "HealthTrackerKafkaDataQueueWriter",
					"cluster":       "open-receiver",
					"deployment":    "nite",
					"fullClassName": "com.sumologic.health.io.HealthTrackerKafkaDataQueueWriter",
					"mtype":         "count",
					"node":          "nite-open-receiver-1",
					"service":       "open-receiver",
					"stat":          "p75",
					// meta tags
					"_primaryMetricType": "carbon",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 0.0,
				}

				return []telegraf.Metric{
					metric.New(
						"kafka.queue.alpha_health_tracker_incidents.offer.timer",
						tags,
						fields,
						time.Unix(1625855958, 0),
						telegraf.Gauge,
					),
				}
			},
		},
		{
			name:  "basic2",
			input: []byte("className=KafkaDataQueueWriter cluster=open-receiver deployment=nite fullClassName=com.sumologic.interchange.kafka.queue.KafkaDataQueueWriter metric=kafka.queue.beta_trace_ingest_traces.offer.timer mtype=count node=nite-open-receiver-1 service=open-receiver stat=p75  _primaryMetricType=carbon 0.00 1625855958"),
			wantedFunc: func() []telegraf.Metric {
				tags := map[string]string{
					"className":          "KafkaDataQueueWriter",
					"cluster":            "open-receiver",
					"deployment":         "nite",
					"fullClassName":      "com.sumologic.interchange.kafka.queue.KafkaDataQueueWriter",
					"mtype":              "count",
					"node":               "nite-open-receiver-1",
					"service":            "open-receiver",
					"stat":               "p75",
					"_primaryMetricType": "carbon",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 0.0,
				}

				return []telegraf.Metric{
					metric.New(
						"kafka.queue.beta_trace_ingest_traces.offer.timer",
						tags,
						fields,
						time.Unix(1625855958, 0),
						telegraf.Gauge,
					),
				}
			},
		},
		{
			name:  "basic3",
			input: []byte("_rawName=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max cluster=open-receiver deployment=nite metric=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max node=nite-open-receiver-1 service=health  _1=nite _2=nite-open-receiver-1 _3=health _4=jmx _5=memoryUsage _6=pools _7=Compressed-Class-Space _8=max name=Compressed-Class-Space 1073741824 1625855945"),
			wantedFunc: func() []telegraf.Metric {
				tags := map[string]string{
					"_rawName":   "nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
					"cluster":    "open-receiver",
					"deployment": "nite",
					"node":       "nite-open-receiver-1",
					"service":    "health",
					// meta tags
					"_1":   "nite",
					"_2":   "nite-open-receiver-1",
					"_3":   "health",
					"_4":   "jmx",
					"_5":   "memoryUsage",
					"_6":   "pools",
					"_7":   "Compressed-Class-Space",
					"_8":   "max",
					"name": "Compressed-Class-Space",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 1073741824,
				}

				return []telegraf.Metric{
					metric.New(
						"nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
						tags,
						fields,
						time.Unix(1625855945, 0),
						telegraf.Gauge,
					),
				}
			},
		},
		{
			name:  "basic3_with_new_line_at_the_end",
			input: []byte("_rawName=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max cluster=open-receiver deployment=nite metric=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max node=nite-open-receiver-1 service=health  _1=nite _2=nite-open-receiver-1 _3=health _4=jmx _5=memoryUsage _6=pools _7=Compressed-Class-Space _8=max name=Compressed-Class-Space 1073741824 1625855945\n"),
			wantedFunc: func() []telegraf.Metric {
				tags := map[string]string{
					"_rawName":   "nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
					"cluster":    "open-receiver",
					"deployment": "nite",
					"node":       "nite-open-receiver-1",
					"service":    "health",
					// meta tags
					"_1":   "nite",
					"_2":   "nite-open-receiver-1",
					"_3":   "health",
					"_4":   "jmx",
					"_5":   "memoryUsage",
					"_6":   "pools",
					"_7":   "Compressed-Class-Space",
					"_8":   "max",
					"name": "Compressed-Class-Space",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 1073741824,
				}

				return []telegraf.Metric{
					metric.New(
						"nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
						tags,
						fields,
						time.Unix(1625855945, 0),
						telegraf.Gauge,
					),
				}
			},
		},
		{
			name: "multiple_metrics",
			input: []byte(`_rawName=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-1.max cluster=open-receiver deployment=nite metric=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-1.max node=nite-open-receiver-1 service=health  _1=nite _2=nite-open-receiver-1 _3=health _4=jmx _5=memoryUsage _6=pools _7=Compressed-Class-Space _8=max name=Compressed-Class-Space 1073741824 1625855945
_rawName=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-2.max cluster=open-receiver deployment=nite metric=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-2.max node=nite-open-receiver-1 service=health  _1=nite _2=nite-open-receiver-1 _3=health _4=jmx _5=memoryUsage _6=pools _7=Compressed-Class-Space _8=max name=Compressed-Class-Space 1073741827 1625855949`),
			wantedFunc: func() []telegraf.Metric {
				return []telegraf.Metric{
					metric.New(
						"nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-1.max",
						map[string]string{
							"_rawName":   "nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-1.max",
							"cluster":    "open-receiver",
							"deployment": "nite",
							"node":       "nite-open-receiver-1",
							"service":    "health",
							// meta tags
							"_1":   "nite",
							"_2":   "nite-open-receiver-1",
							"_3":   "health",
							"_4":   "jmx",
							"_5":   "memoryUsage",
							"_6":   "pools",
							"_7":   "Compressed-Class-Space",
							"_8":   "max",
							"name": "Compressed-Class-Space",
						},
						map[string]interface{}{
							// TODO reconsider this hack
							"": 1073741824,
						},
						time.Unix(1625855945, 0),
						telegraf.Gauge,
					),
					metric.New(
						"nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-2.max",
						map[string]string{
							"_rawName":   "nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space-2.max",
							"cluster":    "open-receiver",
							"deployment": "nite",
							"node":       "nite-open-receiver-1",
							"service":    "health",
							// meta tags
							"_1":   "nite",
							"_2":   "nite-open-receiver-1",
							"_3":   "health",
							"_4":   "jmx",
							"_5":   "memoryUsage",
							"_6":   "pools",
							"_7":   "Compressed-Class-Space",
							"_8":   "max",
							"name": "Compressed-Class-Space",
						},
						map[string]interface{}{
							// TODO reconsider this hack
							"": 1073741827,
						},
						time.Unix(1625855949, 0),
						telegraf.Gauge,
					),
				}
			},
		},
		{
			name:    "without_metric",
			input:   []byte("className=HealthTrackerKafkaDataQueueWriter cluster=open-receiver deployment=nite fullClassName=com.sumologic.health.io.HealthTrackerKafkaDataQueueWriter mtype=count node=nite-open-receiver-1 service=open-receiver stat=p75  _primaryMetricType=carbon 0.00 1625855958"),
			wantErr: true,
		},
	}

	p := Parser{}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := p.Parse(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			for i, e := range tc.wantedFunc() {
				assert.Equalf(t, e.Name(), m[i].Name(), "%d: Metric name not as expected", i)
				assert.Equal(t, e.Fields(), m[i].Fields(), i)
				assert.Equal(t, e.Tags(), m[i].Tags(), i)
				assert.Equal(t, e.Time(), m[i].Time(), i)
			}
		})
	}
}

func TestParseLine(t *testing.T) {
	testcases := []struct {
		name       string
		input      string
		wantErr    bool
		wantedFunc func() telegraf.Metric
	}{
		{
			name:  "basic1",
			input: "className=HealthTrackerKafkaDataQueueWriter cluster=open-receiver deployment=nite fullClassName=com.sumologic.health.io.HealthTrackerKafkaDataQueueWriter metric=kafka.queue.alpha_health_tracker_incidents.offer.timer mtype=count node=nite-open-receiver-1 service=open-receiver stat=p75  _primaryMetricType=carbon 0.00 1625855958",
			wantedFunc: func() telegraf.Metric {
				tags := map[string]string{
					"className":     "HealthTrackerKafkaDataQueueWriter",
					"cluster":       "open-receiver",
					"deployment":    "nite",
					"fullClassName": "com.sumologic.health.io.HealthTrackerKafkaDataQueueWriter",
					"mtype":         "count",
					"node":          "nite-open-receiver-1",
					"service":       "open-receiver",
					"stat":          "p75",
					// meta tags
					"_primaryMetricType": "carbon",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 0.0,
				}

				return metric.New(
					"kafka.queue.alpha_health_tracker_incidents.offer.timer",
					tags,
					fields,
					time.Unix(1625855958, 0),
					telegraf.Gauge,
				)
			},
		},
		{
			name:  "basic2",
			input: "className=KafkaDataQueueWriter cluster=open-receiver deployment=nite fullClassName=com.sumologic.interchange.kafka.queue.KafkaDataQueueWriter metric=kafka.queue.beta_trace_ingest_traces.offer.timer mtype=count node=nite-open-receiver-1 service=open-receiver stat=p75  _primaryMetricType=carbon 0.00 1625855958",
			wantedFunc: func() telegraf.Metric {
				tags := map[string]string{
					"className":          "KafkaDataQueueWriter",
					"cluster":            "open-receiver",
					"deployment":         "nite",
					"fullClassName":      "com.sumologic.interchange.kafka.queue.KafkaDataQueueWriter",
					"mtype":              "count",
					"node":               "nite-open-receiver-1",
					"service":            "open-receiver",
					"stat":               "p75",
					"_primaryMetricType": "carbon",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 0.0,
				}

				return metric.New(
					"kafka.queue.beta_trace_ingest_traces.offer.timer",
					tags,
					fields,
					time.Unix(1625855958, 0),
					telegraf.Gauge,
				)
			},
		},
		{
			name:  "basic3",
			input: "_rawName=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max cluster=open-receiver deployment=nite metric=nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max node=nite-open-receiver-1 service=health  _1=nite _2=nite-open-receiver-1 _3=health _4=jmx _5=memoryUsage _6=pools _7=Compressed-Class-Space _8=max name=Compressed-Class-Space 1073741824 1625855945",
			wantedFunc: func() telegraf.Metric {
				tags := map[string]string{
					"_rawName":   "nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
					"cluster":    "open-receiver",
					"deployment": "nite",
					"node":       "nite-open-receiver-1",
					"service":    "health",
					// meta tags
					"_1":   "nite",
					"_2":   "nite-open-receiver-1",
					"_3":   "health",
					"_4":   "jmx",
					"_5":   "memoryUsage",
					"_6":   "pools",
					"_7":   "Compressed-Class-Space",
					"_8":   "max",
					"name": "Compressed-Class-Space",
				}
				fields := map[string]interface{}{
					// TODO reconsider this hack
					"": 1073741824,
				}

				return metric.New(
					"nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
					tags,
					fields,
					time.Unix(1625855945, 0),
					telegraf.Gauge,
				)
			},
		},
		{
			name:    "without_metric",
			input:   "className=HealthTrackerKafkaDataQueueWriter cluster=open-receiver deployment=nite fullClassName=com.sumologic.health.io.HealthTrackerKafkaDataQueueWriter mtype=count node=nite-open-receiver-1 service=open-receiver stat=p75  _primaryMetricType=carbon 0.00 1625855958",
			wantErr: true,
		},
	}

	p := Parser{}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := p.ParseLine(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			expected := tc.wantedFunc()
			assert.Equalf(t, expected.Name(), m.Name(), "Metric name not as expected")
			assert.Equal(t, expected.Fields(), m.Fields())
			assert.Equal(t, expected.Tags(), m.Tags())
			assert.Equal(t, expected.Time(), m.Time())
		})
	}
}

func TestSerializeAndParseReturnsTheSameMetric(t *testing.T) {
	getTestMetrics := func() []telegraf.Metric {
		metrics := make([]telegraf.Metric, 0, 2)

		{
			tags := map[string]string{
				"className":          "KafkaDataQueueWriter",
				"cluster":            "open-receiver",
				"deployment":         "nite",
				"fullClassName":      "com.sumologic.interchange.kafka.queue.KafkaDataQueueWriter",
				"mtype":              "count",
				"node":               "nite-open-receiver-1",
				"service":            "open-receiver",
				"stat":               "p75",
				"_primaryMetricType": "carbon",
			}
			fields := map[string]interface{}{
				// TODO reconsider this hack
				"": 0.0,
			}

			metrics = append(metrics, metric.New(
				"kafka.queue.beta_trace_ingest_traces.offer.timer",
				tags,
				fields,
				time.Unix(1625855958, 0),
				telegraf.Gauge,
			),
			)
		}
		{
			tags := map[string]string{
				"_rawName":   "nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
				"cluster":    "open-receiver",
				"deployment": "nite",
				"node":       "nite-open-receiver-1",
				"service":    "health",
				// meta tags
				"_1":   "nite",
				"_2":   "nite-open-receiver-1",
				"_3":   "health",
				"_4":   "jmx",
				"_5":   "memoryUsage",
				"_6":   "pools",
				"_7":   "Compressed-Class-Space",
				"_8":   "max",
				"name": "Compressed-Class-Space",
			}
			fields := map[string]interface{}{
				// TODO reconsider this hack
				"": 1073741824,
			}

			metrics = append(metrics, metric.New(
				"nite.nite-open-receiver-1.health.jmx.memoryUsage.pools.Compressed-Class-Space.max",
				tags,
				fields,
				time.Unix(1625855945, 0),
				telegraf.Gauge,
			),
			)
		}
		return metrics
	}

	testcases := []struct {
		serializerFormat string
	}{
		{
			serializerFormat: string(carbon2.Carbon2FormatMetricIncludesField),
		},
		{
			serializerFormat: string(carbon2.Carbon2FormatFieldSeparate),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.serializerFormat, func(t *testing.T) {
			s, err := carbon2.NewSerializer(
				tc.serializerFormat,
				carbon2.DefaultSanitizeReplaceChar,
			)
			require.NoError(t, err)

			expected := getTestMetrics()
			b, err := s.SerializeBatch(expected)
			require.NoError(t, err)

			t.Logf("\n%s", b)

			p := Parser{}
			metrics, err := p.Parse(b)
			require.NoError(t, err)
			require.Len(t, metrics, len(expected))
			for i, m := range metrics {
				assert.Equal(t, expected[i], m)
			}

		})
	}
}
