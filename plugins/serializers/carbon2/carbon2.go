package carbon2

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/influxdata/telegraf"
)

type format string

const (
	Carbon2FormatFieldEmpty          = format("")
	Carbon2FormatFieldSeparate       = format("field_separate")
	Carbon2FormatMetricIncludesField = format("metric_includes_field")
)

var formats = map[format]struct{}{
	Carbon2FormatFieldEmpty:          {},
	Carbon2FormatFieldSeparate:       {},
	Carbon2FormatMetricIncludesField: {},
}

type Serializer struct {
	metricsFormat     format
	metricsFormatLock sync.RWMutex
}

func NewSerializer(metricsFormat string) (*Serializer, error) {
	var f = format(metricsFormat)
	if _, ok := formats[f]; !ok {
		return nil, fmt.Errorf("unknown carbon2 format: %s", f)
	}

	return &Serializer{
		metricsFormat: f,
	}, nil
}

func (s *Serializer) Serialize(metric telegraf.Metric) ([]byte, error) {
	return s.createObject(metric), nil
}

func (s *Serializer) SerializeBatch(metrics []telegraf.Metric) ([]byte, error) {
	var batch bytes.Buffer
	for _, metric := range metrics {
		batch.Write(s.createObject(metric))
	}
	return batch.Bytes(), nil
}

func (s *Serializer) createObject(metric telegraf.Metric) []byte {
	var m bytes.Buffer
	metricsFormat := s.getMetricsFormat()

	for fieldName, fieldValue := range metric.Fields() {
		if isString(fieldValue) {
			continue
		}

		switch metricsFormat {
		// Field separate is the default when no format specified.
		case Carbon2FormatFieldEmpty:
			fallthrough
		case Carbon2FormatFieldSeparate:
			m.WriteString(serializeMetricFieldSeparate(
				metric.Name(), fieldName,
			))

		case Carbon2FormatMetricIncludesField:
			m.WriteString(serializeMetricIncludeField(
				metric.Name(), fieldName,
			))
		}

		for _, tag := range metric.TagList() {
			m.WriteString(strings.Replace(tag.Key, " ", "_", -1))
			m.WriteString("=")
			value := tag.Value
			if len(value) == 0 {
				value = "null"
			}
			m.WriteString(strings.Replace(value, " ", "_", -1))
			m.WriteString(" ")
		}
		m.WriteString(" ")
		m.WriteString(formatValue(fieldValue))
		m.WriteString(" ")
		m.WriteString(strconv.FormatInt(metric.Time().Unix(), 10))
		m.WriteString("\n")
	}
	return m.Bytes()
}

func (s *Serializer) SetMetricsFormat(f format) {
	s.metricsFormatLock.Lock()
	s.metricsFormat = f
	s.metricsFormatLock.Unlock()
}

func (s *Serializer) getMetricsFormat() format {
	s.metricsFormatLock.RLock()
	defer s.metricsFormatLock.RUnlock()
	return s.metricsFormat
}

func (s *Serializer) IsMetricsFormatUnset() bool {
	s.metricsFormatLock.RLock()
	defer s.metricsFormatLock.RUnlock()
	return s.metricsFormat == Carbon2FormatFieldEmpty
}

func serializeMetricFieldSeparate(name, fieldName string) string {
	return fmt.Sprintf("metric=%s field=%s ",
		strings.Replace(name, " ", "_", -1),
		strings.Replace(fieldName, " ", "_", -1),
	)
}

func serializeMetricIncludeField(name, fieldName string) string {
	return fmt.Sprintf("metric=%s_%s ",
		strings.Replace(name, " ", "_", -1),
		strings.Replace(fieldName, " ", "_", -1),
	)
}

func formatValue(fieldValue interface{}) string {
	switch v := fieldValue.(type) {
	case bool:
		// Print bools as 0s and 1s
		return fmt.Sprintf("%d", bool2int(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func isString(v interface{}) bool {
	switch v.(type) {
	case string:
		return true
	default:
		return false
	}
}

func bool2int(b bool) int {
	// Slightly more optimized than a usual if ... return ... else return ... .
	// See: https://0x0f.me/blog/golang-compiler-optimization/
	var i int
	if b {
		i = 1
	} else {
		i = 0
	}
	return i
}
