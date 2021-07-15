package carbon2

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

const (
	tagNameField  = "field"
	tagNameMetric = "metric"
)

type Parser struct {
}

// Parse takes a byte buffer separated by newlines
// ie, `cpu.usage.idle 90\ncpu.usage.busy 10` and parses it into telegraf metrics.
//
// Must be thread-safe.
func (p Parser) Parse(buf []byte) ([]telegraf.Metric, error) {
	// parse even if the buffer begins with a newline
	buf = bytes.TrimPrefix(buf, []byte("\n"))

	var (
		buffer  = bytes.NewBuffer(buf)
		reader  = bufio.NewReader(buffer)
		metrics []telegraf.Metric
	)

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF && len(line) == 0 {
			break
		}

		m, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse line: %s, err: %w", line, err)
		}

		metrics = append(metrics, m)
	}

	return metrics, nil
}

func parseLine(line []byte) (telegraf.Metric, error) {
	var (
		buf      = bytes.NewBuffer(line)
		name     string
		tim      time.Time
		tags     = make(map[string]string)
		fields   = make(map[string]interface{})
		gotValue bool
	)

	for {
		bb, err := buf.ReadBytes(' ')
		if err != nil && err != io.EOF {
			return nil, err
		}

		if bytes.HasPrefix(bb, []byte(" ")) {
			continue
		}

		idx := bytes.IndexByte(bb, '=')
		if idx == -1 {
			if !gotValue {
				// It's a value so parse it
				v, err := parseBytesForValue(bb)
				if err != nil {
					return nil, err
				}

				// Note: this works around the fact that carbon2 serializer can
				// either:
				// * stitch together using '_' telegraf's metric Name and field
				//   name (taken from 'field' tag) and take that to be used
				//   as metric name, e.g. metric=memory_available
				// * use a separate 'field' tag, e.g. metric=memory field=available
				//
				// Hence parsing and serializing a metric would yield a different
				// result then ingested. Because of that reason we
				fields[""] = v
				gotValue = true
				continue
			} else {
				// It's a timestamp so parse it
				t, err := parseBytesForTimestamp(bb)
				if err != nil {
					return nil, err
				}

				tim = t
				break
			}
		}

		tag, value, err := getTag(bb, idx)
		if err != nil {
			return nil, err
		}

		if bytes.Compare(tag, []byte(tagNameMetric)) == 0 {
			// If it's a 'metric' tag then set it as metric's name
			name = string(value)
		} else {
			tags[string(tag)] = string(value)
		}
	}

	if name == "" {
		return nil, errors.New("metric without 'metric' tag")
	}

	return metric.New(name, tags, fields, tim, telegraf.Gauge), nil
}

func parseBytesForTimestamp(b []byte) (time.Time, error) {
	s := string(bytes.TrimSpace(b))

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(i, 0), nil
}

func parseBytesForValue(b []byte) (interface{}, error) {
	trimmed := bytes.TrimSpace(b)

	if bytes.Contains(trimmed, []byte(".")) {
		vf, err := strconv.ParseFloat(string(trimmed), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse value: %s, err: %w", b, err)
		}
		return vf, nil
	}

	vi, err := strconv.ParseInt(string(trimmed), 10, 64)
	if err != nil {
		return nil, err
	}
	return vi, nil
}

func getTag(bb []byte, equalSignIdx int) ([]byte, []byte, error) {
	field := bb[:equalSignIdx]
	value := bytes.TrimSpace(bb[equalSignIdx+1:])
	return field, value, nil
}

// ParseLine takes a single string metric ie, "cpu.usage.idle 90"
// and parses it into a telegraf metric.
//
// Must be thread-safe.
// This function is only called by plugins that expect line based protocols
// Doesn't need to be implemented by non-linebased parsers (e.g. json, xml)
func (p Parser) ParseLine(line string) (telegraf.Metric, error) {
	return parseLine([]byte(line))
}

// SetDefaultTags tells the parser to add all of the given tags
// to each parsed metric.
// NOTE: do _not_ modify the map after you've passed it here!!
func (p Parser) SetDefaultTags(tags map[string]string) {
}
