package serializers

import (
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers/carbon2"
	"github.com/influxdata/telegraf/plugins/serializers/graphite"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/plugins/serializers/json"
)

// SerializerOutput is an interface for output plugins that are able to
// serialize telegraf metrics into arbitrary data formats.
type SerializerOutput interface {
	// SetSerializer sets the serializer function for the interface.
	SetSerializer(serializer Serializer)
}

// Serializer is an interface defining functions that a serializer plugin must
// satisfy.
//
// Implementations of this interface should be reentrant but are not required
// to be thread-safe.
type Serializer interface {
	// Serialize takes a single telegraf metric and turns it into a byte buffer.
	// separate metrics should be separated by a newline, and there should be
	// a newline at the end of the buffer.
	//
	// New plugins should use SerializeBatch instead to allow for non-line
	// delimited metrics.
	Serialize(metric telegraf.Metric) ([]byte, error)

	// SerializeBatch takes an array of telegraf metric and serializes it into
	// a byte buffer.  This method is not required to be suitable for use with
	// line oriented framing.
	SerializeBatch(metrics []telegraf.Metric) ([]byte, error)
}

// Config is a struct that covers the data types needed for all serializer types,
// and can be used to instantiate _any_ of the serializers.
type Config struct {
	// DataFormat can be one of the serializer types listed in NewSerializer.
	DataFormat string `toml:"data_format"`

	// Carbon2 metric format.
	Carbon2Format string `toml:"carbon2_format"`

	// Character used for metric name sanitization in Carbon2.
	Carbon2SanitizeReplaceChar string `toml:"carbon2_sanitize_replace_char"`

	// Support tags in graphite protocol
	GraphiteTagSupport bool `toml:"graphite_tag_support"`

	// Support tags which follow the spec
	GraphiteTagSanitizeMode string `toml:"graphite_tag_sanitize_mode"`

	// Character for separating metric name and field for Graphite tags
	GraphiteSeparator string `toml:"graphite_separator"`

	// Maximum line length in bytes; influx format only
	InfluxMaxLineBytes int `toml:"influx_max_line_bytes"`

	// Sort field keys, set to true only when debugging as it less performant
	// than unsorted fields; influx format only
	InfluxSortFields bool `toml:"influx_sort_fields"`

	// Support unsigned integer output; influx format only
	InfluxUintSupport bool `toml:"influx_uint_support"`

	// Prefix to add to all measurements, only supports Graphite
	Prefix string `toml:"prefix"`

	// Template for converting telegraf metrics into Graphite
	// only supports Graphite
	Template string `toml:"template"`

	// Templates same Template, but multiple
	Templates []string `toml:"templates"`

	// Timestamp units to use for JSON formatted output
	TimestampUnits time.Duration `toml:"timestamp_units"`

	// Timestamp format to use for JSON formatted output
	TimestampFormat string `toml:"timestamp_format"`

	// Include HEC routing fields for splunkmetric output
	HecRouting bool `toml:"hec_routing"`
}

// NewSerializer a Serializer interface based on the given config.
func NewSerializer(config *Config) (Serializer, error) {
	var err error
	var serializer Serializer
	switch config.DataFormat {
	case "influx":
		serializer, err = NewInfluxSerializerConfig(config)
	case "graphite":
		serializer, err = NewGraphiteSerializer(config.Prefix, config.Template, config.GraphiteTagSupport, config.GraphiteTagSanitizeMode, config.GraphiteSeparator, config.Templates)
	case "json":
		serializer, err = NewJSONSerializer(config.TimestampUnits, config.TimestampFormat)
	case "carbon2":
		serializer, err = NewCarbon2Serializer(config.Carbon2Format, config.Carbon2SanitizeReplaceChar)
	default:
		err = fmt.Errorf("invalid data format: %s", config.DataFormat)
	}
	return serializer, err
}

func NewJSONSerializer(timestampUnits time.Duration, timestampFormat string) (Serializer, error) {
	return json.NewSerializer(timestampUnits, timestampFormat)
}

func NewCarbon2Serializer(carbon2format string, carbon2SanitizeReplaceChar string) (Serializer, error) {
	return carbon2.NewSerializer(carbon2format, carbon2SanitizeReplaceChar)
}

func NewInfluxSerializerConfig(config *Config) (Serializer, error) {
	var sort influx.FieldSortOrder
	if config.InfluxSortFields {
		sort = influx.SortFields
	}

	var typeSupport influx.FieldTypeSupport
	if config.InfluxUintSupport {
		typeSupport = typeSupport + influx.UintSupport
	}

	s := influx.NewSerializer()
	s.SetMaxLineBytes(config.InfluxMaxLineBytes)
	s.SetFieldSortOrder(sort)
	s.SetFieldTypeSupport(typeSupport)
	return s, nil
}

func NewInfluxSerializer() (Serializer, error) {
	return influx.NewSerializer(), nil
}

func NewGraphiteSerializer(prefix, template string, tagSupport bool, tagSanitizeMode string, separator string, templates []string) (Serializer, error) {
	graphiteTemplates, defaultTemplate, err := graphite.InitGraphiteTemplates(templates)
	if err != nil {
		return nil, err
	}

	if defaultTemplate != "" {
		template = defaultTemplate
	}

	if tagSanitizeMode == "" {
		tagSanitizeMode = "strict"
	}

	if separator == "" {
		separator = "."
	}

	return &graphite.GraphiteSerializer{
		Prefix:          prefix,
		Template:        template,
		TagSupport:      tagSupport,
		TagSanitizeMode: tagSanitizeMode,
		Separator:       separator,
		Templates:       graphiteTemplates,
	}, nil
}
