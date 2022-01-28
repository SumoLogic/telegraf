package config_test

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf/config"
	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	var d config.Duration

	require.NoError(t, d.UnmarshalTOML([]byte(`"1s"`)))
	require.Equal(t, time.Second, time.Duration(d))

	d = config.Duration(0)
	require.NoError(t, d.UnmarshalTOML([]byte(`1s`)))
	require.Equal(t, time.Second, time.Duration(d))

	d = config.Duration(0)
	require.NoError(t, d.UnmarshalTOML([]byte(`'1s'`)))
	require.Equal(t, time.Second, time.Duration(d))

	d = config.Duration(0)
	require.NoError(t, d.UnmarshalTOML([]byte(`10`)))
	require.Equal(t, 10*time.Second, time.Duration(d))

	d = config.Duration(0)
	require.NoError(t, d.UnmarshalTOML([]byte(`1.5`)))
	require.Equal(t, time.Second, time.Duration(d))
}

func TestSize(t *testing.T) {
	var s config.Size

	require.NoError(t, s.UnmarshalTOML([]byte(`"1B"`)))
	require.Equal(t, int64(1), int64(s))

	s = config.Size(0)
	require.NoError(t, s.UnmarshalTOML([]byte(`1`)))
	require.Equal(t, int64(1), int64(s))

	s = config.Size(0)
	require.NoError(t, s.UnmarshalTOML([]byte(`'1'`)))
	require.Equal(t, int64(1), int64(s))

	s = config.Size(0)
	require.NoError(t, s.UnmarshalTOML([]byte(`"1GB"`)))
	require.Equal(t, int64(1000*1000*1000), int64(s))

	s = config.Size(0)
	require.NoError(t, s.UnmarshalTOML([]byte(`"12GiB"`)))
	require.Equal(t, int64(12*1024*1024*1024), int64(s))
}
