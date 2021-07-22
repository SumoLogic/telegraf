//go:build !custom || parsers || parsers.carbon2

package all

import _ "github.com/influxdata/telegraf/plugins/parsers/carbon2" // register plugin
