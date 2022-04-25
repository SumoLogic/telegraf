package all

import (
	// Blank imports for plugins to register themselves

	_ "github.com/influxdata/telegraf/plugins/inputs/activemq"
	_ "github.com/influxdata/telegraf/plugins/inputs/apache"

	// Dependency conflict:
	// # github.com/influxdata/telegraf/plugins/inputs/cisco_telemetry_mdt
	// ../../../../telegraf/telegraf/plugins/inputs/cisco_telemetry_mdt/cisco_telemetry_mdt.go:66:2: undefined: mdt_dialout.UnimplementedGRPCMdtDialoutServer
	// _ "github.com/influxdata/telegraf/plugins/inputs/cisco_telemetry_mdt"
	//

	_ "github.com/influxdata/telegraf/plugins/inputs/clickhouse"
	_ "github.com/influxdata/telegraf/plugins/inputs/conntrack"
	_ "github.com/influxdata/telegraf/plugins/inputs/consul"
	_ "github.com/influxdata/telegraf/plugins/inputs/consul_agent"
	_ "github.com/influxdata/telegraf/plugins/inputs/couchbase"
	_ "github.com/influxdata/telegraf/plugins/inputs/couchdb"
	_ "github.com/influxdata/telegraf/plugins/inputs/cpu"
	_ "github.com/influxdata/telegraf/plugins/inputs/disk"
	_ "github.com/influxdata/telegraf/plugins/inputs/diskio"
	_ "github.com/influxdata/telegraf/plugins/inputs/disque"
	_ "github.com/influxdata/telegraf/plugins/inputs/dns_query"
	_ "github.com/influxdata/telegraf/plugins/inputs/docker"
	_ "github.com/influxdata/telegraf/plugins/inputs/docker_log"
	_ "github.com/influxdata/telegraf/plugins/inputs/elasticsearch"
	_ "github.com/influxdata/telegraf/plugins/inputs/exec"
	_ "github.com/influxdata/telegraf/plugins/inputs/execd"
	_ "github.com/influxdata/telegraf/plugins/inputs/file"
	_ "github.com/influxdata/telegraf/plugins/inputs/filecount"
	_ "github.com/influxdata/telegraf/plugins/inputs/filestat"
	_ "github.com/influxdata/telegraf/plugins/inputs/fluentd"
	_ "github.com/influxdata/telegraf/plugins/inputs/haproxy"
	_ "github.com/influxdata/telegraf/plugins/inputs/http"
	_ "github.com/influxdata/telegraf/plugins/inputs/http_listener_v2"
	_ "github.com/influxdata/telegraf/plugins/inputs/http_response"
	_ "github.com/influxdata/telegraf/plugins/inputs/httpjson"
	_ "github.com/influxdata/telegraf/plugins/inputs/jenkins"
	_ "github.com/influxdata/telegraf/plugins/inputs/jolokia2"
	_ "github.com/influxdata/telegraf/plugins/inputs/mem"
	_ "github.com/influxdata/telegraf/plugins/inputs/memcached"
	_ "github.com/influxdata/telegraf/plugins/inputs/mongodb"
	_ "github.com/influxdata/telegraf/plugins/inputs/mysql"
	_ "github.com/influxdata/telegraf/plugins/inputs/net"
	_ "github.com/influxdata/telegraf/plugins/inputs/net_response"
	_ "github.com/influxdata/telegraf/plugins/inputs/nginx"
	_ "github.com/influxdata/telegraf/plugins/inputs/nginx_plus"
	_ "github.com/influxdata/telegraf/plugins/inputs/nginx_plus_api"
	_ "github.com/influxdata/telegraf/plugins/inputs/nginx_sts"
	_ "github.com/influxdata/telegraf/plugins/inputs/nginx_upstream_check"
	_ "github.com/influxdata/telegraf/plugins/inputs/nginx_vts"
	_ "github.com/influxdata/telegraf/plugins/inputs/phpfpm"
	_ "github.com/influxdata/telegraf/plugins/inputs/ping"
	_ "github.com/influxdata/telegraf/plugins/inputs/postgresql"
	_ "github.com/influxdata/telegraf/plugins/inputs/postgresql_extensible"
	_ "github.com/influxdata/telegraf/plugins/inputs/processes"
	_ "github.com/influxdata/telegraf/plugins/inputs/procstat"
	_ "github.com/influxdata/telegraf/plugins/inputs/rabbitmq"
	_ "github.com/influxdata/telegraf/plugins/inputs/redis"

	// Dependency conflict:
	// # github.com/influxdata/telegraf/plugins/inputs/riemann_listener
	// ../../../../telegraf/telegraf/plugins/inputs/riemann_listener/riemann_listener.go:194:27: cannot use messagePb (type *"github.com/riemann/riemann-go-client/proto".Msg) as type protoreflect.ProtoMessage in argument to "google.golang.org/protobuf/proto".Unmarshal:
	// *"github.com/riemann/riemann-go-client/proto".Msg does not implement protoreflect.ProtoMessage (missing ProtoReflect method)
	// ../../../../telegraf/telegraf/plugins/inputs/riemann_listener/riemann_listener.go:227:34: cannot use message (type *"github.com/riemann/riemann-go-client/proto".Msg) as type protoreflect.ProtoMessage in argument to "google.golang.org/protobuf/proto".Marshal:
	// *"github.com/riemann/riemann-go-client/proto".Msg does not implement protoreflect.ProtoMessage (missing ProtoReflect method)
	// ../../../../telegraf/telegraf/plugins/inputs/riemann_listener/riemann_listener.go:250:34: cannot use message (type *"github.com/riemann/riemann-go-client/proto".Msg) as type protoreflect.ProtoMessage in argument to "google.golang.org/protobuf/proto".Marshal:
	// *"github.com/riemann/riemann-go-client/proto".Msg does not implement protoreflect.ProtoMessage (missing ProtoReflect method)
	//
	// _ "github.com/influxdata/telegraf/plugins/inputs/riemann_listener"

	_ "github.com/influxdata/telegraf/plugins/inputs/snmp"
	_ "github.com/influxdata/telegraf/plugins/inputs/snmp_trap"
	_ "github.com/influxdata/telegraf/plugins/inputs/socket_listener"
	_ "github.com/influxdata/telegraf/plugins/inputs/sqlserver"
	_ "github.com/influxdata/telegraf/plugins/inputs/system"
	_ "github.com/influxdata/telegraf/plugins/inputs/varnish"
	_ "github.com/influxdata/telegraf/plugins/inputs/win_perf_counters"

	// Dependency conflict:
	//
	// imports\n\tgithub.com/influxdata/telegraf/plugins/inputs/wireguard
	// imports\n\tgolang.zx2c4.com/wireguard/wgctrl
	// imports\n\tgolang.zx2c4.com/wireguard/wgctrl/internal/wguser
	// imports\n\tgolang.zx2c4.com/wireguard/ipc/namedpipe:
	//   package golang.zx2c4.com/wireguard/ipc/namedpipe provided by golang.zx2c4.com/wireguard at latest version v0.0.0-20220117163742-e0b8f11489c5 but not at required version v0.0.20200121
	// _ "github.com/influxdata/telegraf/plugins/inputs/wireguard"

	_ "github.com/influxdata/telegraf/plugins/inputs/zookeeper"
)
