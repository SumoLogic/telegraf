module github.com/influxdata/telegraf

go 1.17

require (
	collectd.org v0.5.0
	github.com/Azure/go-autorest/autorest/adal v0.9.18
	github.com/BurntSushi/toml v0.4.1
	github.com/Shopify/sarama v1.32.0
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/antchfx/jsonquery v1.1.5
	github.com/antchfx/xmlquery v1.3.9
	github.com/antchfx/xpath v1.2.0
	github.com/armon/go-socks5 v0.0.0-20160902184237-e75332964ef5
	github.com/aws/aws-sdk-go-v2 v1.14.0
	github.com/aws/aws-sdk-go-v2/config v1.13.1
	github.com/aws/aws-sdk-go-v2/credentials v1.8.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.14.0
	github.com/benbjohnson/clock v1.3.0
	github.com/bmatcuk/doublestar/v3 v3.0.0
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f
	github.com/couchbase/go-couchbase v0.1.1
	github.com/denisenkom/go-mssqldb v0.12.0
	github.com/dimchansky/utfbom v1.1.1
	github.com/docker/docker v20.10.14+incompatible
	github.com/doclambda/protobufquery v0.0.0-20210317203640-88ffabe06a60
	github.com/fatih/color v1.13.0
	github.com/go-logfmt/logfmt v0.5.1
	github.com/go-ping/ping v0.0.0-20210201095549-52eed920f98c
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gobwas/glob v0.2.3
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.7
	github.com/gosnmp/gosnmp v1.34.0
	github.com/hashicorp/consul/api v1.12.0
	github.com/influxdata/line-protocol/v2 v2.2.1
	github.com/influxdata/tail v1.0.1-0.20210707231403-b283181d1fa7
	github.com/influxdata/toml v0.0.0-20190415235208-270119a8ce65
	github.com/influxdata/wlog v0.0.0-20160411224016-7c63b0a71ef8
	github.com/jackc/pgx/v4 v4.15.0
	github.com/jhump/protoreflect v1.8.3-0.20210616212123-6cc1efa697ca
	github.com/kardianos/service v1.2.1
	github.com/karrick/godirwalk v1.16.1
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369
	github.com/miekg/dns v1.1.48
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.34.0
	github.com/prometheus/prometheus v0.35.1-0.20220503184552-2381d7be5731
	github.com/shirou/gopsutil/v3 v3.22.2
	github.com/sirupsen/logrus v1.8.1
	github.com/sleepinggenius2/gosmi v0.4.4
	github.com/stretchr/testify v1.7.1
	github.com/tidwall/gjson v1.10.2
	github.com/tinylib/msgp v1.1.6
	github.com/vjeantet/grok v1.0.1
	github.com/xdg/scram v1.0.3
	go.mongodb.org/mongo-driver v1.8.3
	go.starlark.net v0.0.0-20210406145628-7a1108eaa012
	golang.org/x/net v0.0.0-20220412020605-290c469a71a5
	golang.org/x/oauth2 v0.0.0-20220411215720-9780585627b5
	golang.org/x/sys v0.0.0-20220412211240-33da011f77ad
	golang.org/x/text v0.3.7
	google.golang.org/protobuf v1.28.0
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7
)

require (
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/Microsoft/go-winio v0.5.1 // indirect
	github.com/alecthomas/participle v0.4.1 // indirect
	github.com/armon/go-metrics v0.3.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.11.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.3.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.9.0 // indirect
	github.com/aws/smithy-go v1.11.0 // indirect
	github.com/containerd/containerd v1.6.1 // indirect
	github.com/couchbase/gomemcached v0.1.3 // indirect
	github.com/couchbase/goutils v0.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gofrs/uuid v4.2.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.2.0 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.0.0-20170517235910-f1bb20e5a188 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.16.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.2.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/serf v0.9.6 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.11.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.10.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.14.4 // indirect
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/naoina/go-stringutil v0.1.0 // indirect
	github.com/onsi/ginkgo v1.16.4 // indirect
	github.com/onsi/gomega v1.15.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rogpeppe/go-internal v1.6.2 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.0 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65 // indirect
	golang.org/x/tools v0.1.10 // indirect
	golang.org/x/xerrors v0.0.0-20220411194840-2f41105eb62f // indirect
	gonum.org/v1/gonum v0.7.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220414192740-2d67ff6cf2b4 // indirect
	google.golang.org/grpc v1.46.0 // indirect
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

// replaced due to https://github.com/satori/go.uuid/issues/73
replace github.com/satori/go.uuid => github.com/gofrs/uuid v3.2.0+incompatible

// replaced due to https//github.com/mdlayher/apcupsd/issues/10
replace github.com/mdlayher/apcupsd => github.com/influxdata/apcupsd v0.0.0-20210427145308-694d5caead0e

// replaced due to open PR updating protobuf https://github.com/cisco-ie/nx-telemetry-proto/pull/1
replace github.com/cisco-ie/nx-telemetry-proto => github.com/sbezverk/nx-telemetry-proto v0.0.0-20210629125746-3c19a51b1abc

// replaced due to open PR updating protobuf https://github.com/riemann/riemann-go-client/pull/27
replace github.com/riemann/riemann-go-client => github.com/dstrand1/riemann-go-client v0.5.1-0.20211028194734-b5eb11fb5754
