module github.com/influxdata/telegraf

go 1.20

require (
	cloud.google.com/go/monitoring v1.14.0
	cloud.google.com/go/pubsub v1.30.1
	cloud.google.com/go/storage v1.29.0
	collectd.org v0.5.0
	github.com/99designs/keyring v1.2.2
	github.com/Azure/azure-event-hubs-go/v3 v3.5.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor v0.4.1
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources v0.3.1
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-autorest/autorest/adal v0.9.23
	github.com/BurntSushi/toml v1.2.1
	github.com/ClickHouse/clickhouse-go v1.5.4
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Mellanox/rdmamap v1.1.0
	github.com/Shopify/sarama v1.38.1
	github.com/aerospike/aerospike-client-go/v5 v5.11.0
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/aliyun/alibaba-cloud-sdk-go v1.62.337
	github.com/antchfx/jsonquery v1.3.1
	github.com/antchfx/xmlquery v1.3.15
	github.com/antchfx/xpath v1.2.4
	github.com/apache/arrow/go/v13 v13.0.0-20230505140406-c2f7d13e16c4
	github.com/apache/thrift v0.18.1
	github.com/aristanetworks/goarista v0.0.0-20190325233358-a123909ec740
	github.com/armon/go-socks5 v0.0.0-20160902184237-e75332964ef5
	github.com/awnumar/memguard v0.22.3
	github.com/aws/aws-sdk-go-v2 v1.18.0
	github.com/aws/aws-sdk-go-v2/config v1.18.8
	github.com/aws/aws-sdk-go-v2/credentials v1.13.20
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.25.9
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.17.3
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.17.8
	github.com/aws/aws-sdk-go-v2/service/sts v1.19.0
	github.com/benbjohnson/clock v1.3.3
	github.com/blues/jsonata-go v1.5.4
	github.com/bmatcuk/doublestar/v3 v3.0.0
	github.com/boschrexroth/ctrlx-datalayer-golang v1.3.0
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/cisco-ie/nx-telemetry-proto v0.0.0-20230117155933-f64c045c77df
	github.com/compose-spec/compose-go v1.13.4
	github.com/coreos/go-semver v0.3.1
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f
	github.com/couchbase/go-couchbase v0.1.1
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/digitalocean/go-libvirt v0.0.0-20220811165305-15feff002086
	github.com/dimchansky/utfbom v1.1.1
	github.com/djherbis/times v1.5.0
	github.com/docker/docker v23.0.4+incompatible
	github.com/docker/go-connections v0.4.0
	github.com/doclambda/protobufquery v0.0.0-20220727165953-0da287796ee9
	github.com/eclipse/paho.golang v0.10.0
	github.com/eclipse/paho.mqtt.golang v1.4.2
	github.com/fatih/color v1.15.0
	github.com/go-ldap/ldap/v3 v3.4.4
	github.com/go-logfmt/logfmt v0.6.0
	github.com/go-ole/go-ole v1.2.6
	github.com/go-redis/redis/v7 v7.4.1
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gobwas/glob v0.2.3
	github.com/gofrs/uuid/v5 v5.0.0
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/golang/snappy v0.0.4
	github.com/google/cel-go v0.14.1-0.20230424164844-d39523c445fc
	github.com/google/gnxi v0.0.0-20221016143401-2aeceb5a2901
	github.com/google/go-cmp v0.5.9
	github.com/google/go-github/v32 v32.1.0
	github.com/google/gopacket v1.1.19
	github.com/google/licensecheck v0.3.1
	github.com/google/uuid v1.3.0
	github.com/gopcua/opcua v0.3.7
	github.com/gophercloud/gophercloud v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/gosnmp/gosnmp v1.35.0
	github.com/grid-x/modbus v0.0.0-20211113184042-7f2251c342c9
	github.com/harlow/kinesis-consumer v0.3.6-0.20211204214318-c2b9f79d7ab6
	github.com/hashicorp/consul/api v1.20.0
	github.com/influxdata/go-syslog/v3 v3.0.0
	github.com/influxdata/line-protocol/v2 v2.2.1
	github.com/influxdata/tail v1.0.1-0.20210707231403-b283181d1fa7
	github.com/influxdata/toml v0.0.0-20190415235208-270119a8ce65
	github.com/influxdata/wlog v0.0.0-20160411224016-7c63b0a71ef8
	github.com/intel/iaevents v1.1.0
	github.com/jackc/pgx/v4 v4.17.1
	github.com/james4k/rcon v0.0.0-20120923215419-8fbb8268b60a
	github.com/jeremywohl/flatten/v2 v2.0.0-20211013061545-07e4a09fb8e4
	github.com/jhump/protoreflect v1.15.1
	github.com/jmespath/go-jmespath v0.4.0
	github.com/kardianos/service v1.2.2
	github.com/karrick/godirwalk v1.16.2
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/klauspost/compress v1.16.5
	github.com/klauspost/pgzip v1.2.6
	github.com/kolo/xmlrpc v0.0.0-20220921171641-a4b6fa1dd06b
	github.com/linkedin/goavro/v2 v2.12.0
	github.com/logzio/azure-monitor-metrics-receiver v1.0.0
	github.com/lxc/lxd v0.0.0-20220920163450-e9b4b514106a
	github.com/matttproud/golang_protobuf_extensions v1.0.4
	github.com/mdlayher/apcupsd v0.0.0-20220319200143-473c7b5f3c6a
	github.com/miekg/dns v1.1.51
	github.com/moby/ipvs v1.1.0
	github.com/multiplay/go-ts3 v1.1.0
	github.com/nats-io/nats-server/v2 v2.9.9
	github.com/nats-io/nats.go v1.24.0
	github.com/netsampler/goflow2 v1.3.3
	github.com/nsqio/go-nsq v1.1.0
	github.com/openconfig/gnmi v0.9.1
	github.com/opensearch-project/opensearch-go/v2 v2.2.0
	github.com/opentracing/opentracing-go v1.2.1-0.20220228012449-10b1cf09e00b
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.5.0
	github.com/openzipkin/zipkin-go v0.4.1
	github.com/p4lang/p4runtime v1.3.0
	github.com/pborman/ansi v1.0.0
	github.com/pion/dtls/v2 v2.2.7
	github.com/prometheus-community/pro-bing v0.1.0
	github.com/prometheus/client_model v0.4.0
	github.com/prometheus/common v0.44.0
	github.com/prometheus/procfs v0.9.0
	github.com/prometheus/prometheus v0.42.0
	github.com/rabbitmq/amqp091-go v1.8.1
	github.com/robbiet480/go.nut v0.0.0-20220219091450-bd8f121e1fa1
	github.com/safchain/ethtool v0.3.0
	github.com/santhosh-tekuri/jsonschema/v5 v5.3.0
	github.com/shirou/gopsutil/v3 v3.23.5
	github.com/showwin/speedtest-go v1.6.2
	github.com/sirupsen/logrus v1.9.0
	github.com/sleepinggenius2/gosmi v0.4.4
	github.com/stretchr/testify v1.8.4
	github.com/tbrandon/mbserver v0.0.0-20170611213546-993e1772cc62
	github.com/testcontainers/testcontainers-go v0.18.0
	github.com/thomasklein94/packer-plugin-libvirt v0.3.4
	github.com/tidwall/gjson v1.14.4
	github.com/tinylib/msgp v1.1.8
	github.com/urfave/cli/v2 v2.25.5
	github.com/vapourismo/knx-go v0.0.0-20220829185957-fb5458a5389d
	github.com/vjeantet/grok v1.0.1
	github.com/vmware/govmomi v0.28.1-0.20220921224932-b4b508abf208
	github.com/wvanbergen/kafka v0.0.0-20171203153745-e2edea948ddf
	github.com/x448/float16 v0.8.4
	github.com/xdg/scram v1.0.5
	github.com/yuin/goldmark v1.5.4
	go.mongodb.org/mongo-driver v1.11.2
	go.starlark.net v0.0.0-20220328144851-d1966c6b9fcd
	golang.org/x/crypto v0.8.0
	golang.org/x/mod v0.10.0
	golang.org/x/net v0.10.0
	golang.org/x/oauth2 v0.8.0
	golang.org/x/sync v0.2.0
	golang.org/x/sys v0.8.0
	golang.org/x/term v0.8.0
	golang.org/x/text v0.9.0
	google.golang.org/api v0.121.0
	google.golang.org/genproto/googleapis/api v0.0.0-20230530153820-e85fd2cbaebc
	google.golang.org/grpc v1.55.0
	google.golang.org/protobuf v1.30.0
	gopkg.in/gorethink/gorethink.v3 v3.0.5
	gopkg.in/olivere/elastic.v5 v5.0.86
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.27.2
	k8s.io/apimachinery v0.27.2
	k8s.io/client-go v0.27.2
	layeh.com/radius v0.0.0-20221205141417-e7fbddd11d68
	modernc.org/sqlite v1.21.0
)

require (
	github.com/Azure/go-autorest/autorest v0.11.28 // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.12 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.3 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/jackc/pgconn v1.14.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	gonum.org/v1/gonum v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20230530153820-e85fd2cbaebc // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230530153820-e85fd2cbaebc // indirect
)

require (
	cloud.google.com/go v0.110.1 // indirect
	cloud.google.com/go/compute v1.19.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.0.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/Azure/azure-amqp-common-go/v4 v4.1.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-sdk-for-go v65.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.21.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.13.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v0.9.1 // indirect
	github.com/Azure/go-amqp v0.19.1 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/Azure/go-ntlmssp v0.0.0-20220621081337-cb9428e4ac1e // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.4.0 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/alecthomas/participle v0.4.1 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230305170008-8188dc5388df // indirect
	github.com/aristanetworks/glog v0.0.0-20191112221043-67e8567f59f3 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/awnumar/memcall v0.1.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.2.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.12.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.14.8 // indirect
	github.com/awslabs/kinesis-aggregation/go v0.0.0-20210630091500-54e17340d32f // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bufbuild/protocompile v0.4.0 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cloudevents/sdk-go/v2 v2.14.0
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/containerd/containerd v1.6.18 // indirect
	github.com/couchbase/gomemcached v0.1.3 // indirect
	github.com/couchbase/goutils v0.1.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230111030713-bf00bc1b83b6 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/emicklei/go-restful/v3 v3.10.1 // indirect
	github.com/flosch/pongo2 v0.0.0-20200913210552-0d938eb266f3 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.4 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-macaroon-bakery/macaroonpb v1.0.0 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/goburrow/modbus v0.1.0 // indirect
	github.com/goburrow/serial v0.1.1-0.20211022031912-bfb69110f8dd // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gofrs/uuid v4.2.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/flatbuffers v23.3.3+incompatible // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/s2a-go v0.1.3 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.8.0 // indirect
	github.com/grid-x/serial v0.0.0-20211107191517-583c7356b3aa // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.4.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/hashicorp/packer-plugin-sdk v0.3.1 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.2 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/juju/webbrowser v1.0.0 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/leodido/ragel-machinery v0.0.0-20181214104525-299bdde78165 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/moby/patternmatcher v0.5.0 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/term v0.0.0-20221128092401-c43b287e0e0f // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/naoina/go-stringutil v0.1.0 // indirect
	github.com/nats-io/jwt/v2 v2.3.0 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc2 // indirect
	github.com/opencontainers/runc v1.1.5 // indirect
	github.com/opentracing-contrib/go-observer v0.0.0-20170622124052-a52f23424492 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/transport/v2 v2.2.1 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/sftp v1.13.5 // indirect
	github.com/pkg/xattr v0.4.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/fastuuid v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/samuel/go-zookeeper v0.0.0-20200724154423-2164a8ac840e // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/vishvananda/netlink v1.2.1-beta.2 // indirect
	github.com/vishvananda/netns v0.0.4
	github.com/wvanbergen/kazoo-go v0.0.0-20180202103751-f72d8611297a // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.8.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/errgo.v1 v1.0.1 // indirect
	gopkg.in/fatih/pool.v2 v2.0.0 // indirect
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/httprequest.v1 v1.2.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/macaroon-bakery.v3 v3.0.0 // indirect
	gopkg.in/macaroon.v2 v2.1.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.90.1 // indirect
	k8s.io/kube-openapi v0.0.0-20230501164219-8b0f38b5fd1f // indirect
	k8s.io/utils v0.0.0-20230308161112-d77c459e9343 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.22.3 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.1.0 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
