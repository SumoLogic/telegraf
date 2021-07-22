# Carbon2

The carbon2 parser parses the incoming metrics in [`carbon2` format][metrics20].

**NOTE:** All tags (both `intrinsic_tags` and `meta_tags` are treated as telegraf
tags hance parsing and then serializing a metric will yield a different metric
than was ingested because of data model.

[metrics20]: http://metrics20.org/implementations/

## Configuration

```toml
[[inputs.file]]
  files = ["example_carbon2_file"]

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "carbon2"
