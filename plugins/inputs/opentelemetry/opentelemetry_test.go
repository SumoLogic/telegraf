package opentelemetry

// func TestOpenTelemetry(t *testing.T) {
// 	mockListener := bufconn.Listen(1024 * 1024)
// 	plugin := inputs.Inputs["opentelemetry"]().(*OpenTelemetry)
// 	plugin.listener = mockListener
// 	accumulator := new(testutil.Accumulator)

// 	err := plugin.Start(accumulator)
// 	require.NoError(t, err)
// 	t.Cleanup(plugin.Stop)

// 	metricExporter, err := otlpmetricgrpc.New(context.Background(),
// 		otlpmetricgrpc.WithInsecure(),
// 		otlpmetricgrpc.WithDialOption(
// 			grpc.WithBlock(),
// 			grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
// 				return mockListener.Dial()
// 			})),
// 	)
// 	require.NoError(t, err)
// 	t.Cleanup(func() { _ = metricExporter.Shutdown(context.Background()) })

// 	pusher := controller.New(
// 		processor.New(
// 			simple.NewWithExactDistribution(),
// 			metricExporter,
// 		),
// 		controller.WithExporter(metricExporter),
// 	)

// 	err = pusher.Start(context.Background())
// 	require.NoError(t, err)
// 	t.Cleanup(func() { _ = pusher.Stop(context.Background()) })

// 	global.SetMeterProvider(pusher.MeterProvider())

// 	// write metrics
// 	meter := global.Meter("library-name")
// 	counter := metric.Must(meter).NewInt64Counter("measurement-counter")
// 	meter.RecordBatch(context.Background(), nil, counter.Measurement(7))

// 	err = pusher.Stop(context.Background())
// 	require.NoError(t, err)

// 	// Shutdown

// 	plugin.Stop()

// 	err = metricExporter.Shutdown(context.Background())
// 	require.NoError(t, err)

// 	// Check

// 	assert.Empty(t, accumulator.Errors)

// 	if assert.Len(t, accumulator.Metrics, 1) {
// 		got := accumulator.Metrics[0]
// 		assert.Equal(t, "measurement-counter", got.Measurement)
// 		assert.Equal(t, telegraf.Counter, got.Type)
// 		assert.Equal(t, "library-name", got.Tags["otel.library.name"])
// 	}
// }
