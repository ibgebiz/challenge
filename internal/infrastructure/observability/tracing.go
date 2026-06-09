package observability

import (
	"context"
	"net/url"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ShutdownFunc flushes and stops the tracer provider.
type ShutdownFunc func(context.Context) error

// InitTracing configures an OTLP/HTTP tracer provider for the given service. If
// endpoint is empty, tracing is disabled and a no-op shutdown is returned.
func InitTracing(ctx context.Context, serviceName, endpoint string) (ShutdownFunc, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
	if u.Scheme != "https" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
