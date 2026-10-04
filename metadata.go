package joeyrpc

import "context"

// Metadata carries per-request information separately from business arguments.
type Metadata map[string]string

type metadataKey struct{}

func copyMetadata(md map[string]string) Metadata {
	if md == nil {
		return nil
	}
	copy := make(Metadata, len(md))
	for key, value := range md {
		copy[key] = value
	}
	return copy
}

// WithMetadata takes a snapshot; callers may reuse their map after this returns.
func WithMetadata(ctx context.Context, md Metadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, copyMetadata(md))
}

// MetadataFromContext returns a copy so readers cannot change other calls' data.
func MetadataFromContext(ctx context.Context) Metadata {
	md, _ := ctx.Value(metadataKey{}).(Metadata)
	return copyMetadata(md)
}
