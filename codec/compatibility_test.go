package codec

import (
	"bytes"
	"encoding/gob"
	"testing"
)

type legacyHeader struct {
	ServiceMethod string
	Seq           uint64
	Error         string
}

func TestGobHeaderMetadataCompatibility(t *testing.T) {
	for _, direction := range []string{"old-to-new", "new-to-old"} {
		t.Run(direction, func(t *testing.T) {
			var wire bytes.Buffer
			encoder := gob.NewEncoder(&wire)
			old := legacyHeader{ServiceMethod: "Service.Method", Seq: 7}
			current := Header{ServiceMethod: old.ServiceMethod, Seq: old.Seq, Metadata: map[string]string{"traceID": "trace"}}
			var source interface{} = old
			if direction == "old-to-new" {
				current = Header{}
			}
			var target interface{} = &current
			if direction == "new-to-old" {
				source, target = current, &old
			}
			if err := encoder.Encode(source); err != nil {
				t.Fatal(err)
			}
			if err := encoder.Encode(42); err != nil {
				t.Fatal(err)
			}
			decoder := gob.NewDecoder(&wire)
			if err := decoder.Decode(target); err != nil {
				t.Fatal(err)
			}
			if direction == "old-to-new" && current.Metadata != nil {
				t.Fatal("old peer unexpectedly supplied metadata")
			}
			var body int
			if err := decoder.Decode(&body); err != nil || body != 42 || old.Seq != 7 || current.Seq != 7 {
				t.Fatalf("header/body compatibility: %d %v", body, err)
			}
		})
	}
}
