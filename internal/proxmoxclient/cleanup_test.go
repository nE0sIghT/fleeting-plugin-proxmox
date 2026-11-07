package proxmoxclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVMIDAvailable(t *testing.T) {
	for _, tc := range []struct {
		name, body      string
		status          int
		free, wantError bool
	}{
		{"string", `{"data":"5000"}`, 200, true, false},
		{"number", `{"data":5000}`, 200, true, false},
		{"occupied", `{"errors":{"vmid":"VM 5000 already exists"},"data":null}`, 400, false, false},
		{"permission", `{"data":null}`, 403, false, true},
		{"wrong id", `{"data":5001}`, 200, false, true},
		{"missing", `{"data":null}`, 200, false, true},
		{"other validation error", `{"errors":{"vmid":"invalid VMID"}}`, 400, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api2/json/cluster/nextid", r.URL.Path)
				require.Equal(t, "5000", r.URL.Query().Get("vmid"))
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := New(Config{BaseURL: server.URL})
			require.NoError(t, err)
			free, err := client.VMIDAvailable(context.Background(), 5000)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.free, free)
			}
		})
	}
}
