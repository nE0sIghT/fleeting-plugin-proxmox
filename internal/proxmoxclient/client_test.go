package proxmoxclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringListUnmarshalCommaSeparated(t *testing.T) {
	t.Parallel()

	var values StringList
	err := json.Unmarshal([]byte(`"pve1,pve2, pve3"`), &values)
	require.NoError(t, err)
	require.Equal(t, StringList{"pve1", "pve2", "pve3"}, values)
}
