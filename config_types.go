package proxmox

import (
	"bytes"
	"encoding/json"
	"reflect"
)

type LaxStringList []string

func (o *LaxStringList) UnmarshalJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewBuffer(data))

	var v any
	if err := d.Decode(&v); err != nil {
		return err
	}

	switch typed := v.(type) {
	case string:
		*o = []string{typed}
	case []any:
		for _, itemI := range typed {
			item, ok := itemI.(string)
			if !ok {
				return &json.UnmarshalTypeError{Value: string(data), Type: reflect.TypeOf(*o)}
			}
			*o = append(*o, item)
		}
	default:
		return &json.UnmarshalTypeError{Value: string(data), Type: reflect.TypeOf(*o)}
	}

	return nil
}
