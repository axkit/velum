package velum

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// NullTime defines types for nullable varchar column without additional "Valid bool" attributes.
type NullTime time.Time

// Value implements interface sql.Valuer
func (ns NullTime) Value() (driver.Value, error) {
	if !ns.Valid() {
		return nil, nil
	}

	return time.Time(ns), nil
}

// Valid return true if value is not empty or false, if len==0.
func (ns NullTime) Valid() bool {
	return !time.Time(ns).IsZero()
}

// Scan implements database/sql Scanner interface.
func (ns *NullTime) Scan(value any) error {
	if value == nil {
		*ns = NullTime{}
		return nil
	}

	fmt.Printf("NullTime.Scan: value: %T (%q)\n", value, value)

	v, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("NullTime.Scan: expected time.Time, got %T (%q)", value, value)
	}
	if v.IsZero() {
		*ns = NullTime{}
		return nil
	}

	*ns = NullTime(v)
	return nil
}

// String returns string representation of value.
func (ns NullTime) String() string {
	return time.Time(ns).String()
}

// UnmarshalJSON implements encoding/json Unmarshaller interface.
func (ns *NullTime) UnmarshalJSON(b []byte) error {

	if len(b) == 0 || string(b) == "null" {
		*ns = NullTime{}
		return nil
	}
	var t time.Time

	err := json.Unmarshal(b, &t)
	if err != nil {
		return err
	}
	*ns = NullTime(t)
	return nil
}

// UnmarshalText implements encoding/text TextUnmarshaller interface.
func (ns *NullTime) UnmarshalText(b []byte) error {
	return ns.UnmarshalJSON(b)
}

// MarshalText implements encoding/text TextMarshaller interface.
func (ns NullTime) MarshalText(b []byte) ([]byte, error) {
	return ns.MarshalJSON()
}

// MarshalJSON implements encoding/json Marshaller interface.
func (ns NullTime) MarshalJSON() ([]byte, error) {
	if time.Time(ns).IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(time.Time(ns))
}

// SetNow sets value to current time.
func (ns *NullTime) SetNow() {
	now := time.Now()
	fmt.Printf("SetNow: %s\n", now)
	*ns = NullTime(now)
}

// SetNull sets value to null.
func (ns *NullTime) SetNull() {
	*ns = NullTime{}
}

func (ns NullTime) T() time.Time {
	return time.Time(ns)
}
