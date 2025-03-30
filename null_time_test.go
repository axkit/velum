package velum

import (
	"bytes"
	"database/sql/driver"
	"testing"
	"time"
)

func TestNullTime_Valid(t *testing.T) {
	tests := []struct {
		name string
		ns   NullTime
		want bool
	}{
		{
			name: "Valid time",
			ns:   NullTime(time.Now()),
			want: true,
		},
		{
			name: "Zero time",
			ns:   NullTime{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ns.Valid(); got != tt.want {
				t.Errorf("NullTime.Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNullTime_Value(t *testing.T) {
	tests := []struct {
		name    string
		ns      NullTime
		want    driver.Value
		wantErr bool
	}{
		{
			name:    "Valid time",
			ns:      NullTime(time.Now()),
			want:    time.Now().Format(time.RFC3339), // Match format
			wantErr: false,
		},
		{
			name:    "Zero time",
			ns:      NullTime{},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ns.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("NullTime.Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil && got.(time.Time).IsZero() {
				t.Errorf("NullTime.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNullTime_Scan(t *testing.T) {

	now := time.Now()
	tests := []struct {
		name    string
		value   any
		want    NullTime
		wantErr bool
	}{
		{
			name:    "Valid time",
			value:   now,
			want:    NullTime(now),
			wantErr: false,
		},
		{
			name:    "Nil value",
			value:   nil,
			want:    NullTime{},
			wantErr: false,
		},
		{
			name:    "Invalid type",
			value:   "invalid",
			want:    NullTime{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ns NullTime
			err := ns.Scan(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("NullTime.Scan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !time.Time(ns).Equal(time.Time(tt.want)) {
				t.Errorf("NullTime.Scan() = %v, want %v", ns, tt.want)
			}
		})
	}
}

func TestNullTime_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		ns      NullTime
		want    []byte
		wantErr bool
	}{
		{
			name:    "Valid time",
			ns:      NullTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
			want:    []byte(`"2023-01-01T00:00:00Z"`),
			wantErr: false,
		},
		{
			name:    "Zero time",
			ns:      NullTime{},
			want:    []byte("null"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ns.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("NullTime.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("NullTime.MarshalJSON() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNullTime_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    NullTime
		wantErr bool
	}{
		{
			name:    "Valid time",
			input:   []byte(`"2023-01-01T00:00:00Z"`),
			want:    NullTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: false,
		},
		{
			name:    "Null value",
			input:   []byte("null"),
			want:    NullTime{},
			wantErr: false,
		},
		{
			name:    "Invalid JSON",
			input:   []byte(`"invalid"`),
			want:    NullTime{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ns NullTime
			err := ns.UnmarshalJSON(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("NullTime.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !time.Time(ns).Equal(time.Time(tt.want)) {
				t.Errorf("NullTime.UnmarshalJSON() = %v, want %v", ns, tt.want)
			}
		})
	}
}

func TestNullTime_SetNow(t *testing.T) {
	var ns NullTime
	ns.SetNow()
	if ns.T().IsZero() {
		t.Errorf("NullTime.SetNow() did not set the current time")
	}
}

func TestNullTime_SetNull(t *testing.T) {
	ns := NullTime(time.Now())
	ns.SetNull()
	if !ns.T().IsZero() {
		t.Errorf("NullTime.SetNull() did not set the time to null")
	}
}

func TestNullTime_T(t *testing.T) {
	now := time.Now()
	ns := NullTime(now)
	if !ns.T().Equal(now) {
		t.Errorf("NullTime.T() = %v, want %v", ns.T(), now)
	}
}
