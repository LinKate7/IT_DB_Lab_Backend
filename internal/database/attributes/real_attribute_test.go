package attributes

import "testing"

func TestColorAttribute_Validate(t *testing.T) {
	tests := []struct {
		color     string
		expectErr bool
	}{
		{"#FFFFFF", false}, // Valid white color
		{"#000000", false}, // Valid black color
		{"#FF5733", false}, // Valid color
		{"#123456", false}, // Valid color
		{"#ABCDEF", false}, // Valid color
		{"#GHIJKL", true},  // Invalid color: non-hex characters
		{"#ZZZZZZ", true},  // Invalid color: non-hex characters
		{"FFFFFF", true},   // Invalid: missing hash
		{"#12345", true},   // Invalid: too short
		{"#1234567", true}, // Invalid: too long
	}

	colorAttr := NewColorAttribute("test_color")

	for _, tt := range tests {
		t.Run(tt.color, func(t *testing.T) {
			err := colorAttr.Validate(tt.color)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v for color %v", tt.expectErr, err != nil, tt.color)
			}
		})
	}
}
