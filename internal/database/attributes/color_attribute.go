package attributes

import (
	"fmt"
	"regexp"
)

type ColorAttribute struct {
	name string
}

func NewColorAttribute(name string) *ColorAttribute {
	return &ColorAttribute{
		name: name,
	}
}

func (a *ColorAttribute) Name() string {
	return a.name
}

func (a *ColorAttribute) Type() string {
	return "color"
}

func (a *ColorAttribute) Validate(value string) error {
	// Regex pattern to match RGB color code in the form #RRGGBB
	rgbPattern := `^#[0-9A-Fa-f]{6}$`
	matched, err := regexp.MatchString(rgbPattern, value)
	if err != nil {
		return err
	}

	if !matched {
		return fmt.Errorf("%v is not a valid RGB color code", value)
	}

	return nil
}
