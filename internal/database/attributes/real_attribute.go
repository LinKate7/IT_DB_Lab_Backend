package attributes

import (
	"fmt"
	"strconv"
)

type RealAttribute struct {
	name string
}

func NewRealAttribute(name string) *RealAttribute {
	return &RealAttribute{
		name: name,
	}
}

func (a *RealAttribute) Name() string {
	return a.name
}

func (a *RealAttribute) Type() string {
	return "real"
}

func (a *RealAttribute) Validate(value string) error {
	_, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%v is not a valid real number", value)
	}
	return nil
}
