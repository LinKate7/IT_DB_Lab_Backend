package attributes

import (
	"fmt"
	"strconv"
)

type IntegerAttribute struct {
	name string
}

func NewIntegerAttribute(name string) *IntegerAttribute {
	return &IntegerAttribute{
		name: name,
	}
}

func (a *IntegerAttribute) Name() string {
	return a.name
}

func (a *IntegerAttribute) Type() string {
	return "integer"
}

func (a *IntegerAttribute) Validate(value string) error {
	_, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%v is not a valid integer", value)
	}
	return nil
}