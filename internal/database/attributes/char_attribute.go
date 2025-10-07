package attributes

import (
	"fmt"
)

type CharAttribute struct {
	name string
}

func NewCharAttribute(name string) *CharAttribute {
	return &CharAttribute{
		name: name,
	}
}

func (a *CharAttribute) Name() string {
	return a.name
}

func (a *CharAttribute) Type() string {
	return "char"
}

func (a *CharAttribute) Validate(value string) error {
	if len(value) != 1 {
		return fmt.Errorf("%v is not char", value)
	}

	return nil
}
