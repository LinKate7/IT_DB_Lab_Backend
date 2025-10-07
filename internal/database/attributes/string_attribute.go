package attributes

type StringAttribute struct {
	name string
}

func NewStringAttribute(name string) *StringAttribute {
	return &StringAttribute{
		name: name,
	}
}

func (a *StringAttribute) Name() string {
	return a.name
}

func (a *StringAttribute) Type() string {
	return "string"
}

func (a *StringAttribute) Validate(value string) error {
	// Since the input is always a string, no validation is required.
	return nil
}
