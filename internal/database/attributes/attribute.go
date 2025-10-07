package attributes

type Attribute interface {
	Name() string
	Type() string
	Validate(string) error
}
