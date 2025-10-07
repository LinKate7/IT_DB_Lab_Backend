package attributes

import "fmt"

func CreateAttribute(attr_type string, attr_name string) (Attribute, error) {
	switch attr_type {
	case "char":
		return NewCharAttribute(attr_name), nil
	case "integer":
		return NewIntegerAttribute(attr_name), nil
	case "string":
		return NewStringAttribute(attr_name), nil
	case "real":
		return NewRealAttribute(attr_name), nil
	case "color":
		return NewColorAttribute(attr_name), nil
	default:
		return nil, fmt.Errorf("type %v not found", attr_type)
	}
}
