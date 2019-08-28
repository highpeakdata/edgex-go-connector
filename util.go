package edgex

import "bytes"

// ArrToJSON - convert k/v pairs to json
func ArrToJSON(arr ...string) string {
	var b bytes.Buffer

	b.WriteString("{")
	n := 0
	for i := 0; i < len(arr); i += 2 {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString(" \"")
		b.WriteString(arr[i])
		b.WriteString("\": \"")
		b.WriteString(arr[i+1])
		b.WriteString("\"")
		n++
	}
	b.WriteString("}")

	return b.String()
}

// ArrToCVS - convert k/v pairs to cvs
func ArrToCVS(arr ...string) string {
	var b bytes.Buffer

	n := 0
	for i := 0; i < len(arr); i += 2 {
		if n > 0 {
			b.WriteString("\n")
		}
		b.WriteString(arr[i])
		b.WriteString(";")
		b.WriteString(arr[i+1])
		n++
	}

	return b.String()
}
