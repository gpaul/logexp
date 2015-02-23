package logexp

type ByteFilter func(b []byte) (bool, error)

func AnyFilter(b []byte) (bool, error) {
	return true, nil
}
