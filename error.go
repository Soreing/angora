package angora

type Error struct {
	Id          int
	Message     string
	Description string
}

func (err *Error) Error() string {
	return err.Message
}

func NewConnectionClosedError() *Error {
	return &Error{
		Id:          1000,
		Message:     "ConnectionClosed",
		Description: "the connection is closing or already closed",
	}
}

func NewContextClosedError() *Error {
	return &Error{
		Id:          2000,
		Message:     "ContextClosed",
		Description: "the context is closing or already closed",
	}
}

func NewDispatchBufferFullError() *Error {
	return &Error{
		Id:          2100,
		Message:     "DispatchBufferFull",
		Description: "the buffered dispatch queue is full",
	}
}
