package angora

type State int

const (
	Created State = iota
	Running
	Closing
	Closed
)

func (st State) String() string {
	switch st {
	case Created:
		return "created"
	case Running:
		return "running"
	case Closing:
		return "closing"
	case Closed:
		return "closed"
	default:
		return "unknown"
	}
}
