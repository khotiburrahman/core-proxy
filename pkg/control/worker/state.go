package worker

type State int32

const (
	StateCreated State = iota
	StateStarting
	StateConnecting
	StateConnected
	StateReady
	StateRunning
	StateDegraded
	StateReconnecting
	StateStopping
	StateStopped
)

func (s State) String() string {
	switch s {
	case StateCreated:
		return "CREATED"
	case StateStarting:
		return "STARTING"
	case StateConnecting:
		return "CONNECTING"
	case StateConnected:
		return "CONNECTED"
	case StateReady:
		return "READY"
	case StateRunning:
		return "RUNNING"
	case StateDegraded:
		return "DEGRADED"
	case StateReconnecting:
		return "RECONNECTING"
	case StateStopping:
		return "STOPPING"
	case StateStopped:
		return "STOPPED"
	default:
		return "UNKNOWN"
	}
}

