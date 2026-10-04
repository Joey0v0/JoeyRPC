package joeyrpc

// RemoteError is an error carried by a server response, not a transport failure.
type RemoteError struct{ Message string }

func (e *RemoteError) Error() string { return e.Message }

// TransportError retains the underlying I/O error for errors.Is/errors.As.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }
