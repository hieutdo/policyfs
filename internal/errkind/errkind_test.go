package errkind

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKindError_shouldExposeStableMsgKindAndCause verifies KindError preserves a stable message,
// supports errors.Is matching against Kind, and unwraps to Cause.
func TestKindError_shouldExposeStableMsgKindAndCause(t *testing.T) {
	cause := errors.New("cause")
	err := &KindError{Kind: ErrInvalid, Msg: "bad input", Cause: cause}

	require.Equal(t, "bad input", err.Error())
	require.ErrorIs(t, err, ErrInvalid)
	require.NotErrorIs(t, err, ErrRequired)
	require.ErrorIs(t, err, cause)
	require.Same(t, cause, errors.Unwrap(err))
}

// TestKindError_nilReceiver_shouldBeSafe verifies a nil receiver behaves safely even when stored
// in a non-nil error interface.
func TestKindError_nilReceiver_shouldBeSafe(t *testing.T) {
	var ke *KindError
	var err error = ke
	require.Equal(t, "", err.Error())
	require.False(t, errors.Is(err, ErrInvalid))
	require.Nil(t, errors.Unwrap(err))
}

// TestRequiredError_shouldFormatAndMatchKind verifies RequiredError formatting and errors.Is kind matching.
func TestRequiredError_shouldFormatAndMatchKind(t *testing.T) {
	cases := []struct {
		name string
		err  *RequiredError
		want string
	}{
		{name: "should format default", err: &RequiredError{}, want: "required"},
		{name: "should format with what", err: &RequiredError{What: "mount name"}, want: "mount name is required"},
		{name: "should prefer custom msg", err: &RequiredError{What: "mount name", Msg: "custom"}, want: "custom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.err.Error())
			require.ErrorIs(t, tc.err, ErrRequired)
		})
	}
}

// TestInvalidError_shouldFormatUnwrapAndMatchKind verifies InvalidError formatting, unwrap behavior,
// and errors.Is kind matching.
func TestInvalidError_shouldFormatUnwrapAndMatchKind(t *testing.T) {
	cause := errors.New("cause")

	cases := []struct {
		name string
		err  *InvalidError
		want string
	}{
		{name: "should format default", err: &InvalidError{}, want: "invalid"},
		{name: "should format with what", err: &InvalidError{What: "config"}, want: "invalid config"},
		{name: "should prefer custom msg", err: &InvalidError{What: "config", Msg: "custom"}, want: "custom"},
		{name: "should unwrap cause", err: &InvalidError{Msg: "custom", Cause: cause}, want: "custom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.err.Error())
			require.ErrorIs(t, tc.err, ErrInvalid)
			if tc.err.Cause != nil {
				require.Same(t, tc.err.Cause, errors.Unwrap(tc.err))
				require.ErrorIs(t, tc.err, tc.err.Cause)
			} else {
				require.Nil(t, errors.Unwrap(tc.err))
			}
		})
	}
}

// TestNotFoundError_shouldFormatAndMatchKind verifies NotFoundError formatting and errors.Is kind matching.
func TestNotFoundError_shouldFormatAndMatchKind(t *testing.T) {
	cases := []struct {
		name string
		err  *NotFoundError
		want string
	}{
		{name: "should format default", err: &NotFoundError{}, want: "not found"},
		{name: "should format with what", err: &NotFoundError{What: "mount"}, want: "mount not found"},
		{name: "should prefer custom msg", err: &NotFoundError{What: "mount", Msg: "custom"}, want: "custom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.err.Error())
			require.ErrorIs(t, tc.err, ErrNotFound)
		})
	}
}

// TestBusyError_shouldFormatAndMatchKind verifies BusyError formatting and errors.Is kind matching.
func TestBusyError_shouldFormatAndMatchKind(t *testing.T) {
	cases := []struct {
		name string
		err  *BusyError
		want string
	}{
		{name: "should format default", err: &BusyError{}, want: "busy"},
		{name: "should format with what", err: &BusyError{What: "job lock"}, want: "job lock is busy"},
		{name: "should prefer custom msg", err: &BusyError{What: "job lock", Msg: "custom"}, want: "custom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.err.Error())
			require.ErrorIs(t, tc.err, ErrBusy)
		})
	}
}

// TestNilError_shouldFormatAndMatchKind verifies NilError formatting and errors.Is kind matching.
func TestNilError_shouldFormatAndMatchKind(t *testing.T) {
	cases := []struct {
		name string
		err  *NilError
		want string
	}{
		{name: "should format default", err: &NilError{}, want: "nil"},
		{name: "should format with what", err: &NilError{What: "router"}, want: "router is nil"},
		{name: "should prefer custom msg", err: &NilError{What: "router", Msg: "custom"}, want: "custom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.err.Error())
			require.ErrorIs(t, tc.err, ErrNil)
		})
	}
}
