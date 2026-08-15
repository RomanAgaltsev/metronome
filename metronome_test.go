package metronome

import "testing"

func TestHello(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{name: "named", in: "world", want: "hello, world"},
		{name: "empty", in: "", want: "hello, "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Hello(tc.in); got != tc.want {
				t.Errorf("Hello(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
