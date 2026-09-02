package metronome

// Recorder accumulates Results into an aggregate.
//
// *Stats, *RollingStats and *LabeledStats all satisfy it. It is the seam the
// per-label breakdown and the recorder combinators are written against, so a
// caller can substitute any of them — or their own — wherever one is taken.
//
// Window is deliberately absent: it is meaningful for *RollingStats and for
// nothing else, and the honest answers a lifetime aggregate could give (a zero
// Snapshot, or the lifetime one relabelled) are both worse than not being
// asked. LabeledStats recovers typed access to it by being generic over the
// child type instead.
//
// Bytes is absent for a different reason: a Recorder that writes CSV has no
// meaningful size to report, and should not owe one to satisfy an interface
// about recording. LabeledStats.Bytes reaches it through an optional interface.
type Recorder interface {
	Record(Result)
	Snapshot() Snapshot
}

// byteSizer is the optional interface LabeledStats.Bytes probes for. *Stats and
// *RollingStats satisfy it; a custom Recorder need not.
type byteSizer interface {
	Bytes() int64
}
