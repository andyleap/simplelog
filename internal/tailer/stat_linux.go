//go:build linux

package tailer

import "golang.org/x/sys/unix"

// statBirthNano returns the file's birth (creation) time in unix nanoseconds.
// Unlike ctime/mtime, btime is stable across appends, so it correctly
// identifies a file generation: a rotated/replaced file at the same path gets a
// fresh btime (and usually a fresh inode), while ongoing writes don't churn it.
// Returns 0 when the filesystem doesn't report btime (identity then falls back
// to dev+inode, which still distinguishes rotation-by-recreate).
func statBirthNano(path string) int64 {
	var stx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, 0, unix.STATX_BTIME, &stx); err != nil {
		return 0
	}
	if stx.Mask&unix.STATX_BTIME == 0 {
		return 0
	}
	return stx.Btime.Sec*1e9 + int64(stx.Btime.Nsec)
}
