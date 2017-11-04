package sybil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
)

var LOCK_US = time.Millisecond * 3
var LOCK_TRIES = 50
var MAX_LOCK_BREAKS = 5

func PutLocksInTestMode() {
	LOCK_US = 1
	LOCK_TRIES = 3
}

// Every LockFile should have a recovery plan
type RecoverableLock interface {
}

var BREAK_MAP = make(map[string]int, 0)

type Lock struct {
	Name   string
	Table  *Table
	broken bool
}

type InfoLock struct {
	Lock
}

type BlockLock struct {
	Lock
}

type CacheLock struct {
	Lock
}

type DigestLock struct {
	Lock
}

type LockRecoveryFunc func(lock RecoverableLock) bool

var THIS_RECOVER_FUNC = RecoverLock

func SetRecoverFunction(lf LockRecoveryFunc) {
	THIS_RECOVER_FUNC = lf

}

func RecoverLock(lock RecoverableLock) bool {
	// TODO: log the auto recovery into a recovery file
	return MissingRecoverLock()
}

func MissingRecoverLock() bool {
	Debug("UNIMPLEMENTED RECOVERY FOR LOCK")
	return false
}

func (l *Lock) ForceDeleteFile() {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*FLAGS.DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	Debug("FORCE DELETING", lockfile)
	os.RemoveAll(lockfile)
}

func (l *Lock) ForceMakeFile(pid int64) {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*FLAGS.DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	Debug("FORCE MAKING", lockfile)
	nf, err := os.Create(lockfile)
	if err != nil {
		nf, err = os.OpenFile(lockfile, os.O_CREATE, 0666)
	}

	defer nf.Close()

	nf.WriteString(strconv.FormatInt(pid, 10))
	nf.Sync()

}

func is_active_pid(val []byte) bool {
	// Check if its our PID or not...
	pid_str := strconv.FormatInt(int64(os.Getpid()), 10)
	if pid_str == string(val) {
		return true
	}

	return false
}

func check_if_broken(lockfile string, l *Lock) bool {
	var val []byte
	var err error
	// To check if a PID is active, we... first parse the PID in the file, then
	// we ask the os for the process and send it Signal 0. If the process is
	// alive, there will be no error, or if it isn't owned by us, we'll get an
	// EPERM error
	val, err = ioutil.ReadFile(lockfile)

	var pid_int = int64(0)
	if err == nil {
		pid_int, err = strconv.ParseInt(string(val), 10, 32)

		if err != nil {
			breaks, ok := BREAK_MAP[lockfile]
			if ok {
				breaks = breaks + 1
			} else {
				breaks = 1
			}

			BREAK_MAP[lockfile] = breaks

			Debug("CANT READ PID FROM LOCK:", lockfile, string(val), err, breaks)
			if breaks > MAX_LOCK_BREAKS {
				l.broken = true
				Debug("PUTTING LOCK INTO RECOVERY", lockfile)
			}
			return false
		}
	}

	if err == nil && pid_int != 0 {
		process, err := os.FindProcess(int(pid_int))
		// err is Always nil on *nix
		if err == nil {
			err := process.Signal(syscall.Signal(0))
			if err == nil || err == syscall.EPERM {
				// PROCESS IS STILL RUNNING
			} else {
				time.Sleep(time.Millisecond * 100)
				nextval, err := ioutil.ReadFile(lockfile)

				if err == nil {
					if string(nextval) == string(val) {
						if l.broken {
							Debug("SECOND TRY TO RECOVER A BROKEN LOCK... GIVING UP")
							l.broken = false
							return true
						}

						Debug("OWNER PROCESS IS DEAD, MARKING LOCK FOR RECOVERY", l.Name, val)
						l.broken = true
					}
				}
			}
		}
	}

	return false
}

func check_pid(lockfile string, l *Lock) bool {
	cangrab := false

	var val []byte
	var err error

	// check if the PID is active or not. If the PID isn't active, we enter
	// recovery mode for this Lock() and say it's grabbable
	if check_if_broken(lockfile, l) {
		return true
	}

	for i := 0; i < LOCK_TRIES; i++ {
		val, err = ioutil.ReadFile(lockfile)

		if err == nil {
			// Check if its our PID or not...
			if is_active_pid(val) {
				return true
			}

			time.Sleep(LOCK_US)
			continue
		} else {
			cangrab = true
			break
		}
	}

	return cangrab
}

func (l *Lock) Grab() bool {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*FLAGS.DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	var err error
	for i := 0; i < LOCK_TRIES; i++ {
		time.Sleep(LOCK_US)
		if check_pid(lockfile, l) == false {
			if l.broken {
				Debug("MARKING BROKEN LOCKFILE", lockfile)
				return false
			}

			continue
		}

		nf, er := os.Create(lockfile)
		if er != nil {
			err = er
			continue
		}

		defer nf.Close()

		pid := int64(os.Getpid())
		nf.WriteString(strconv.FormatInt(pid, 10))
		Debug("WRITING PID", pid, "TO LOCK", lockfile)
		nf.Sync()

		if check_pid(lockfile, l) == false {
			continue
		}

		Debug("LOCKING", lockfile)
		return true
	}

	Debug("CANT CREATE LOCK FILE:", err)
	Debug("LOCK FAIL!", lockfile)
	return false

}

func (l *Lock) Release() bool {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*FLAGS.DIR, t.Name, fmt.Sprintf("%s.lock", digest))
	for i := 0; i < LOCK_TRIES; i++ {
		val, err := ioutil.ReadFile(lockfile)

		if err != nil {
			continue
		}

		if is_active_pid(val) {
			Debug("UNLOCKING", lockfile)
			os.RemoveAll(lockfile)
			break
		}

	}

	return true
}

func GrabInfoLock(t *Table) bool {
	lock := Lock{Table: t, Name: "info"}
	info := &InfoLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = THIS_RECOVER_FUNC(info)
	}

	return ret
}

func ReleaseInfoLock(t *Table) bool {
	lock := Lock{Table: t, Name: "info"}
	info := &InfoLock{lock}
	ret := info.Release()
	return ret
}

func GrabDigestLock(t *Table) bool {
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = THIS_RECOVER_FUNC(info)
	}
	return ret
}

func ReleaseDigestLock(t *Table) bool {
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Release()
	return ret
}

func GrabBlockLock(t *Table, name string) bool {
	lock := Lock{Table: t, Name: name}
	info := &BlockLock{lock}
	ret := info.Grab()
	// INFO RECOVER IS GOING TO HAVE TIMING ISSUES... WHEN MULTIPLE THREADS ARE
	// AT PLAY
	if !ret && info.broken {
		ret = THIS_RECOVER_FUNC(info)
	}
	return ret

}

func ReleaseBlockLock(t *Table, name string) bool {
	lock := Lock{Table: t, Name: name}
	info := &BlockLock{lock}
	ret := info.Release()
	return ret
}

func GrabCacheLock(t *Table) bool {
	lock := Lock{Table: t, Name: CACHE_DIR}
	info := &CacheLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = THIS_RECOVER_FUNC(info)
	}
	return ret
}

func ReleaseCacheLock(t *Table) bool {
	lock := Lock{Table: t, Name: CACHE_DIR}
	info := &CacheLock{lock}
	ret := info.Release()
	return ret
}

func HasFlagFile(t *Table) bool {
	// Make a determination of whether this is a new table or not. if it is a
	// new table, we are fine, but if it's not - we are in trouble!
	flagfile := path.Join(*FLAGS.DIR, t.Name, "info.db.exists")
	_, err := os.Open(flagfile)
	// If the flagfile exists and we couldn't read the file info, we are in trouble!
	if err == nil {
		ReleaseInfoLock(t)
		Warn("Table info missing, but flag file exists!")
		return true
	}

	return false

}
