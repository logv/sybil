package sybil

import "path"
import "os"
import "syscall"
import "fmt"
import "strconv"
import "io/ioutil"
import "time"

var LOCK_US = time.Millisecond * 3
var LOCK_TRIES = 50
var MAX_LOCK_BREAKS = 5

// Every LockFile should have a recovery plan
type RecoverableLock interface {
	Grab(flags *FlagDefs) bool
	Release(flags *FlagDefs) bool
	Recover(flags *FlagDefs) bool
}

var BREAK_MAP = make(map[string]int)

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

func RecoverLock(flags *FlagDefs, lock RecoverableLock) bool {
	// TODO: log the auto recovery into a recovery file
	return lock.Recover(flags)
}

func (l *InfoLock) Recover(flags *FlagDefs) bool {
	t := l.Lock.Table
	Debug("INFO LOCK RECOVERY")
	dirname := path.Join(*flags.DIR, t.Name)
	backup := path.Join(dirname, "info.bak")
	infodb := path.Join(dirname, "info.db")

	if t.LoadTableInfoFrom(infodb) {
		Debug("LOADED REASONABLE TABLE INFO, DELETING LOCK")
		l.ForceDeleteFile(flags)
		return true
	}

	if t.LoadTableInfoFrom(backup) {
		Debug("LOADED TABLE INFO FROM BACKUP, RESTORING BACKUP")
		os.Remove(infodb)
		RenameAndMod(backup, infodb)
		l.ForceDeleteFile(flags)
		return l.Grab(flags)
	}

	Debug("CANT READ info.db OR RECOVER info.bak")
	Debug("TRY DELETING LOCK BY HAND FOR", l.Name)

	return false
}

func (l *DigestLock) Recover(flags *FlagDefs) bool {
	Debug("RECOVERING DIGEST LOCK", l.Name)
	t := l.Table
	ingestdir := path.Join(*flags.DIR, t.Name, INGEST_DIR)

	os.MkdirAll(ingestdir, 0777)
	// TODO: understand if any file in particular is messing things up...
	pid := int64(os.Getpid())
	l.ForceMakeFile(flags, pid)
	t.RestoreUningestedFiles(flags)
	l.ForceDeleteFile(flags)

	return true
}

func (l *BlockLock) Recover(flags *FlagDefs) bool {
	Debug("RECOVERING BLOCK LOCK", l.Name)
	t := l.Table
	tb := t.LoadBlockFromDir(flags, l.Name, nil, true)
	if tb == nil || tb.Info == nil || tb.Info.NumRecords <= 0 {
		Debug("BLOCK IS NO GOOD, TURNING IT INTO A BROKEN BLOCK")
		// This block is not good! need to put it into remediation...
		RenameAndMod(l.Name, fmt.Sprint(l.Name, ".broke"))
		l.ForceDeleteFile(flags)
	} else {
		Debug("BLOCK IS FINE, TURNING IT BACK INTO A REAL BLOCK")
		os.RemoveAll(fmt.Sprint(l.Name, ".partial"))
		l.ForceDeleteFile(flags)
	}

	return true
}

func (l *CacheLock) Recover(flags *FlagDefs) bool {
	Debug("RECOVERING BLOCK LOCK", l.Name)
	t := l.Table
	files, err := ioutil.ReadDir(path.Join(*flags.DIR, t.Name, CACHE_DIR))

	if err != nil {
		l.ForceDeleteFile(flags)
		return true
	}

	for _, blockFile := range files {
		filename := path.Join(*flags.DIR, t.Name, CACHE_DIR, blockFile.Name())
		blockCache := SavedBlockCache{}

		err := decodeInto(filename, &blockCache)
		if err != nil {
			os.RemoveAll(filename)
			continue
		}

		if err != nil {
			os.RemoveAll(filename)
			Debug("DELETING BAD CACHE FILE", filename)

		}

	}

	l.ForceDeleteFile(flags)

	return true

}

func (l *Lock) Recover() bool {
	Debug("UNIMPLEMENTED RECOVERY FOR LOCK", l.Table.Name, l.Name)
	return false
}

func (l *Lock) ForceDeleteFile(flags *FlagDefs) {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*flags.DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	Debug("FORCE DELETING", lockfile)
	os.RemoveAll(lockfile)
}

func (l *Lock) ForceMakeFile(flags *FlagDefs, pid int64) {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*flags.DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	Debug("FORCE MAKING", lockfile)
	nf, err := os.Create(lockfile)
	if err != nil {
		nf, _ = os.OpenFile(lockfile, os.O_CREATE, 0666)
	}

	defer nf.Close()

	nf.WriteString(strconv.FormatInt(pid, 10))
	nf.Sync()

}

func isActivePid(val []byte) bool {
	// Check if its our PID or not...
	pidStr := strconv.FormatInt(int64(os.Getpid()), 10)
	return pidStr == string(val)
}

func checkIfBroken(lockfile string, l *Lock) bool {
	var val []byte
	var err error
	// To check if a PID is active, we... first parse the PID in the file, then
	// we ask the os for the process and send it Signal 0. If the process is
	// alive, there will be no error, or if it isn't owned by us, we'll get an
	// EPERM error
	val, err = ioutil.ReadFile(lockfile)

	var pidInt = int64(0)
	if err == nil {
		pidInt, err = strconv.ParseInt(string(val), 10, 32)

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

	if err == nil && pidInt != 0 {
		process, err := os.FindProcess(int(pidInt))
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

func checkPid(lockfile string, l *Lock) bool {
	cangrab := false

	var val []byte
	var err error

	// check if the PID is active or not. If the PID isn't active, we enter
	// recovery mode for this Lock() and say it's grabbable
	if checkIfBroken(lockfile, l) {
		return true
	}

	for i := 0; i < LOCK_TRIES; i++ {
		val, err = ioutil.ReadFile(lockfile)

		if err == nil {
			// Check if its our PID or not...
			if isActivePid(val) {
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

func (l *Lock) Grab(flags *FlagDefs) bool {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*flags.DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	var err error
	for i := 0; i < LOCK_TRIES; i++ {
		time.Sleep(LOCK_US)
		if !checkPid(lockfile, l) {
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

		if !checkPid(lockfile, l) {
			continue
		}

		Debug("LOCKING", lockfile)
		return true
	}

	Debug("CANT CREATE LOCK FILE:", err)
	Debug("LOCK FAIL!", lockfile)
	return false

}

func (l *Lock) Release(flags *FlagDefs) bool {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*flags.DIR, t.Name, fmt.Sprintf("%s.lock", digest))
	for i := 0; i < LOCK_TRIES; i++ {
		val, err := ioutil.ReadFile(lockfile)

		if err != nil {
			continue
		}

		if isActivePid(val) {
			Debug("UNLOCKING", lockfile)
			os.RemoveAll(lockfile)
			break
		}

	}

	return true
}

func (t *Table) GrabInfoLock(flags *FlagDefs) bool {
	lock := Lock{Table: t, Name: "info"}
	info := &InfoLock{lock}
	ret := info.Grab(flags)
	if !ret && info.broken {
		ret = RecoverLock(flags, info)
	}

	return ret
}

func (t *Table) ReleaseInfoLock(flags *FlagDefs) bool {
	lock := Lock{Table: t, Name: "info"}
	info := &InfoLock{lock}
	ret := info.Release(flags)
	return ret
}

func (t *Table) GrabDigestLock(flags *FlagDefs) bool {
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Grab(flags)
	if !ret && info.broken {
		ret = RecoverLock(flags, info)
	}
	return ret
}

func (t *Table) ReleaseDigestLock(flags *FlagDefs) bool {
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Release(flags)
	return ret
}

func (t *Table) GrabBlockLock(flags *FlagDefs, name string) bool {
	lock := Lock{Table: t, Name: name}
	info := &BlockLock{lock}
	ret := info.Grab(flags)
	// INFO RECOVER IS GOING TO HAVE TIMING ISSUES... WHEN MULTIPLE THREADS ARE
	// AT PLAY
	if !ret && info.broken {
		ret = RecoverLock(flags, info)
	}
	return ret

}

func (t *Table) ReleaseBlockLock(flags *FlagDefs, name string) bool {
	lock := Lock{Table: t, Name: name}
	info := &BlockLock{lock}
	ret := info.Release(flags)
	return ret
}

func (t *Table) GrabCacheLock(flags *FlagDefs) bool {
	lock := Lock{Table: t, Name: CACHE_DIR}
	info := &CacheLock{lock}
	ret := info.Grab(flags)
	if !ret && info.broken {
		ret = RecoverLock(flags, info)
	}
	return ret
}

func (t *Table) ReleaseCacheLock(flags *FlagDefs) bool {
	lock := Lock{Table: t, Name: CACHE_DIR}
	info := &CacheLock{lock}
	ret := info.Release(flags)
	return ret
}
