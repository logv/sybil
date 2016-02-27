package sybil

import "log"
import "path"
import "os"
import "syscall"
import "fmt"
import "strconv"
import "io/ioutil"
import "time"

var LOCK_US = time.Millisecond * 3
var LOCK_TRIES = 50

// Every LockFile should have a recovery plan
type RecoverableLock interface {
	Grab() bool
	Release() bool
	Recover() bool
}

type Lock struct {
	name   string
	table  *Table
	broken bool
}

type InfoLock struct {
	Lock
}

type BlockLock struct {
	Lock
}

type DigestLock struct {
	Lock
}

func RecoverLock(lock RecoverableLock) bool {
	// TODO: log the auto recovery into a recovery file
	return lock.Recover()
}

func (l *InfoLock) Recover() bool {
	t := l.Lock.table
	log.Println("INFO LOCK RECOVERY")
	dirname := path.Join(*f_DIR, t.Name)
	backup := path.Join(dirname, "info.bak")
	infodb := path.Join(dirname, "info.db")

	if t.LoadTableInfoFrom(infodb) {
		log.Println("LOADED REASONABLE TABLE INFO, DELETING LOCK")
		l.ForceDeleteFile()
		return true
	}

	if t.LoadTableInfoFrom(backup) {
		log.Println("LOADED TABLE INFO FROM BACKUP, RESTORING BACKUP")
		os.Remove(infodb)
		os.Rename(backup, infodb)
		l.ForceDeleteFile()
		return l.Grab()
	}

	log.Println("CANT READ info.db OR RECOVER info.bak")
	log.Println("TRY DELETING LOCK BY HAND FOR", l.name)

	return false
}

func (l *DigestLock) Recover() bool {
	log.Println("RECOVERING DIGEST LOCK", l.name)
	t := l.table
	ingestdir := path.Join(*f_DIR, t.Name, INGEST_DIR)

	os.MkdirAll(ingestdir, 0777)
	// TODO: understand if any file in particular is messing things up...
	t.RestoreUningestedFiles()
	l.ForceDeleteFile()

	return true
}

func (l *BlockLock) Recover() bool {
	log.Println("RECOVERING BLOCK LOCK", l.name)
	t := l.table
	tb := t.LoadBlockFromDir(l.name, nil, true)
	if tb == nil {
		log.Println("BLOCK IS NO GOOD, TURNING IT INTO A BROKEN BLOCK")
		// This block is not good! need to put it into remediation...
		os.Rename(l.name, fmt.Sprint(l.name, ".broke"))
		l.ForceDeleteFile()
	} else {
		log.Println("BLOCK IS FINE, TURNING IT BACK INTO A REAL BLOCK")
		os.RemoveAll(fmt.Sprint(l.name, ".partial"))
		l.ForceDeleteFile()
	}

	return true
}

func (l *Lock) Recover() bool {
	log.Println("UNIMPLEMENTED RECOVERY FOR LOCK", l.table.Name, l.name)
	return false
}

func (l *Lock) ForceDeleteFile() {
	t := l.table
	digest := l.name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*f_DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	log.Println("FORCE DELETING", lockfile)
	os.RemoveAll(lockfile)
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

		if err != nil && string(val) != "" {
			l.broken = true
			log.Println("CANT READ PID FROM INFO LOCK:", string(val), err)
			log.Println("PUTTING INFO LOCK INTO RECOVERY")
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
							log.Println("SECOND TRY TO RECOVER A BROKEN LOCK... GIVING UP")
							l.broken = false
							return true
						}

						log.Println("OWNER PROCESS IS DEAD, MARKING LOCK FOR RECOVERY", l.name, val)
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
		dirname := path.Dir(lockfile)
		os.MkdirAll(dirname, 0777)
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
	t := l.table
	digest := l.name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*f_DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	if check_pid(lockfile, l) == false {
		return false
	}

	for i := 0; i < LOCK_TRIES; i++ {
		time.Sleep(LOCK_US)

		nf, err := os.Create(lockfile)
		if err != nil {
			continue
		}

		defer nf.Close()

		pid := int64(os.Getpid())
		nf.WriteString(strconv.FormatInt(pid, 10))
		nf.Sync()

		if check_pid(lockfile, l) == false {
			continue
		}

		log.Println("LOCKING", lockfile)
		return true
	}

	log.Println("LOCK FAIL!", lockfile)
	return false

}

func (l *Lock) Release() bool {
	t := l.table
	digest := l.name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*f_DIR, t.Name, fmt.Sprintf("%s.lock", digest))
	for i := 0; i < LOCK_TRIES; i++ {
		val, err := ioutil.ReadFile(lockfile)

		if err != nil {
			continue
		}

		if is_active_pid(val) {
			log.Println("UNLOCKING", lockfile)
			os.RemoveAll(lockfile)
			break
		}

	}

	return true
}

func (t *Table) GrabInfoLock() bool {
	lock := Lock{table: t, name: "info"}
	info := &InfoLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = RecoverLock(info)
	}

	return ret
}

func (t *Table) ReleaseInfoLock() bool {
	lock := Lock{table: t, name: "info"}
	info := &InfoLock{lock}
	ret := info.Release()
	return ret
}

func (t *Table) GrabDigestLock() bool {
	lock := Lock{table: t, name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = RecoverLock(info)
	}
	return ret
}

func (t *Table) ReleaseDigestLock() bool {
	lock := Lock{table: t, name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Release()
	return ret
}

func (t *Table) GrabBlockLock(name string) bool {
	lock := Lock{table: t, name: name}
	info := &BlockLock{lock}
	ret := info.Grab()
	// INFO RECOVER IS GOING TO HAVE TIMING ISSUES... WHEN MULTIPLE THREADS ARE
	// AT PLAY
	if !ret && info.broken {
		ret = RecoverLock(info)
	}
	return ret

}

func (t *Table) ReleaseBlockLock(name string) bool {
	lock := Lock{table: t, name: name}
	info := &BlockLock{lock}
	ret := info.Release()
	return ret
}
