package sybil

import "log"
import "path"
import "os"
import "fmt"
import "strconv"
import "io/ioutil"
import "time"

var LOCK_US = time.Millisecond * 3
var LOCK_TRIES = 100

func is_active_pid(val []byte) bool {

	// Check if its our PID or not...
	pid_str := strconv.FormatInt(int64(os.Getpid()), 10)
	if pid_str == string(val) {
		return true
	}

	return false

}

func check_pid(lockfile string) bool {
	cangrab := false
	for i := 0; i < LOCK_TRIES; i++ {
		dirname := path.Dir(lockfile)
		os.MkdirAll(dirname, 0777)
		val, err := ioutil.ReadFile(lockfile)

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

func (t *Table) GetLock(digest string) bool {

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*f_DIR, t.Name, fmt.Sprintf("%s.lock", digest))

	if check_pid(lockfile) == false {
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

		if check_pid(lockfile) == false {
			continue
		}

		log.Println("LOCKING", lockfile)
		return true
	}

	log.Println("LOCK FAIL!", lockfile, check_pid(lockfile))
	return false

}

func (t *Table) ReleaseLock(digest string) bool {
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
		}

	}

	return true
}
