package dlog

import (
	"fmt"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	. "github.com/topicai/candy"
)

func TestFsNotify(t *testing.T) {
	watcher, e := fsnotify.NewWatcher()
	Must(e)
	defer watcher.Close()

	root := "/tmp/dlog"
	work := path.Join(root, "SearchImpression")

	Must(mkDirIfNotExist(work))
	Must(watcher.Add(work))

	go func() {
		time.Sleep(time.Second)
		f, e := os.Create(path.Join(work, "a.txt"))
		Must(e)
		fmt.Fprintf(f, "Hello\n")
		f.Sync()
		time.Sleep(time.Second)
		fmt.Fprintf(f, "World!\n")
		f.Sync()
		time.Sleep(time.Second)
		f.Close()
	}()

	for {
		select {
		case event := <-watcher.Events:
			log.Println("event:", event)
		case err := <-watcher.Errors:
			log.Println("error:", err)
			return
		}
	}
}

func mkDirIfNotExist(dir string) error {
	if _, e := os.Stat(dir); e != nil && os.IsNotExist(e) {
		if e := os.MkdirAll(dir, 0744); e != nil {
			return fmt.Errorf("Failed to create dir (%v) : %v", dir, e)
		}
	}

	return nil
}
