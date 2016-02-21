package lc

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	. "github.com/topicai/candy"
	"github.com/wangkuiyi/fs"
)

type SearchImpression struct {
	Session string
	Query   string
	Results []string
}

func TestNewLogger(t *testing.T) {
	l, e := NewLogger("/tmp", SearchImpression{}, 10)
	assert.Nil(t, e)
	assert.Equal(t, "SearchImpression", l.msgType.Name())
}

func TestLog(t *testing.T) {
	dir := path.Join(os.TempDir(), fmt.Sprintf("%016x", os.Getpid()))
	l, _ := NewLogger(dir, SearchImpression{}, 10) // A very small cap msg size so that each file has one msg.

	assert.NotPanics(t, func() { l.Log(SearchImpression{Session: "1", Query: "something"}) })

	type si struct {
		Session string
		Query   string
		Results []string
	}
	assert.Panics(t, func() { l.Log(si{Session: "1"}) }) // si is not assignable to SearchImpression.

	assert.NotPanics(t, func() { l.Log(SearchImpression{Session: "1", Query: "something again"}) })
	fi, e := fs.ReadDir(path.Join(dir, "SearchImpression"))
	Must(e)
	assert.Equal(t, 2, len(fi))
}
