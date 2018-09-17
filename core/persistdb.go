package core

//TODO this file is largely copied from the BW2 store. It could perhaps be
//improved

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/pborman/uuid"
)

func interlaceURI(uri []string) []string {
	rv := make([]string, len(uri))
	for i := 0; i < len(uri); i += 2 {
		rv[i] = uri[i/2]
		if i+1 < len(uri) {
			rv[i+1] = uri[(len(uri)-1)-i/2]
		}
	}
	return rv
}
func unInterlaceURI(rv []string) []string {
	uri := make([]string, len(rv))
	for i := 0; i < len(uri); i += 2 {
		uri[i/2] = rv[i]
		if i+1 < len(uri) {
			uri[(len(uri)-1)-i/2] = rv[i+1]
		}
	}
	return uri
}

//Given an interlaced string and some _remaining_ *D. Construct the
//uninterlaced string
func advancedUnInterlaceURI(rv []string, frontD []string, backD []string) []string {
	uri := make([]string, len(rv)+len(frontD)+len(backD))
	bidx := len(uri) - 1
	fidx := 0
	//copy interlacing
	for i, v := range rv {
		if i&1 == 0 {
			//front
			uri[fidx] = v
			fidx++
		}
		if i&1 == 1 {
			//back
			uri[bidx] = v
			bidx--
		}
	}
	//copy frontD
	for _, v := range frontD {
		uri[fidx] = v
		fidx++
	}
	for _, v := range backD {
		uri[bidx] = v
		bidx--
	}
	return uri
}

const cfMsgI = 1
const cfMsg = 2

func (t *Terminus) LoadID() (id string) {
	err := t.db.Update(func(txn *badger.Txn) error {
		key := []byte("routerid")
		v, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			fmt.Printf("creating new ID for router\n")
			id = uuid.NewRandom().String()
			return txn.Set(key, []byte(id))
		}
		idb, err := v.Value()
		if err != nil {
			return err
		}
		id = string(idb)
		return nil
	})
	if err != nil {
		panic(err)
	}

	return
}
func (t *Terminus) putObject(cf int, path []byte, object []byte) {
	t.db.Update(func(txn *badger.Txn) error {
		key := make([]byte, len(path)+1)
		key[0] = byte(cf)
		copy(key[1:], path)
		err := txn.Set(key, object)
		if err != nil {
			panic(err)
		}
		return nil
	})
}
func (t *Terminus) getObject(cf int, path []byte) (rv []byte, err error) {
	t.db.View(func(txn *badger.Txn) error {
		key := make([]byte, len(path)+1)
		key[0] = byte(cf)
		copy(key[1:], path)
		item, e := txn.Get(key)
		if e == badger.ErrKeyNotFound {
			rv = nil
			err = nil
			return nil
		}
		if e != nil {
			rv = nil
			err = e
			return e
		} else {
			rv, e = item.Value()
			return nil
		}
	})
	return
}
func (t *Terminus) exists(cf int, path []byte) (ex bool) {
	t.db.View(func(txn *badger.Txn) error {
		key := make([]byte, len(path)+1)
		key[0] = byte(cf)
		copy(key[1:], path)
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			ex = false
			return nil
		} else {
			ex = true
			return nil
		}
		if err != nil {
			panic(err)
		}
		return nil
	})
	return
}

type DBIterator interface {
	Next()
	OK() bool
	Key() []byte
	Value() []byte
	Release()
}

type iteratorAdapter struct {
	it  *badger.Iterator
	pfx []byte
	txn *badger.Txn
}

func (ia *iteratorAdapter) Next() {
	ia.it.Next()
}
func (ia *iteratorAdapter) OK() bool {
	return ia.it.ValidForPrefix(ia.pfx)
}
func (ia *iteratorAdapter) Key() []byte {
	return ia.it.Item().Key()
}
func (ia *iteratorAdapter) Value() []byte {
	rv, err := ia.it.Item().Value()
	if err != nil {
		panic(err)
	}
	return rv
}
func (ia *iteratorAdapter) Release() {
	ia.it.Close()
	ia.txn.Discard()
}
func (t *Terminus) createIterator(cf int, pfx []byte) DBIterator {
	txn := t.db.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	it.Seek(pfx)
	return &iteratorAdapter{
		it:  it,
		txn: txn,
		pfx: pfx,
	}
}

//PutMessage inserts a message into the database. Note that the topic must be
//well formed and complete (no wildcards etc)
func (t *Terminus) putMessage(topic string, payload []byte) {
	ts := strings.Split(topic, "/")
	tb := make([]byte, len(topic)+1)
	copy(tb[1:], []byte(topic))
	tb[0] = byte(len(ts))
	mrg := interlaceURI(ts)
	smrgs := strings.Join(mrg, "/")
	smrg := make([]byte, len(smrgs)+1)
	copy(smrg[1:], []byte(smrgs))
	smrg[0] = byte(len(mrg))
	t.putObject(cfMsgI, smrg, payload)
	t.putObject(cfMsg, tb, payload)

	//Put parents
	for i := len(ts) - 1; i > 0; i-- {
		pstrs := []byte(strings.Join(ts[0:i], "/"))
		pstr := make([]byte, len(pstrs)+1)
		pstr[0] = byte(i)
		copy(pstr[1:], pstrs)
		if !t.exists(cfMsg, pstr) {
			t.putObject(cfMsg, pstr, []byte{0})
		} else {
			//We assume that if a path exists, all its parents exist
			break
		}
	}

	for i := len(mrg) - 1; i > 0; i-- {
		pstrs := []byte(strings.Join(mrg[0:i], "/"))
		pstr := make([]byte, len(pstrs)+1)
		pstr[0] = byte(i)
		copy(pstr[1:], pstrs)
		if !t.exists(cfMsgI, pstr) {
			t.putObject(cfMsgI, pstr, []byte{0})
		} else {
			//We assume that if a path exists, all its parents exist
			break
		}
	}
}

func (t *Terminus) getExactMessage(topic string) ([]byte, bool) {
	ts := strings.Split(topic, "/")
	key := make([]byte, len(topic)+1)
	copy(key[1:], []byte(topic))
	key[0] = byte(len(ts))
	value, err := t.getObject(cfMsg, key)
	if err != nil || isDummy(value) {
		return nil, false
	}
	return value, true
}

type SM struct {
	URI  string
	Body []byte
}

func MakeSMFromParts(uriparts []string, body []byte) SM {
	return SM{URI: strings.Join(uriparts, "/"),
		Body: body,
	}
}

func iswild(s string) bool {
	return s == "*" || s == "+"
}
func mkkey(uri []string) []byte {
	ms := strings.Join(uri, "/")
	key := make([]byte, len(ms)+1)
	key[0] = byte(len(uri))
	copy(key[1:], []byte(ms))
	return key
}
func mkchildkey(uri []string) []byte {
	ms := strings.Join(uri, "/")
	key := make([]byte, len(ms)+1)
	key[0] = byte(len(uri) + 1) //This is so we find children
	copy(key[1:], []byte(ms))
	return key
}
func unmakekey(key []byte) []string {
	return strings.Split(string(key[1:]), "/")
}
func isDummy(value []byte) bool {
	return len(value) == 1 && value[0] == 0
}

//The logic here is a bit over the top, so let me clarify for future me.
//We are handling two cases: interlaced and non-interlaced. For non interlaced everything
//should be simple. frontD should be emtpy and backD can have some stuffs. if interlaced,
//either (but not both) can have some stuff. they represent the delta after the uri is split to
//handle the star. If there is a star, it will be the last element in the uri (interlaced)
//or not. When the base cases for the star expansion are being tested, frontD and backD
//need to be inserted. When the children of an interlaced uri are being inspected,
//evey second level can be skipped and populated by a *D element.
//if the length of the uri is even, it is a frontD element, else a backD
//frontD is in left to right order, backD is in right to left order
func (t *Terminus) getMatchingMessage(interlaced bool, uri []string, prefix int, frontD []string, backD []string,
	skipbase bool, handle chan SM, wg *sync.WaitGroup) {
	//Make CF
	cf := cfMsg
	if interlaced {
		cf = cfMsgI
	}
	//Extend our prefix until the next wildcard
	nprefix := prefix
	for ; nprefix < len(uri) && !iswild(uri[nprefix]); nprefix++ {
	}
	//if there is no next wildcard, it is the end
	if nprefix == len(uri) {
		if len(backD) != 0 || len(frontD) != 0 {
			panic("invariant failure")
		}
		value, err := t.getObject(cf, mkkey(uri))
		if err == nil && !isDummy(value) {
			var newUri []string
			if interlaced {
				newUri = unInterlaceURI(uri)
			}
			handle <- MakeSMFromParts(newUri, value)
		}
		wg.Done()
		return
	}
	//if the next wildcard is a star, the base case is it being omitted
	//we do extensions via recursion below. We also only query the uninterlaced
	//store here. If the parent call populated a level from a *D then the
	//resulting base case has already been evaluated

	if uri[nprefix] == "*" && !skipbase {
		if nprefix != len(uri)-1 {
			panic("invariant failure")
		}
		var directUri []string
		if interlaced {
			directUri = advancedUnInterlaceURI(uri[:nprefix], frontD, backD)
		} else {
			directUri = make([]string, len(uri)+len(backD)-1)
			copy(directUri, uri[:nprefix])
			idx := nprefix
			for i := len(backD) - 1; i >= 0; i-- {
				directUri[idx] = backD[i]
				idx++
			}
		}
		value, err := t.getObject(cfMsg, mkkey(directUri))
		if err == nil && !isDummy(value) {
			handle <- MakeSMFromParts(directUri, value)
		}
	}

	//if the next wildcard is a star, we also need to scan, expanding *D
	if uri[nprefix] == "*" {
		if !interlaced {
			//No reason to have a frontD if there is no interlacing
			if len(frontD) != 0 {
				panic("invariant failure")
			}
		}
		if interlaced && nprefix%2 == 0 && len(frontD) != 0 { //even == front
			//Skip scan, we can populate from frontD
			newUri := make([]string, len(uri)+1)
			copy(newUri, uri[:nprefix])
			newUri[nprefix] = frontD[0]
			newUri[nprefix+1] = "*"
			//Don't increment nprefix because frontD[0] may have been a +
			t.getMatchingMessage(interlaced, newUri, nprefix, frontD[1:], backD, true, handle, wg)
			return //Don't need to wg because we invoke a function that will decrement
		} else if interlaced && nprefix%2 == 1 && len(backD) != 0 { //odd == back
			//Skip scan we can populate from backD
			newUri := make([]string, len(uri)+1)
			copy(newUri, uri[:nprefix])
			newUri[nprefix] = backD[0] //backD is in reverse order so this is correct
			newUri[nprefix+1] = "*"
			//Don't increment nprefix because frontD[0] may have been a +
			t.getMatchingMessage(interlaced, newUri, nprefix, frontD, backD[1:], true, handle, wg)
			return
		}
	}
	//If we got here, we could not skip the scan by using *D
	if uri[nprefix] == "+" || uri[nprefix] == "*" {
		pfx := mkchildkey(uri[:nprefix])
		it := t.createIterator(cf, pfx)
		for it.OK() {
			k := it.Key()
			actualkey := unmakekey(k)
			var newUri []string
			if uri[nprefix] == "+" {
				newUri = make([]string, len(uri))
				copy(newUri, actualkey)
				copy(newUri[nprefix+1:], uri[nprefix+1:])
			} else {
				//new uri must include the star
				newUri = make([]string, len(uri)+1)
				copy(newUri, actualkey)
				copy(newUri[nprefix+1:], uri[nprefix:])
			}
			wg.Add(1)
			go t.getMatchingMessage(interlaced, newUri, nprefix, frontD, backD, false, handle, wg)
			it.Next()
		}
		it.Release()
		wg.Done()
		return
	}
}
func (t *Terminus) ListChildren(uri string, handle chan string) {
	parts := strings.Split(uri, "/")
	ckey := mkchildkey(parts)
	it := t.createIterator(cfMsg, ckey)
	for it.OK() {
		k := it.Key()
		handle <- string(k[1:])
		it.Next()
	}
	it.Release()
	close(handle)
}

func (t *Terminus) GetMatchingMessage(uri string, handle chan SM) {
	parts := strings.Split(uri, "/")
	staridx := -1
	pluscount := 0
	for i, v := range parts {
		if v == "*" {
			staridx = i
		}
		if v == "+" {
			pluscount++
		}
	}
	if pluscount == 0 && staridx == -1 {
		m, ok := t.getExactMessage(uri)
		if ok {
			handle <- MakeSMFromParts(parts, m)
		}
		close(handle)
		return
	}

	if staridx == -1 {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		t.getMatchingMessage(false, parts, 0, nil, nil, false, handle, wg)
		wg.Wait()
		close(handle)
		return
	}

	pfxlen := staridx
	sfxlen := len(parts) - staridx - 1
	if pfxlen-sfxlen > sfxlen { //Prefix is much longer than suffix, use leftscan
		uri := parts[:pfxlen+1]
		frontD := []string{}
		backD := make([]string, sfxlen)
		for i := 0; i < sfxlen; i++ {
			backD[i] = parts[len(parts)-1-i]
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		t.getMatchingMessage(false, uri, 0, frontD, backD, false, handle, wg)
		wg.Wait()
		close(handle)
	} else {
		partslen := pfxlen
		if sfxlen < partslen {
			partslen = sfxlen
		}
		common := partslen
		partslen *= 2
		uri := interlaceURI(parts)[:partslen+1]
		uri[partslen] = "*"
		frontD := make([]string, pfxlen-common)
		backD := make([]string, sfxlen-common)
		for i := 0; i < len(frontD); i++ {
			frontD[i] = parts[common+i]
		}
		for i := 0; i < len(backD); i++ {
			backD[i] = parts[len(parts)-1-i-common]
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		t.getMatchingMessage(true, uri, 0, frontD, backD, false, handle, wg)
		wg.Wait()
		close(handle)
	}
}
