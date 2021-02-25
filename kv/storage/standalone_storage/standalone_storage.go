package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {

	dbPath := conf.DBPath
	standAlonePath := filepath.Join(dbPath, "standAlone")

	os.MkdirAll(standAlonePath, os.ModePerm)

	db := engine_util.CreateDB(conf.DBPath, false)

	// Your Code Here (1).
	return &StandAloneStorage{
		config: conf,
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := StandAloneReader{txn}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writes := engine_util.WriteBatch{}
	for _, m := range batch {
		key := m.Key()
		value := m.Value()
		cf := m.Cf()
		if value != nil {
			writes.SetCF(cf, key, value)
		} else {
			writes.DeleteCF(cf, key)
		}
	}

	return writes.WriteToDB(s.db)
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r StandAloneReader) Close() {
	r.txn.Discard()
}
