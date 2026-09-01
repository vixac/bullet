package boltdb

import "github.com/vixac/bullet/store/store_interface"

func (s *BoltStore) LedgerAppend(store_interface.TenancySpace, store_interface.LedgerID, store_interface.LedgerAppendID, string) (store_interface.LedgerRecord, error) {
	return store_interface.LedgerRecord{}, store_interface.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerAppendMany(store_interface.TenancySpace, store_interface.LedgerID, []store_interface.LedgerAppendItem) ([]store_interface.LedgerRecord, error) {
	return nil, store_interface.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerReadBackward(store_interface.TenancySpace, store_interface.LedgerSelector, *string, int) (store_interface.LedgerPage, error) {
	return store_interface.LedgerPage{}, store_interface.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerReadForward(store_interface.TenancySpace, store_interface.LedgerSelector, store_interface.LedgerPosition, *store_interface.LedgerPosition, int) ([]store_interface.LedgerRecord, error) {
	return nil, store_interface.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerDelete(store_interface.TenancySpace, store_interface.LedgerID) error {
	return store_interface.ErrLedgerUnsupported
}
