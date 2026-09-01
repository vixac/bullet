package mongodb

import "github.com/vixac/bullet/store/store_interface"

func (s *MongoStore) LedgerAppend(store_interface.TenancySpace, store_interface.LedgerID, store_interface.LedgerAppendID, string) (store_interface.LedgerRecord, error) {
	return store_interface.LedgerRecord{}, store_interface.ErrLedgerUnsupported
}
func (s *MongoStore) LedgerAppendMany(store_interface.TenancySpace, store_interface.LedgerID, []store_interface.LedgerAppendItem) ([]store_interface.LedgerRecord, error) {
	return nil, store_interface.ErrLedgerUnsupported
}
func (s *MongoStore) LedgerReadBackward(store_interface.TenancySpace, store_interface.LedgerSelector, *string, int) (store_interface.LedgerPage, error) {
	return store_interface.LedgerPage{}, store_interface.ErrLedgerUnsupported
}
func (s *MongoStore) LedgerReadForward(store_interface.TenancySpace, store_interface.LedgerSelector, store_interface.LedgerPosition, *store_interface.LedgerPosition, int) ([]store_interface.LedgerRecord, error) {
	return nil, store_interface.ErrLedgerUnsupported
}
func (s *MongoStore) LedgerDelete(store_interface.TenancySpace, store_interface.LedgerID) error {
	return store_interface.ErrLedgerUnsupported
}
