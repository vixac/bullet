package boltdb

import "github.com/vixac/bullet/model"

func (s *BoltStore) LedgerAppend(model.TenancySpace, model.LedgerID, model.LedgerAppendID, string) (model.LedgerRecord, error) {
	return model.LedgerRecord{}, model.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerAppendMany(model.TenancySpace, model.LedgerID, []model.LedgerAppendItem) ([]model.LedgerRecord, error) {
	return nil, model.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerReadBackward(model.TenancySpace, model.LedgerSelector, *string, int) (model.LedgerPage, error) {
	return model.LedgerPage{}, model.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerReadForward(model.TenancySpace, model.LedgerSelector, model.LedgerPosition, *model.LedgerPosition, int) ([]model.LedgerRecord, error) {
	return nil, model.ErrLedgerUnsupported
}
func (s *BoltStore) LedgerDelete(model.TenancySpace, model.LedgerID) error {
	return model.ErrLedgerUnsupported
}
