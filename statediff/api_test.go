// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package statediff_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff"
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}
}

var (
	address1Hex = "0x1ca7c995f8eF0A2989BbcE08D5B7Efe50A584aa1"
	address2Hex = "0xe799eE0191652c864E49F3A3344CE62535B15afe"
	address1    = common.HexToAddress(address1Hex)
	address2    = common.HexToAddress(address2Hex)

	watchedAddresses0 []common.Address
	watchedAddresses1 = []common.Address{address1}
	watchedAddresses2 = []common.Address{address1, address2}

	expectedError = fmt.Errorf("Address %s already watched", address1Hex)

	service = statediff.Service{}
)

func TestWatchAddress(t *testing.T) {
	watchedAddresses := service.GetWathchedAddresses()
	if !reflect.DeepEqual(watchedAddresses, watchedAddresses0) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual watched addresses not equal expected watched addresses.\nactual: %+v\nexpected: %+v", watchedAddresses, watchedAddresses0)
	}

	testWatchUnwatchedAddress(t)
	testWatchWatchedAddress(t)
}

func testWatchUnwatchedAddress(t *testing.T) {
	err := service.WatchAddress(address1)
	if err != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Unexpected error %s thrown on an attempt to watch an unwatched address.", err.Error())
	}
	watchedAddresses := service.GetWathchedAddresses()
	if !reflect.DeepEqual(watchedAddresses, watchedAddresses1) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual watched addresses not equal expected watched addresses.\nactual: %+v\nexpected: %+v", watchedAddresses, watchedAddresses1)
	}

	err = service.WatchAddress(address2)
	if err != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Unexpected error %s thrown on an attempt to watch an unwatched address.", err.Error())
	}
	watchedAddresses = service.GetWathchedAddresses()
	if !reflect.DeepEqual(watchedAddresses, watchedAddresses2) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual watched addresses not equal expected watched addresses.\nactual: %+v\nexpected: %+v", watchedAddresses, watchedAddresses2)
	}
}

func testWatchWatchedAddress(t *testing.T) {
	err := service.WatchAddress(address1)
	if err == nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Expected error %s not thrown on an attempt to watch an already watched address.", expectedError.Error())
	}
	if err.Error() != expectedError.Error() {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual thrown error not equal expected error.\nactual: %+v\nexpected: %+v", err.Error(), expectedError.Error())
	}
	watchedAddresses := service.GetWathchedAddresses()
	if !reflect.DeepEqual(watchedAddresses, watchedAddresses2) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual watched addresses not equal expected watched addresses.\nactual: %+v\nexpected: %+v", watchedAddresses, watchedAddresses2)
	}
}
