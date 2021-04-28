-- +goose Up
CREATE TABLE eth.access_list_entry (
   id                    SERIAL PRIMARY KEY,
   index                 INTEGER NOT NULL,
   tx_id                 INTEGER NOT NULL REFERENCES eth.transaction_cids (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
   address               VARCHAR(66),
   storage_keys          VARCHAR(66)[],
   UNIQUE (tx_id, index)
);

CREATE INDEX accesss_list_address_index ON eth.access_list_entry USING btree (address);

-- +goose Down
DROP INDEX eth.accesss_list_address_index;
DROP TABLE eth.access_list_entry;
