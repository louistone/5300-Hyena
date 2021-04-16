// heap_storage.cpp

#include "heap_storage.h"

bool test_heap_storage() {return true;}

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new): DbBlock(block, block_id, is_new)
{
  if (is_new) {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  }
  else {
    get_header(this->num_records, this->end_free);
  }
}

RecordID SlottedPage::add(const Dbt *data) {
  if (!has_room(data->get_size())){
    throw DbBlockNoRoomError("Not enough room in block");
  }
  this->num_records += 1;
  u_int16_t record_id = this->num_records;
  u_int16_t size = data->get_size();
  this->end_free -= size;
  u_int16_t loc = this->end_free + 1;
  put_header();
  put_header(record_id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return record_id;
}

Dbt SlottedPage::*get(RecordID record_id) {
  u_int16_t size = get;
  u_int16_t loc;
}
