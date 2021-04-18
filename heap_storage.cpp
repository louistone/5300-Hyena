// heap_storage.cpp

#include "heap_storage.h"
#include <cstring>

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


Dbt* SlottedPage::get(RecordID record_id) {
  u_int16_t size;
  u_int16_t loc;

  get_header(size, loc, record_id);

  if (loc == 0){
    return NULL;
  }
  
  return new Dbt(this->address(loc), size);

}


void SlottedPage::put(RecordID record_id, const Dbt &data){
  
  u_int16_t size;
  u_int16_t loc;
  u_int16_t new_size = data.get_size();
  u_int16_t extra;
  
  get_header(size, loc, record_id);

  if(new_size > size){
    extra = new_size - size;
    if(!has_room(extra)){
      throw DbBlockNoRoomError("Not enough room in block");
    }
    
    slide(loc, loc - extra);
    memcpy(this->address(loc - extra), data.get_data(), size);
  }
  else{
    memcpy(this->address(loc), data.get_data(), new_size);
    slide(loc + new_size, loc + size);
  }

  get_header(size, loc, record_id);
  put_header(record_id, size, loc);
}


void SlottedPage::del(RecordID record_id){
  u_int16_t size;
  u_int16_t loc;

  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);

}

/*RecordIDs* SlottedPage::ids(void){
  RecordIDs *id;
  
  for(int i = 1; i < num_records + 1; i++){
    u_int16_t size;
    u_int16_t loc;
    get_header(size, loc, i);
    
    if(loc != 0){
      id->push_back(i);
    }
    
    return id;
    }*/


void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID record_id){
  size = get_n(4 * record_id);
  loc = get_n(4 * record_id + 2);
}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
  if (id == 0) {
    size = this->num_records;
    loc = this->end_free;
  }
  put_n(4*id, size);
  put_n(4*id + 2, loc);
}


u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u_int16_t*)this->address(offset);
}


void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u_int16_t*)this->address(offset) = n;
}


void* SlottedPage::address(u_int16_t offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


