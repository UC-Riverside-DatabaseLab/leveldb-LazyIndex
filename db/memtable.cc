// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp, bool isSecondarydb)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
    this->isSecondaryDB = isSecondarydb;
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}
std::string M_GetVal(const rapidjson::Value& val) {

  std::ostringstream pKey;

  if(val.IsNumber()) {
    if(val.IsUint64()) {
      unsigned long long int tid = val.GetUint64();
      pKey<<tid;
    }
    else if (val.IsInt64()) {
      long long int tid = val.GetInt64();
      pKey<<tid;
    }
    else if (val.IsDouble()) {
      double tid = val.GetDouble();
      pKey<<tid;
    }
    else if (val.IsUint()) {
      unsigned int tid = val.GetUint();
      pKey<<tid;
    }
    else if (val.IsInt()) {
      int tid = val.GetInt();
      pKey<<tid;
    }
  }
  else if (val.IsString()) {
    const char* tid = val.GetString();
    pKey<<tid;
  }
  else if(val.IsBool()) {
    bool tid = val.GetBool();
    pKey<<tid;
  }

  return pKey.str();
}
std::string M_GetAttr(const rapidjson::Document& doc, const char* attr) {
  if(!doc.IsObject() || !doc.HasMember(attr) || doc[attr].IsNull())
    return "";

  std::ostringstream pKey;

  if(doc[attr].IsNumber()) {
    if(doc[attr].IsUint64()) {
      unsigned long long int tid = doc[attr].GetUint64();
      pKey<<tid;
    }
    else if (doc[attr].IsInt64()) {
      long long int tid = doc[attr].GetInt64();
      pKey<<tid;
    }
    else if (doc[attr].IsDouble()) {
      double tid = doc[attr].GetDouble();
      pKey<<tid;
    }
    else if (doc[attr].IsUint()) {
      unsigned int tid = doc[attr].GetUint();
      pKey<<tid;
    }
    else if (doc[attr].IsInt()) {
      int tid = doc[attr].GetInt();
      pKey<<tid;
    }
  }
  else if (doc[attr].IsString()) {
    const char* tid = doc[attr].GetString();
    pKey<<tid;
  }
  else if(doc[attr].IsBool()) {
    bool tid = doc[attr].GetBool();
    pKey<<tid;
  }

  return pKey.str();
}


void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& rawvalue) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
    
  //For seq number insertion in index table
    
  if(this->isSecondaryDB)
  {
    //std::ofstream outputFile;  
    //outputFile.open("/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Debug/lazy_debug_RangeLookUp.txt", std::ofstream::out | std::ofstream::app);
    
    //<<"raw: "<<rawvalue.data()<"\n";
      
    rapidjson::Document key_list;
    key_list.Parse<0>(rawvalue.data());

 

    std::string new_key_list = "[";
    //rapidjson::SizeType j = 0;

    
    for (rapidjson::SizeType i = 0; i < key_list.Size(); i++)
    {       
        if (i != 0)
        {
          new_key_list += ",";
          
        
        }
        if(i==key_list.Size()-1)
         
            new_key_list += ("\"" + SSTR(s) +"+"+ M_GetVal(key_list[i]) + "\"");
        else            
            new_key_list += ("\"" + M_GetVal(key_list[i]) + "\"");
        
         
        
    }  
      
    new_key_list += "]";
          
    Slice value = new_key_list;
  
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len =
        VarintLength(internal_key_size) + internal_key_size +
        VarintLength(val_size) + val_size;
    char* buf = arena_.Allocate(encoded_len);
    char* p = EncodeVarint32(buf, internal_key_size);
    memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (s << 8) | type);
    p += 8;
    p = EncodeVarint32(p, val_size);
    memcpy(p, value.data(), val_size);
    assert((p + val_size) - buf == encoded_len);
    
    //outputFile<<buf<<"\n";
    table_.Insert(buf);
  }
  else
  {
    size_t key_size = key.size();
    size_t val_size = rawvalue.size();
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len =
        VarintLength(internal_key_size) + internal_key_size +
        VarintLength(val_size) + val_size;
    char* buf = arena_.Allocate(encoded_len);
    char* p = EncodeVarint32(buf, internal_key_size);
    memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (s << 8) | type);
    p += 8;
    p = EncodeVarint32(p, val_size);
    memcpy(p, rawvalue.data(), val_size);
    assert((p + val_size) - buf == encoded_len);
    table_.Insert(buf);
  }
}


int MemTable::RangeGet(const LookupKey& startkey, std::string& endkey, std::vector<RangeKeyValuePair>* value_list, Status* s, std::string secAttribute, DB* db, int topk, std::unordered_set<std::string>* resultSetofKeysFound)
{
	Slice memkey = startkey.memtable_key();
	Table::Iterator iter(&table_);
	iter.Seek(memkey.data());
	int resultCount = 0;
	std::string prevseckey= "";
	leveldb::ReadOptions roption;
	for (; iter.Valid(); iter.Next())
	{
		const char* entry = iter.key();
		uint32_t key_length;
		const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);

		std::string seckey = Slice(key_ptr, key_length - 8).ToString();

		if(prevseckey == seckey)
			continue;

		prevseckey = seckey;

		if( seckey.compare(endkey) > 0)
		{
		// key >= end
		  break;

		}

		std::string val;
		const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
		switch (static_cast<ValueType>(tag & 0xff)) {
			case kTypeValue: {
				resultCount++;
				Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
				val.assign(v.data(), v.size());

				rapidjson::Document key_list;

				key_list.Parse<0>(val.c_str());
				rapidjson::SizeType len = key_list.Size();
				unsigned i = 0;

				while (i < len )
				{
					std::string pkey = M_GetVal(key_list[i]);
					i++;
					//Add in result set
					std::string pValue;
					std::string delim = "+";
					std::size_t found = pkey.find(delim);

					if (found!=std::string::npos)
					{
						char *pEnd;
						uint64_t seqN =   std::strtoul(pkey.substr(0, found).c_str(), &pEnd, 0);
						pkey = pkey.substr(found+1);
						struct RangeKeyValuePair newVal;;

						newVal.key = pkey;
						newVal.sequence_number = seqN;
						int vsize = value_list->size();
						if(vsize>=topk && seqN<value_list->front().sequence_number)
							continue;

						if(resultSetofKeysFound->find(pkey)==resultSetofKeysFound->end())
						{

							Status db_status = db->Get(roption, pkey, &pValue);
							newVal.value = pValue;
							// if there are no errors, push KV pair onto return vector, latest record first
							// check for updated values
							if (db_status.ok()&&!db_status.IsNotFound()) {
								rapidjson::Document temp_val;
								temp_val.Parse<0>(pValue.c_str());
								//outputFile<<pkey<<" -> "<<pValue<<"\n"<<skey.ToString()<<" == "<< GetAttr(temp_val, this->options_.secondary_key.c_str())<<"\n";
								if (seckey == M_GetAttr(temp_val, secAttribute.c_str()))
								{
									if(vsize>=topk)
										newVal.Pop(value_list);
									newVal.Push(value_list, newVal);
									resultSetofKeysFound->insert(pkey);

								}
							}
						}
					}



				}
			}

		}
	}
	//delete iter.;
	return resultCount;

}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
