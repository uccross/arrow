#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/type.h>
#include <arrow/util/checked_cast.h>
#include <flatbuffers/flatbuffers.h>

#include <iostream>

#include "Shape_generated.h"
#include "FieldMetadata_generated.h"

namespace flatbuf = org::apache::arrow::flatbuf;

constexpr int BEGIN = 1342;
constexpr int SHAPE_SIZE = 40;
constexpr int FIRST_FIELD_META = 64;
constexpr int OTHER_FIELD_META = 72;
constexpr int FRAGMENT_SIZE = 1024*1024*1024;


inline int64_t PaddedLength(int64_t nbytes) {
  static const int64_t alignment = 8;
  return ((nbytes + alignment - 1) / alignment) * alignment;
}

std::shared_ptr<arrow::Table> DecodeArrowTableBuffer(std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<arrow::Schema> schema) {
    auto shape_buffer = arrow::SliceBuffer(buffer, BEGIN, SHAPE_SIZE);
    auto shape_fbs = flatbuf::GetSizePrefixedShape(shape_buffer->data());
    std::cout << "------------------------------\n";
    std::cout << "Reading Arrow Table Buffer -\n";
    std::cout << "------------------------------\n";
    std::cout << "Num rows:    " << shape_fbs->num_rows() << "\n";
    std::cout << "Num columns: " << shape_fbs->num_columns() << "\n";
    std::cout << "------------------------------\n";
    
    std::vector<std::shared_ptr<arrow::ChunkedArray>> cols(shape_fbs->num_columns());

    int64_t pos = BEGIN + SHAPE_SIZE;
    for (int64_t i = 0; i < shape_fbs->num_columns(); i++) {
        int diff = FIRST_FIELD_META;
        if (i > 0) diff = OTHER_FIELD_META;
        auto field_meta_buffer = arrow::SliceBuffer(buffer, pos, diff);
        auto field_meta_fbs = flatbuf::GetSizePrefixedFieldMetadata(field_meta_buffer->data());
        std::cout << "------------------------------\n";
        std::cout << "Index:       " << field_meta_fbs->index() << "\n";
        std::cout << "Length:      " << field_meta_fbs->length() << "\n";
        std::cout << "Offset:      " << field_meta_fbs->offset() << "\n";
        std::cout << "Total bytes: " << field_meta_fbs->total_bytes() << "\n";
        std::cout << "Null count:  " << field_meta_fbs->null_count() << "\n";
        std::cout << "Type:        " << field_meta_fbs->type() << "\n";
        std::cout << "------------------------------\n";
        pos += diff;

        std::vector<std::shared_ptr<arrow::Buffer>> buffers;
        int64_t offset = 0;
        auto col_buf = arrow::SliceBuffer(buffer, field_meta_fbs->offset(), field_meta_fbs->total_bytes());

        auto type = schema->field(i)->type();
        if (type->id() == arrow::Type::DICTIONARY) {
            type = arrow::internal::checked_cast<const arrow::DictionaryType&>(*type).index_type();
        }

        if (field_meta_fbs->null_count() > 0) {
            int64_t null_bitmap_size = PaddedLength(arrow::BitUtil::BytesForBits(field_meta_fbs->length()));
            buffers.push_back(SliceBuffer(col_buf, offset, null_bitmap_size));
            offset += null_bitmap_size;
        } else {
            buffers.push_back(nullptr);
        }

        if (arrow::is_binary_like(type->id())) {
            int64_t offsets_size = PaddedLength((field_meta_fbs->length() + 1) * sizeof(int32_t));
            buffers.push_back(arrow::SliceBuffer(col_buf, offset, offsets_size));
            offset += offsets_size;
        } else if (arrow::is_large_binary_like(type->id())) {
            int64_t offsets_size = PaddedLength((field_meta_fbs->length() + 1) * sizeof(int64_t));
            buffers.push_back(arrow::SliceBuffer(col_buf, offset, offsets_size));
            offset += offsets_size;
        }

        buffers.push_back(arrow::SliceBuffer(col_buf, offset, col_buf->size() - offset));

        auto data = arrow::ArrayData::Make(type, field_meta_fbs->length(), std::move(buffers), field_meta_fbs->null_count());
        cols[i] = std::make_shared<arrow::ChunkedArray>(arrow::MakeArray(data));
    }
    return arrow::Table::Make(schema, std::move(cols), shape_fbs->num_rows());
}

arrow::Status Driver() {
    std::shared_ptr<arrow::ResizableBuffer> buffer = arrow::AllocateResizableBuffer(FRAGMENT_SIZE).ValueOrDie();    
    auto file = arrow::io::ReadableFile::Open("/users/noobjc/128MB.feather", buffer).ValueOrDie();
    auto reader = arrow::ipc::feather::Reader::Open(file).ValueOrDie();
    reader->SetSource(buffer);
    std::shared_ptr<arrow::Table> table;
    reader->Read(&table);

    auto res = DecodeArrowTableBuffer(buffer, table->schema());
    std::cout << res->ToString();

    return arrow::Status::OK();
}

int main() {
    Driver();
}


/*
Arrow tables should always be in this format, no matter 
where it is on-wire or in memory. We just need to know 
the beginning of the buffer and the length.
 
| Shape (32) | Col Headers (56 * N columns) | Garbage | Col Data |
*/