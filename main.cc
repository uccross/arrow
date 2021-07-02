#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/type.h>
#include "arrow/util/checked_cast.h"

#include <iostream>

#include <flatbuffers/flatbuffers.h>
#include "Shape_generated.h"
#include "FieldMetadata_generated.h"

namespace flatbuf = org::apache::arrow::flatbuf;

/*
Arrow tables should always be in this format, no matter 
where it is on-wire or in memory. We just need to know 
the beginning of the buffer and the length.
 
| Shape (32) | Col Headers (56 * N columns) | Garbage | Col Data |
*/

arrow::Status DecodeArrowTableBuffer(std::shared_ptr<arrow::Buffer> buffer) {
    auto shape_buffer = arrow::SliceBuffer(buffer, 1342, 40);
    auto shape_fbs = flatbuf::GetSizePrefixedShape(shape_buffer->data());
    std::cout << "------------------------------\n";
    std::cout << "Reading Arrow Table Buffer -\n";
    std::cout << "------------------------------\n";
    std::cout << "Num rows: " << shape_fbs->num_rows() << "\n";
    std::cout << "Num columns: " << shape_fbs->num_columns() << "\n";
    std::cout << "------------------------------\n";

    int64_t pos = 1342 + 40;
    for (int64_t i = 0; i < shape_fbs->num_columns(); i++) {
        int diff = 48;
        if (i > 0) diff = 56;
        auto field_meta_buffer = arrow::SliceBuffer(buffer, pos, diff);
        auto field_meta_fbs = flatbuf::GetSizePrefixedFieldMetadata(field_meta_buffer->data());
        std::cout << "------------------------------\n";
        std::cout << "Index:      " << field_meta_fbs->index() << "\n";
        std::cout << "Length:     " << field_meta_fbs->length() << "\n";
        std::cout << "Offset:     " << field_meta_fbs->offset() << "\n";
        std::cout << "Null count: " << field_meta_fbs->null_count() << "\n";
        std::cout << "Type: " << field_meta_fbs->type() << "\n";
        std::cout << "------------------------------\n";
        pos += diff;
    }
    return arrow::Status::OK();
}

arrow::Status Driver() {
    // read into a contiguous buffer
    int64_t size = 1024*1024*1024;
    std::shared_ptr<arrow::ResizableBuffer> buffer = arrow::AllocateResizableBuffer(size).ValueOrDie();    
    auto file = arrow::io::ReadableFile::Open("/users/noobjc/128MB.feather", buffer).ValueOrDie();
    auto reader = arrow::ipc::feather::Reader::Open(file).ValueOrDie();
    reader->SetSource(buffer);
    std::shared_ptr<arrow::Table> table;
    reader->Read(&table);
    auto bytes_read = file->Tell().ValueOrDie();
    std::cout << "Bytes Read: " << bytes_read << "\n";
    buffer->Resize(bytes_read);
    std::cout << "Buffer Size: " << buffer->size() << "\n";
    std::cout << "Buffer Capacity: " << buffer->capacity() << "\n";

    arrow::RecordBatchVector batches;
    std::cout << "Rows in the table: " << table->num_rows() << "\n";
    auto tablereader = std::make_shared<arrow::TableBatchReader>(*table);
    tablereader->ReadAll(&batches);
    std::cout << "Num batches: " << batches.size() << "\n";
    // std::cout << table->ToString() << "\n";
    // if this is 1, then only 1 chunk per chunk array

    
    // so the end product is 
    // buffer->data() and buffer->size()

    DecodeArrowTableBuffer(buffer);

    return arrow::Status::OK();
}

int main() {
    Driver();
}
