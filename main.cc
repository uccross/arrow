#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <iostream>

/* To decode every column, We need the offset and length 
inside the single contiguous buffer, where we can find all 
the data for that particular column. Also, we need to know 
the name and data type of the column to materialize it as 
part of a Table. We also need to know the null count and 
number of values in the column (basically the num rows of 
the column) to let arrow do it's thing. */
struct FieldMetadata {
    std::string name;
    int64_t offset;
    int64_t length;
    int8_t type;
    int64_t null_count;
};

/* A container to hold the number of rows and cols */
struct Shape {
    int64_t num_rows;
    int64_t num_columns;
};

/* The column metadata is final metadata consiting of metadata
of all the fields. We need to this ship this in the single
contiguous message. */
using ColumnMetadata = std::vector<std::shared_ptr<FieldMetadata>>;

/*
Arrow tables should always be in this format, no matter 
where it is on-wire or in memory. We just need to know 
the beginning of the buffer and the length.

| Shape size | Shape | Col Headers size | Col Headers[] | Garbage | Col Data[] |
*/

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
    std::cout << table->ToString() << "\n";
    // if this is 1, then only 1 chunk per chunk array

    
    // so the end product is 
    // buffer->data() and buffer->size()

    return arrow::Status::OK();
}

/*
schema, bytes, length. that's all what we got on the client
*/
arrow::Status SimulateClient(
    std::shared_ptr<arrow::Schema> schema, uint8_t *bytes, int64_t length) {

    // wrap our raw bytes to a arrow::Buffer
    std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>(bytes, length);
}

int main() {
    Driver();
}
