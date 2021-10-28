package gclouj;

import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

public class CallbackStorageRowReader implements AutoCloseable {
    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    // Decoder object will be reused to avoid re-allocation and too much garbage collection.
    private final VectorSchemaRoot root;
    private final VectorLoader loader;

    public CallbackStorageRowReader(
            ArrowSchema arrowSchema
    ) throws IOException {
        Schema schema =
                MessageSerializer.deserializeSchema(
                        new ReadChannel(
                                new ByteArrayReadableSeekableByteChannel(
                                        arrowSchema.getSerializedSchema().toByteArray())));
        requireNonNull(schema);
        List<FieldVector> vectors = new ArrayList<>();

        for (Field field : schema.getFields()) {
            vectors.add(field.createVector(allocator));
        }

        this.root = new VectorSchemaRoot(vectors);
        this.loader = new VectorLoader(root);
    }

    /**
     * Sample method for processing Arrow data which only validates decoding.
     *
     * @param batch object returned from the ReadRowsResponse.
     */
    public void processRows(ArrowRecordBatch batch, Consumer<Map<String, Object>> rowConsumer) throws IOException {
        org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
                MessageSerializer.deserializeRecordBatch(
                        new ReadChannel(
                                new ByteArrayReadableSeekableByteChannel(
                                        batch.getSerializedRecordBatch().toByteArray())),
                        allocator);

        loader.load(deserializedBatch);
        // Release buffers from batch (they are still held in the vectors in root).
        deserializedBatch.close();

        List<String> fieldNames = root.getSchema()
                .getFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList());

        for (int i = 0; i < root.getRowCount(); i++) {
            Map<String, Object> row = new HashMap<>();
            for (int j = 0; j < fieldNames.size(); j++) {
                row.put(fieldNames.get(j), root.getFieldVectors().get(j).getObject(i));
            }
            rowConsumer.accept(row);
        }

        // Release buffers from vectors in root.
        root.clear();
    }

    @Override
    public void close() {
        root.close();
        allocator.close();
    }
}
