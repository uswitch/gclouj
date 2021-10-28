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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class StorageRowReader implements AutoCloseable {
    private final CallbackStorageRowReader delegate;

    public StorageRowReader(ArrowSchema arrowSchema) throws IOException {
        this.delegate = new CallbackStorageRowReader(arrowSchema);
    }

    public List<Map<String, Object>> processRows(ArrowRecordBatch batch) throws IOException {
        List<Map<String, Object>> batchResults = new ArrayList<>();
        delegate.processRows(batch, batchResults::add);
        return batchResults;
    }

    @Override
    public void close() {
        delegate.close();
    }
}
