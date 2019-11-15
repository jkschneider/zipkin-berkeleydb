package zipkin2.module.storage.berkeley;

import zipkin2.Call;
import zipkin2.Callback;

import java.io.IOException;

public class SupplierCall<V> extends Call.Base<V> {
  public interface SupplierWithIOException<V> {
    V get() throws IOException;
  }

  private final SupplierWithIOException<V> supplier;

  SupplierCall(SupplierWithIOException<V> supplier) {
    this.supplier = supplier;
  }

  @Override
  public Call<V> clone() {
    return new SupplierCall<>(supplier);
  }

  @Override
  protected V doExecute() throws IOException {
    return supplier.get();
  }

  @Override
  protected void doEnqueue(Callback<V> callback) {
    try {
      V v = supplier.get();
      callback.onSuccess(v);
    } catch (IOException e) {
      callback.onError(e);
    }
  }
}
