package io.vertx.mqtt.test.client;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.MqttServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(VertxUnitRunner.class)
public class MqttClientLeakTest {

  private Vertx vertx;
  private MqttServer server;

  private void startServer(TestContext ctx) throws Exception {
    server.listen().toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
  }

  @Before
  public void before() {
    vertx = Vertx.vertx();
    server = MqttServer.create(vertx);
  }

  @After
  public void after(TestContext ctx) {
    server.close().onComplete(ctx.asyncAssertSuccess(v -> {
      vertx.close().onComplete(ctx.asyncAssertSuccess());
    }));
  }

  @Test
  public void testConnectDisconnectWithoutOOM(TestContext ctx) throws Exception {
    server.endpointHandler(endpoint -> {
      endpoint.accept(false);
    });
    startServer(ctx);
    MqttClientOptions options = new MqttClientOptions();
    options.setAutoKeepAlive(true);
    options.setKeepAliveInterval(1);
    MqttClient client = MqttClient.create(vertx, options);
    Promise<Void> promise = Promise.promise();
    step(client, promise, 100000);
    promise.future().onComplete(ctx.asyncAssertSuccess());
  }

  private static void step(MqttClient client, Promise<Void> promise, int step) {
    if (step < 1) {
      promise.complete();
      return;
    }
    client.connect(MqttClientOptions.DEFAULT_PORT, MqttClientOptions.DEFAULT_HOST)
      .compose(v -> client.disconnect())
      .onComplete(ar -> {
        if (ar.succeeded()) {
          step(client, promise, step - 1);
        } else {
          promise.fail(ar.cause());
        }
      });
  }
}
