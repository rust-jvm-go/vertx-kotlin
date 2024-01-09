package initiative.vertx.kotlin.vertx_kotlin

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.Router
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.pgclient.PgBuilder
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.PoolOptions
import java.io.IOException
import java.util.logging.LogManager
import java.util.logging.Logger

class MainVerticle : AbstractVerticle() {

  companion object {
    private val LOGGER = Logger.getLogger(MainVerticle::class.java.name)

    /**
     * Configure logging from logging.properties file.
     * When using custom JUL logging properties, named it to vertx-default-jul-logging.properties
     * or set java.util.logging.config.file system property to locate the properties file.
     */
    @Throws(IOException::class)
    private fun setupLogging() {
      MainVerticle::class.java.getResourceAsStream("/logging.properties")
        .use { f -> LogManager.getLogManager().readConfiguration(f) }
    }

    init {
      LOGGER.info("Customizing the built-in jackson ObjectMapper...")
      val objectMapper = DatabindCodec.mapper()
      objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      objectMapper.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
      objectMapper.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
      val module = JavaTimeModule()
      objectMapper.registerModule(module)
    }
  }

  override fun start(startPromise: Promise<Void>) {

    val store = ConfigStoreOptions()
      .setType("file")
      .setFormat("yaml")
      .setConfig(
        JsonObject()
          .put("path", "application.yaml")
      );

    val retriever = ConfigRetriever.create(
      vertx,
      ConfigRetrieverOptions().addStore(store)
    )
    var appConfig: JsonObject
    retriever.config.onComplete() { ar ->
      if (ar.failed()) {
        // Failed to retrieve the configuration
        println(">>>> Failed to retrieve the configuration, cause = ${ar.cause().message}")
      } else {
        appConfig = ar.result()
        println(">>>> Success! config = $appConfig")

        // Create a Router
        val router = Router.router(vertx)

        // Mount the handler for all incoming requests at every path and HTTP method
        router.route().handler { context ->

          // Get the address of the request
          val address = context.request().connection().remoteAddress().toString()

          // Get the query parameter "name"
          val queryParams = context.queryParams()

          val name = queryParams.get("name") ?: "unknown"
          // Write a json response
          context.json(
            json {
              obj(
                "name" to name,
                "address" to address,
                "message" to "Hello $name connected from $address"
              )
            }
          )
        }

        val datasource: JsonObject = appConfig.getJsonObject("datasource")
        println(">>>>> datasource = $datasource")
        val connectOptions = datasource.let {
          PgConnectOptions()
            .setPort(it.getInteger("port"))
            .setHost(it.getString("host"))
            .setDatabase(it.getString("database"))
            .setUser(it.getString("user"))
            .setPassword(it.getString("password"))
        }

        // Pool options
        val poolOptions: PoolOptions = PoolOptions()
          .setMaxSize(datasource.getInteger("maxSize"))

        // Create the client pool
        val pool = PgBuilder
          .pool()
          .with(poolOptions)
          .connectingTo(connectOptions)
          .using(vertx)
          .build()

        // Populate database
        val sqlStatements = listOf(
          "CREATE TABLE MOVIE (ID VARCHAR(16) PRIMARY KEY, TITLE VARCHAR(256) NOT NULL)",
          "CREATE TABLE RATING (ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value INT, MOVIE_ID VARCHAR(16))",
          "INSERT INTO MOVIE (ID, TITLE) VALUES ('starwars', 'Star Wars')",
          "INSERT INTO MOVIE (ID, TITLE) VALUES ('indianajones', 'Indiana Jones')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (1, 'starwars')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (5, 'starwars')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (9, 'starwars')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (10, 'starwars')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (4, 'indianajones')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (7, 'indianajones')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (3, 'indianajones')",
          "INSERT INTO RATING (VALUE, MOVIE_ID) VALUES (9, 'indianajones')"
        )
        pool.connection
          .compose() { conn ->
            sqlStatements.stream().forEach { sqlStatement: String ->
              conn
                .query(sqlStatement)
                .execute()
                .onComplete() { rowSetAsyncResult ->
                  if (rowSetAsyncResult.failed()) {
                    println(">>>> Failed to run SQL statement = '${sqlStatement}', cause = ${rowSetAsyncResult.cause().message}")
                  } else {
                    println(
                      ">>>> Success! SQL statement = '${sqlStatement}', row count =  ${
                        rowSetAsyncResult.result().rowCount()
                      }"
                    )
                  }
                }
            }
            conn.close()
          }

        // Create the HTTP server
        vertx
          .createHttpServer()

          // Handle every request using the router
          .requestHandler(router)

          // Start listening
          .listen(8888) { http ->

            // Complete the promise and print the port
            if (http.succeeded()) {
              startPromise.complete()
              println("HTTP server started on port 8888")
            } else {
              startPromise.fail(http.cause());
            }
          }
      }
    }
  }
}
