package swe.troye;

import java.net.URI;

import javax.annotation.PostConstruct;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;

@Path("movies")
public class MovieResource {

    @Inject
    PgPool client;

    @Inject
    @ConfigProperty(name = "movie.schema.create", defaultValue = "true")
    boolean schemaCreate;

    @PostConstruct
    void config(@Observes StartupEvent ev) {
        if (schemaCreate) {
            initDB();
        }
    }

    @GET
    public Multi<Movie> getAll() {
        return Movie.findAll(client);
    }

    @GET
    @Path("{id}")
    public Uni<Movie> get(@PathParam("id") Long id) {
        return Movie.findById(client, id);
    }

    @POST
    public Uni<Response> create(Movie movie) {
        return Movie.save(client, movie.getTitle())
                .onItem()
                .transform(id -> URI.create("/movies/" + id))
                .onItem()
                .transform(uri -> Response.created(uri).build());
    }

    @DELETE
    @Path("{id}")
    public Uni<Response> delete(@PathParam("id") Long id) {
        return Movie.delete(client, id)
                .onItem()
                .transform(deleted -> deleted ? Response.Status.NO_CONTENT : Response.Status.NOT_FOUND)
                .onItem()
                .transform(status -> Response.status(status).build());
    }

    private void initDB() {
        client.query("DROP TABLE IF EXISTS movies").execute()
                .flatMap(m -> client.query("CREATE TABLE movies (id SERIAL PRIMARY KEY, " +
                        "title TEXT NOT NULL)").execute())
                .flatMap(m -> client.query("INSERT INTO movies (title) VALUES ('Lord of the Rings')").execute())
                .flatMap(m -> client.query("INSERT INTO movies (title) VALUES ('Harry Potter')").execute())
                .await()
                .indefinitely();
    }
}
