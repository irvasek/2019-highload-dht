package ru.mail.polis.service.vasekha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import com.google.common.base.Charsets;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

public class MyService extends HttpServer implements Service {
    private final DAO dao;

    public MyService(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/entity") public Response entity(@Param("id") final String id, final Request request) {
        if (id == null) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
            case Request.METHOD_GET:
                ByteBuffer value = dao.get(key);
                ByteBuffer duplicate = value.duplicate();
                byte[] body = new byte[duplicate.remaining()];
                duplicate.get(body);
                return new Response(Response.OK, body);
            case Request.METHOD_PUT:
                dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                return new Response(Response.CREATED, Response.EMPTY);
            case Request.METHOD_DELETE:
                dao.remove(key);
                return new Response(Response.ACCEPTED, Response.EMPTY);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, "Internal server error".getBytes(Charsets.UTF_8));
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "No such element".getBytes(Charsets.UTF_8));
        }
    }

    private static HttpServerConfig getConfig(int port) {
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[] { acceptor };
        return config;
    }
}
