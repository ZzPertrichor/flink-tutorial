package org.example.wordcount;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author zm
 * @since 2021-10-19
 */
public class SocketServer {

    public static void main(String[] args) throws IOException {
        try (final ServerSocket server = new ServerSocket(8888);
             Socket socket = server.accept();
             final OutputStream os = socket.getOutputStream()) {

            // noinspection InfiniteLoopStatement
            for (; ; ) {
                os.write(System.in.read());
            }
        }
    }
}
