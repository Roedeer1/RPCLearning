package example;

import example.proto.Mail;
import example.proto.Message;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by U6071369 on 7/16/2018.
 */

class MailImpl implements Mail {
    @Override
    public CharSequence send(Message message) throws AvroRemoteException {
        System.out.println("Message Received:" + message);
        return new Utf8("Received your message: " + message.getFrom().toString() + " with body: "
                + message.getBody().toString());
    }
}

public class AvroServer {

    private static Server server;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting server");
        startServer();
        Thread.sleep(1000);
        System.out.println("Server started");
        Thread.sleep(60 * 1000);
        server.close();
    }

    private static void startServer() throws IOException {
        server = new NettyServer(new SpecificResponder(Mail.class, new MailImpl()), new InetSocketAddress(65111));
    }

}
