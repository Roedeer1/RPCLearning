package example;

import example.proto.Mail;
import example.proto.Message;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import java.net.InetSocketAddress;

/**
 * Created by U6071369 on 7/16/2018.
 */
public class AvroClient {
    public static void main(String[] args) throws Exception {
        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(65111));
        // get the interface mail of implements proxy
        Mail proxy = SpecificRequestor.getClient(Mail.class, client);
        System.out.println("Client of Mail proxy is built");

        // fill in the Message record and send it
        args = new String[] {"to:Tom", "from:Jack", "body:How are you"};
        Message message = new Message();
        message.setTo(new Utf8(args[0]));
        message.setFrom(new Utf8(args[1]));
        message.setBody(new Utf8(args[2]));
        System.out.println("RPC call with message:" + message.toString());

        //send the transfer of method send to server
        System.out.println("Result: " + proxy.send(message));

        // cleanup
        client.close();
    }
}
