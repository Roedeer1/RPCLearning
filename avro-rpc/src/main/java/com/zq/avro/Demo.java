package com.zq.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by U6071369 on 7/16/2018.
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        User user1 = new User();
        user1.setName("Arway");
        user1.setFavoriteNumber(3);
        user1.setFavoriteColor("green");
        User user2 = new User("Ben", 7, "red");
        // construct with builder
        User user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue").setFavoriteNumber(100).build();
        File file = new File("D:\\IDEA_workplace\\RPCLearning\\avro-rpc\\src\\main\\avro\\user.avro");
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        try {
            dataFileWriter.create(user1.getSchema(), file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
            dataFileWriter.close();
        } catch (IOException e) {
        }
        // Deserialize Users from dist
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<User>(file, userDatumReader);
        } catch (IOException e) {
        }
        User user = null;
        try {
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves
                // us from allocating and garbage collecting many objects for
                // files with many items.
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException e) {
        }

    }

}
