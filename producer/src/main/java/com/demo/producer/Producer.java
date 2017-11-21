package com.demo.producer;

import java.util.Random;

public class Producer {

    public static void main(String[] args) throws Exception {

        //arg 0 broker
        //arg 1 topic
        //arg 3 count of message count

        if (args.length < 3) {
            System.err.println("Args are wrong");
            System.exit(1);
        }

        String broker=args[0];
        String topic=args[1];
        Integer count=Integer.getInteger(args[2]);

        for (int i = 0; i < count; i++) {
            RandomString gen = new RandomString(8);
            KafkaSender.SendMessage(broker,topic,gen.nextString());
        }


    }

}
