package com.demo.producer;

/**
 * @author sskapci
 */

public class Producer {

    public static void main(String[] args) {

        //arg 0 broker
        //arg 1 topics separated with comma
        //arg 3 count of message count

        if (args.length < 3) {
            System.err.println("Args are wrong");
            System.exit(1);
        }

        String broker=args[0];
        String[] topics=args[1].split(",");
        Integer count=Integer.valueOf(args[2]);

        for (String topic:topics) {
            for (int i = 0; i < count; i++) {
                KafkaSender.SendMessage(broker,topic,new RandomString(8).nextString());
            }
        }




    }

}
