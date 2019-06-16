package com.atguigu.gmall1111.common.constant;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GmallConstant {

    public static final  String KAFKA_TOPIC_STARTUP="GMALL_STARTUP";
    public static final  String KAFKA_TOPIC_EVENT="GMALL_EVENT";
    public static final  String KAFKA_TOPIC_ORDER="GMALL_ORDER";

    public static final String ES_INDEX_DAU="gmall1111_dau";
    public static final String ES_TYPE_DEFAULT="_doc";



    public static void main(String[] args) {
        Lock lock =   new ReentrantLock();
                lock.lock();
                try {

                }finally {
                    lock.unlock();
                }
    }
}
