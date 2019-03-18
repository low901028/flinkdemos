package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Auther: dalan
 * @Date: 19-3-18 11:07
 * @Description:
 */
public class UserDefinePoJo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinePoJo.class);

    // main
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> stones = env.fromElements(
                new Person("Fred",35),
                new Person("Wilma", 35),
                new Person("Tom", 45),
                new Person("Pebbles", 2)
        );

        stones.filter(new FilterFunction<Person>() {
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        })
        .print();

        env.execute("a simple filnk demo");
    }

    public static  class Person{
        public String name;
        public Integer age;
        public Person(){}

        public Person(String name, Integer age){
            this.age = age;
            this.name = name;
        }

        public String toString(){
            return this.name.toString() + " :age= " + this.age.toString();
        }
    }
}
