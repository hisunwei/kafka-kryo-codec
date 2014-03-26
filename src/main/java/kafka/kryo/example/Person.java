package kafka.kryo.example;

import java.util.Date;

public class Person {
	String name;
	Date date;

	Person() {
	}

	Person(String name, Date date) {
		this.name = name;
		this.date = date;
	}

	public String toString() {
		return this.name + " " + this.date;
	}
}