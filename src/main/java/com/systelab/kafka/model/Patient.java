package com.systelab.kafka.model;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;

public class Patient {
    String id;
    String name;
    String surname;
    String country;

    public Patient(String id) {
        this.id = id;
    }

    public Patient(String id, String name, String surname, String country) {
        this.id = id;
        this.name = name;
        this.surname = surname;
        this.country = country;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String toJSON() {
        JsonObject json = Json.createObjectBuilder()
                .add("id", id)
                .add("name", name)
                .add("surname", surname)
                .add("country", country).build();
        return json.toString();
    }

    public static Patient fromJSON(String s) {
        JsonReader reader = Json.createReader(new StringReader(s));
        JsonObject object = reader.readObject();
        reader.close();

        Patient patient = new Patient(object.getString("id"));
        patient.setName(object.getString("name"));
        patient.setSurname(object.getString("surname"));
        patient.setCountry(object.getString("country"));
        return patient;
    }

    @Override
    public String toString() {
        return "Patient{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", country='" + country + '\'' +
                '}';
    }

}
